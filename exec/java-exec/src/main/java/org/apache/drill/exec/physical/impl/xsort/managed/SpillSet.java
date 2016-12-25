/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

/**
 * Generates the set of spill files for this sort session.
 */

public class SpillSet {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpillSet.class);

  /**
   * Spilling on the Mac using the HDFS file system is very inefficient,
   * affects performance numbers. This interface allows using HDFS in
   * production, but to bypass the HDFS file system when needed.
   */

  private interface FileManager {

    void deleteOnExit(String fragmentSpillDir) throws IOException;

    OutputStream createForWrite(String fileName) throws IOException;

    InputStream openForInput(String fileName) throws IOException;

    void deleteFile(String fileName) throws IOException;

    void deleteDir(String fragmentSpillDir) throws IOException;
  }

  /**
   * Normal implementation of spill files using the HDFS file system.
   */

  private static class HadoopFileManager implements FileManager{
    /**
     * The HDFS file system (for local directories, HDFS storage, etc.) used to
     * create the temporary spill files. Allows spill files to be either on local
     * disk, or in a DFS. (The admin can choose to put spill files in DFS when
     * nodes provide insufficient local disk space)
     */

    private FileSystem fs;

    protected HadoopFileManager(String fsName) {
      Configuration conf = new Configuration();
      conf.set("fs.default.name", fsName);
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw UserException.resourceError(e)
              .message("Failed to get the File System for external sort")
              .build(logger);
      }
    }

    @Override
    public void deleteOnExit(String fragmentSpillDir) throws IOException {
      fs.deleteOnExit(new Path(fragmentSpillDir));
    }

    @Override
    public OutputStream createForWrite(String fileName) throws IOException {
      return fs.create(new Path(fileName));
    }

    @Override
    public InputStream openForInput(String fileName) throws IOException {
      return fs.open(new Path(fileName));
    }

    @Override
    public void deleteFile(String fileName) throws IOException {
      Path path = new Path(fileName);
      if (fs.exists(path)) {
        fs.delete(path, false);
      }
    }

    @Override
    public void deleteDir(String fragmentSpillDir) throws IOException {
      Path path = new Path(fragmentSpillDir);
      if (path != null && fs.exists(path)) {
        if (fs.delete(path, true)) {
            fs.cancelDeleteOnExit(path);
        }
      }
    }
  }

  /**
   * Performance-oriented direct access to the local file system which
   * bypasses HDFS.
   */

  private static class LocalFileManager implements FileManager {

    private File baseDir;

    public LocalFileManager(String fsName) {
      baseDir = new File(fsName.replace("file://", ""));
    }

    @Override
    public void deleteOnExit(String fragmentSpillDir) throws IOException {
      File dir = new File(baseDir, fragmentSpillDir);
      dir.mkdirs();
      dir.deleteOnExit();
    }

    @Override
    public OutputStream createForWrite(String fileName) throws IOException {
      return new FileOutputStream(new File(baseDir, fileName));
    }

    @Override
    public InputStream openForInput(String fileName) throws IOException {
      return new FileInputStream(new File(baseDir, fileName));
    }

    @Override
    public void deleteFile(String fileName) throws IOException {
      new File(baseDir, fileName).delete();
    }

    @Override
    public void deleteDir(String fragmentSpillDir) throws IOException {
      new File(baseDir, fragmentSpillDir).delete();
    }
  }

  private final Iterator<String> dirs;

  /**
   * Set of directories to which this operator should write spill files in a round-robin
   * fashion. The operator requires at least one spill directory, but can
   * support any number. The admin must ensure that sufficient space exists
   * on all directories as this operator does not check space availability
   * before writing to the directories.
   */

  private Set<String> currSpillDirs = Sets.newTreeSet();

  /**
   * The base part of the file name for spill files. Each file has this
   * name plus an appended spill serial number.
   */

  private final String spillDirName;

  private int fileCount = 0;

  private FileManager fileManager;

  public SpillSet(FragmentContext context, PhysicalOperator popConfig) {
    DrillConfig config = context.getConfig();
    dirs = Iterators.cycle(config.getStringList(ExecConstants.EXTERNAL_SORT_SPILL_DIRS));

    // Use the high-performance local file system if the local file
    // system is selected and impersonation is off. (We use that
    // as a proxy for a non-production Drill setup.)

    String spillFs = config.getString(ExecConstants.EXTERNAL_SORT_SPILL_FILESYSTEM);
    boolean impersonationEnabled = config.getBoolean(ExecConstants.IMPERSONATION_ENABLED);
    if (spillFs.startsWith("file:///") && ! impersonationEnabled) {
      fileManager = new LocalFileManager(spillFs);
    } else {
      fileManager = new HadoopFileManager(spillFs);
    }
    FragmentHandle handle = context.getHandle();
    spillDirName = String.format("%s_major%s_minor%s_op%s", QueryIdHelper.getQueryId(handle.getQueryId()),
        handle.getMajorFragmentId(), handle.getMinorFragmentId(), popConfig.getOperatorId());
  }

  public String getNextSpillFile() {

    // Identify the next directory from the round-robin list to
    // the file created from this round of spilling. The directory must
    // must have sufficient space for the output file.

    String spillDir = dirs.next();
    String currSpillPath = Joiner.on("/").join(spillDir, spillDirName);
    currSpillDirs.add(currSpillPath);
    String outputFile = Joiner.on("/").join(currSpillPath, "spill" + ++fileCount);
    try {
        fileManager.deleteOnExit(currSpillPath);
    } catch (IOException e) {
        // since this is meant to be used in a batches's spilling, we don't propagate the exception
        logger.warn("Unable to mark spill directory " + currSpillPath + " for deleting on exit", e);
    }
    return outputFile;
  }

  public boolean hasSpilled() {
    return fileCount > 0;
  }

  public int getFileCount() { return fileCount; }

  public InputStream openForInput(String fileName) throws IOException {
    return fileManager.openForInput(fileName);
  }

  public OutputStream openForOutput(String fileName) throws IOException {
    return fileManager.createForWrite(fileName);
  }

  public void delete(String fileName) throws IOException {
    fileManager.deleteFile(fileName);
  }

  public void close() {
    for (String path : currSpillDirs) {
      try {
        fileManager.deleteDir(path);
      } catch (IOException e) {
          // since this is meant to be used in a batches's cleanup, we don't propagate the exception
          logger.warn("Unable to delete spill directory " + path,  e);
      }
    }
  }
}
