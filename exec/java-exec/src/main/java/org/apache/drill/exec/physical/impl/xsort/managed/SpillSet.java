package org.apache.drill.exec.physical.impl.xsort.managed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
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
   * The HDFS file system (for local directories, HDFS storage, etc.) used to
   * create the temporary spill files. Allows spill files to be either on local
   * disk, or in a DFS. (The admin can choose to put spill files in DFS when
   * nodes provide insufficient local disk space)
   */

  private FileSystem fs;

  private final Iterator<String> dirs;

  /**
   * Set of directories to which this operator should write spill files in a round-robin
   * fashion. The operator requires at least one spill directory, but can
   * support any number. The admin must ensure that sufficient space exists
   * on all directories as this operator does not check space availability
   * before writing to the directories.
   */

  private Set<Path> currSpillDirs = Sets.newTreeSet();

  /**
   * The base part of the file name for spill files. Each file has this
   * name plus an appended spill serial number.
   */

  private final String fileName;

  private int fileCount = 0;

  public SpillSet( FragmentContext context, PhysicalOperator popConfig ) {
    DrillConfig config = context.getConfig();
    dirs = Iterators.cycle(config.getStringList(ExecConstants.EXTERNAL_SORT_SPILL_DIRS));
    Configuration conf = new Configuration();
    conf.set("fs.default.name", config.getString(ExecConstants.EXTERNAL_SORT_SPILL_FILESYSTEM));
    try {
      this.fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    FragmentHandle handle = context.getHandle();
    fileName = String.format("%s_major%s_minor%s_op%s", QueryIdHelper.getQueryId(handle.getQueryId()),
        handle.getMajorFragmentId(), handle.getMinorFragmentId(), popConfig.getOperatorId());
  }

  public String getNextSpillFile() {

    // Identify the next directory from the round-robin list to
    // the file created from this round of spilling. The directory must
    // must have sufficient space for the output file.

    String spillDir = dirs.next();
    Path currSpillPath = new Path(Joiner.on("/").join(spillDir, fileName));
    currSpillDirs.add(currSpillPath);
    String outputFile = Joiner.on("/").join(currSpillPath, "spill" + ++fileCount);
    try {
        fs.deleteOnExit(currSpillPath);
    } catch (IOException e) {
        // since this is meant to be used in a batches's spilling, we don't propagate the exception
        logger.warn("Unable to mark spill directory " + currSpillPath + " for deleting on exit", e);
    }
    return outputFile;
  }

  public boolean hasSpilled( ) {
    return fileCount > 0;
  }

  public int getFileCount( ) { return fileCount; }

  public InputStream openForInput( String fileName ) throws IOException {
    return fs.open(new Path(fileName));
  }

  public OutputStream openForOutput( String fileName ) throws IOException {
    return fs.create(new Path(fileName));
  }

  public void delete( String fileName ) throws IOException {
    Path path = new Path(fileName);
    if (fs.exists(path)) {
      fs.delete(path, false);
    }
  }

  public void close() {
    for ( Path path : currSpillDirs ) {
      try {
          if (fs != null && path != null && fs.exists(path)) {
              if (fs.delete(path, true)) {
                  fs.cancelDeleteOnExit(path);
              }
          }
      } catch (IOException e) {
          // since this is meant to be used in a batches's cleanup, we don't propagate the exception
          logger.warn("Unable to delete spill directory " + path,  e);
      }
    }
  }
}
