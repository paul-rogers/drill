/*
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
package org.apache.drill.exec.physical.impl.scan.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiatorImpl;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiatorImpl.NegotiatorListener;
import org.apache.drill.exec.physical.impl.scan.framework.SimpleReaderFactory;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * The file scan framework adds into the scan framework support for implicit
 * reading from DFS splits (a file and a block). Since this framework is
 * file-based, it also adds support for file metadata (AKA implicit columns.
 * The file scan framework brings together a number of components:
 * <ul>
 * <li>The set of options defined by the base framework.</li>
 * <li>The set of files and/or blocks to read.</li>
 * <li>The file system configuration to use for working with the files
 * or blocks.</li>
 * <li>The factory class to create a reader for each of the files or blocks
 * defined above. (Readers are created one-by-one as files are read.)</li>
 * <li>Options as defined by the base class.</li>
 * </ul>
 * <p>
 * The framework iterates over file descriptions, creating readers at the
 * moment they are needed. This allows simpler logic because, at the point of
 * reader creation, we have a file system, context and so on.
 * <p>
 * @See {AbstractScanFramework} for details.
 */

public class FileScanFramework extends ManagedScanFramework {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileScanFramework.class);

  /**
   * The file schema negotiator adds no behavior at present, but is
   * created as a placeholder anticipating the need for file-specific
   * behavior later. Readers are expected to use an instance of this
   * class so that their code need not change later if/when we add new
   * methods. For example, perhaps we want to specify an assumed block
   * size for S3 files, or want to specify behavior if the file no longer
   * exists. Those are out of scope of this first round of changes which
   * focus on schema.
   */

  public interface FileSchemaNegotiator extends SchemaNegotiator {
    public FileSplit fileSplit();
  }

  /**
   * Implementation of the file-level schema negotiator. At present, no
   * file-specific features exist. This class shows, however, where we would
   * add such features.
   */

  public static class FileSchemaNegotiatorImpl extends SchemaNegotiatorImpl
      implements FileSchemaNegotiator {

    private FileSplit split;

    protected void bindSplit(FileSplit split) {
      this.split = split;
    }

    @Override
    public FileSplit fileSplit() {
      return split;
    }
  }

  /**
   * Options for a file-based scan.
   */

  public static class FileScanBuilder extends ScanFrameworkBuilder {
    private List<? extends FileWork> files;
    private Configuration fsConf;
    private Path scanRootDir;
    private int partitionDepth;

    public void setFiles(Configuration fsConf, List<? extends FileWork> files) {
      this.fsConf = fsConf;
      this.files = files;
    }

   /**
     * Specify the selection root for a directory scan, if any.
     * Used to populate partition columns. Also, specify the maximum
     * partition depth.
     *
     * @param rootPath Hadoop file path for the directory
     * @param partitionDepth maximum partition depth across all files
     * within this logical scan operator (files in this scan may be
     * shallower)
     */

    public void setSelectionRoot(Path rootPath, int partitionDepth) {
      this.scanRootDir = rootPath;
      this.partitionDepth = partitionDepth;
    }

    public void setSchemaNegotiator(Class<? extends FileSchemaNegotiatorImpl> negotiatorClass) {
      this.negotiatorClass = negotiatorClass;
    }

    public void setReader(Class<? extends ManagedReader<? extends FileSchemaNegotiator>> readerClass) {
      this.readerClass = readerClass;
    }
  }

  public static class FileReaderFactory extends SimpleReaderFactory {

    private FileScanFramework fileFramework;
    private DrillFileSystem dfs;
    private List<FileSplit> spilts = new ArrayList<>();
    private Iterator<FileSplit> splitIter;
    private FileSplit currentSplit;

    @Override
    public void bind(ManagedScanFramework baseFramework) {
      this.fileFramework = (FileScanFramework) baseFramework;
    }

    private List<Path> configure() {
      FileScanBuilder options = fileFramework.options();

      // Create the Drill file system.

      try {
        dfs = fileFramework.context.newFileSystem(options.fsConf);
      } catch (IOException e) {
        throw UserException.dataReadError(e)
          .addContext("Failed to create FileSystem")
          .build(logger);
      }

      // Prepare the list of files. We need the list of paths up
      // front to compute the maximum partition. Then, we need to
      // iterate over the splits to create readers on demand.

      List<Path> paths = new ArrayList<>();
      for (FileWork work : options.files) {
        Path path = dfs.makeQualified(work.getPath());
        paths.add(path);
        FileSplit split = new FileSplit(path, work.getStart(), work.getLength(), new String[]{""});
        spilts.add(split);
      }
      splitIter = spilts.iterator();
      return paths;
    }

    @Override
    public ManagedReader<? extends SchemaNegotiator> next() {

      // Create a reader on demand for the next split.

      if (! splitIter.hasNext()) {
        currentSplit = null;
        return null;
      }
      currentSplit = splitIter.next();


      // Tell the metadata manager about the current file so it can
      // populate the metadata columns, if requested.

      fileFramework.metadataManager.startFile(currentSplit.getPath());
      return newReader();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean open(ManagedReader<? extends SchemaNegotiator> reader,
        NegotiatorListener listener) {
      FileSchemaNegotiatorImpl schemaNegotiator = (FileSchemaNegotiatorImpl) newNegotiator();
      schemaNegotiator.bind(fileFramework, listener);
      schemaNegotiator.bindSplit(currentSplit);
      try {
        return ((ManagedReader<SchemaNegotiator>) reader).open(schemaNegotiator);
      } catch (UserException e) {
        throw e;
      } catch (Exception e) {
        throw UserException.executionError(e)
          .addContext("File", currentSplit.getPath().toString())
          .build(logger);
      }
    }
  }

  private FileMetadataManager metadataManager;

  public FileScanFramework(FileScanBuilder builder) {
    super(builder);
    assert builder.files != null;
    assert builder.fsConf != null;
  }

  public FileScanBuilder options() {
    return (FileScanBuilder) builder;
  }

  @Override
  protected void configure() {
    super.configure();

    FileScanBuilder fsBuilder = options();
    if (fsBuilder.negotiatorClass() == null) {
      fsBuilder.setSchemaNegotiator(FileSchemaNegotiatorImpl.class);
    }

    FileReaderFactory fileReaderFactory = (FileReaderFactory) readerFactory;
    List<Path> paths = fileReaderFactory.configure();

    // Create the metadata manager to handle file metadata columns
    // (so-called implicit columns and partition columns.)

    metadataManager = new FileMetadataManager(
        context.getFragmentContext().getOptions(),
        true, // Expand partition columns with wildcard
        false, // Put partition columns after table columns
        fsBuilder.scanRootDir,
        fsBuilder.partitionDepth,
        paths);
    builder.withMetadata(metadataManager);
  }
}
