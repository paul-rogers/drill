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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.impl.scan.framework.AbstractScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ShimBatchReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Scan framework for a file that implements metadata columns (AKA "implicit"
 * columns and partition columns.)
 */

public abstract class BaseFileScanFramework<T extends BaseFileScanFramework.FileSchemaNegotiator>
    extends AbstractScanFramework<T> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseFileScanFramework.class);

  /**
   * The file schema negotiator adds no behavior at present, but is
   * created as a placeholder anticipating the need for file-specific
   * behavior later.
   */

  public interface FileSchemaNegotiator extends SchemaNegotiator {
  }

  private final List<? extends FileWork> files;
  private final Configuration fsConfig;
  private List<FileSplit> spilts = new ArrayList<>();
  private Iterator<FileSplit> splitIter;
  private Path scanRootDir;
  private boolean useLegacyWildcardExpansion = true;
  protected DrillFileSystem dfs;
  private FileMetadataManager metadataManager;

  public BaseFileScanFramework(List<SchemaPath> projection,
      List<? extends FileWork> files,
      Configuration fsConf) {
    super(projection);
    this.files = files;
    this.fsConfig = fsConf;
  }

  /**
   * Specify the selection root for a directory scan, if any.
   * Used to populate partition columns.
   * @param rootPath Hadoop file path for the directory
   */

  public void setSelectionRoot(Path rootPath) {
    this.scanRootDir = rootPath;
   }

  public void useLegacyWildcardExpansion(boolean flag) {
    useLegacyWildcardExpansion = flag;
  }

  @Override
  protected void configure() {

    // And the Drill file system.
    // TODO: Move this into the scan framework, made available via
    // the schema negotiator. No reason for this to be in the context as
    // not all scanner need a file system.

    try {
      dfs = context.newFileSystem(fsConfig);
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .addContext("Failed to create FileSystem")
        .build(logger);
    }

    List<Path> paths = new ArrayList<>();
    for(FileWork work : files) {
      Path path = dfs.makeQualified(new Path(work.getPath()));
      paths.add(path);
      FileSplit split = new FileSplit(path, work.getStart(), work.getLength(), new String[]{""});
      spilts.add(split);
    }
    splitIter = spilts.iterator();

    metadataManager = new FileMetadataManager(
        context.getFragmentContext().getOptionSet(),
        useLegacyWildcardExpansion,
        scanRootDir,
        paths);
    scanProjector.withMetadata(metadataManager);
  }

  @Override
  public RowBatchReader nextReader() {
    if (! splitIter.hasNext()) {
      return null;
    }
    FileSplit split = splitIter.next();
    startFile(split);
    try {
      return new ShimBatchReader<T>(this, newReader(split));
    } catch (ExecutionSetupException e) {
      throw UserException.executionError(e)
        .addContext("File", split.getPath().toString())
        .build(logger);
    }
  }

  protected abstract ManagedReader<T> newReader(FileSplit split) throws ExecutionSetupException;

  protected void startFile(FileSplit split) {
    metadataManager.startFile(split.getPath());
  }
}
