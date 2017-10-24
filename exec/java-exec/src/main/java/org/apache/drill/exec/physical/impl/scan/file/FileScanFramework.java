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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.impl.scan.file.FileBatchReader.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.AbstractReaderShim;
import org.apache.drill.exec.physical.impl.scan.framework.AbstractScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.AbstractSchemaNegotiatorImpl;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.hadoop.fs.Path;

/**
 * Scan framework for a file that implements metadata columns (AKA "implicit"
 * columns and partition columns.)
 */

public class FileScanFramework extends AbstractScanFramework<FileSchemaNegotiator> {

  /**
   * Configuration of the scan framework.
   */

  public static class FileScanConfig extends AbstractScanConfig<FileSchemaNegotiator> {
    protected Iterator<FileBatchReader> readerFactory;
    protected Path scanRootDir;
    protected boolean useLegacyWildcardExpansion = true;
    protected List<Path> filePaths = new ArrayList<>();

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

    public void addFile(Path file) {
      filePaths.add(file);
    }

    public void setReaderFactory(Iterator<FileBatchReader> readerFactory) {
      this.readerFactory = readerFactory;
    }
  }

  /**
   * Implementation of the file-level schema negotiator.
   */

  public static class FileSchemaNegotiatorImpl extends AbstractSchemaNegotiatorImpl
      implements FileSchemaNegotiator {

    private final FileReaderShim shim;
    protected Path filePath;

    public FileSchemaNegotiatorImpl(OperatorContext context, FileReaderShim shim) {
      super(context);
      this.shim = shim;
    }

    @Override
    public void setFilePath(Path filePath) {
      this.filePath = filePath;
    }

    @Override
    public ResultSetLoader build() {
      return shim.build(this);
    }
  }

  /**
   * Implementation of the reader-level framework.
   */

  public static class FileReaderShim extends AbstractReaderShim<FileSchemaNegotiator> {

    public FileReaderShim(AbstractScanFramework<FileSchemaNegotiator> manager,
        ManagedReader<FileSchemaNegotiator> reader) {
      super(manager, reader);
    }

    @Override
    protected boolean openReader() {
      FileSchemaNegotiator schemaNegotiator = new FileSchemaNegotiatorImpl(manager.context(), this);
      return reader.open(schemaNegotiator);
    }

    public ResultSetLoader build(FileSchemaNegotiatorImpl schemaNegotiator) {
      ((FileScanFramework) manager).metadataManager.startFile(schemaNegotiator.filePath);
      return super.build(schemaNegotiator);
    }
  }

  private FileScanConfig scanConfig;
  private FileMetadataManager metadataManager;

  public FileScanFramework(FileScanConfig fileConfig) {
    this.scanConfig = fileConfig;
  }

  @Override
  protected AbstractScanConfig<FileSchemaNegotiator> scanConfig() { return scanConfig; }

  @Override
  public void bind(OperatorContext context) {
    super.bind(context);
    configure(scanConfig);
    metadataManager = new FileMetadataManager(
        context.getFragmentContext().getOptionSet(),
        scanConfig.useLegacyWildcardExpansion,
        scanConfig.scanRootDir,
        scanConfig.filePaths);
    scanProjector.withMetadata(metadataManager);
    buildProjection(scanConfig);
  }

  @Override
  public RowBatchReader nextReader() {
    if (! scanConfig.readerFactory.hasNext()) {
      return null;
    }
    FileBatchReader reader = scanConfig.readerFactory.next();
    return new FileReaderShim(this, reader);
  }
}
