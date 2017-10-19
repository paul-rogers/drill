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

import java.util.Iterator;

import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.impl.scan.managed.AbstractScanFramework;
import org.apache.drill.exec.physical.impl.scan.managed.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector;
import org.apache.hadoop.fs.Path;

import jersey.repackaged.com.google.common.collect.Lists;

public class FileScanFramework extends AbstractScanFramework<FileSchemaNegotiator> {

  public static class FileScanConfig extends AbstractScanConfig<FileSchemaNegotiator> {
    protected Iterator<ManagedReader<FileSchemaNegotiator>> readerFactory;
    protected Path scanRootDir;
    protected boolean useLegacyWildcardExpansion = true;

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

    public void setReaderFactory(Iterator<ManagedReader<FileSchemaNegotiator>> readerFactory) {
      this.readerFactory = readerFactory;
    }
  }

  private FileScanConfig scanConfig;
  private FileMetadataManager metadataManager;
  private ScanProjector scanProjector;

  public FileScanFramework(FileScanConfig fileConfig) {
    this.scanConfig = fileConfig;
  }

  @Override
  protected AbstractScanConfig<FileSchemaNegotiator> scanConfig() { return scanConfig; }

  @Override
  public void bind(OperatorContext context) {
    super.bind(context);
    metadataManager = new FileMetadataManager(
        context.getFragmentContext().getOptionSet(),
        scanConfig.useLegacyWildcardExpansion,
        scanConfig.scanRootDir);
    ScanLevelProjection scanProj = new ScanLevelProjection(
        scanConfig.projection(),
        Lists.newArrayList(metadataManager.projectionParser()));
    scanProjector = new ScanProjector(context.getAllocator(), scanProj, metadataManager, scanConfig.nullType());
  }

  @Override
  public RowBatchReader nextReader() {
    if (! scanConfig.readerFactory.hasNext()) {
      return null;
    }
    ManagedReader<FileSchemaNegotiator> reader = scanConfig.readerFactory.next();
    return new FileReaderShim(this, reader);
  }

  @Override
  public ScanProjector projector() {
    return scanProjector;
  }

  @Override
  public void close() {
    if (scanProjector != null) {
      scanProjector.close();
      scanProjector = null;
    }
  }
}