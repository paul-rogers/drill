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
package org.apache.drill.exec.physical.impl.scan.basic;

import java.util.Iterator;

import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumnsParser;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataProjection;
import org.apache.drill.exec.physical.impl.scan.managed.AbstractScanFramework;
import org.apache.drill.exec.physical.impl.scan.managed.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.managed.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjectionBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector;

public class BasicScanFramework extends AbstractScanFramework<SchemaNegotiator> {

  public static class BasicScanConfig extends AbstractScanConfig<SchemaNegotiator> {

    protected Iterator<ManagedReader<SchemaNegotiator>> readerFactory;

    public void setReaderFactory(Iterator<ManagedReader<SchemaNegotiator>> readerFactory) {
      this.readerFactory = readerFactory;
    }
  }

  private BasicScanConfig scanConfig;
  protected ScanProjector scanProjector;

  public BasicScanFramework(BasicScanConfig config) {
    this.scanConfig = config;
  }

  @Override
  protected AbstractScanConfig<SchemaNegotiator> scanConfig() { return scanConfig; }

  // Temporary

  private FileMetadataColumnsParser parser;
  @Override
  protected void defineParsers(ScanProjectionBuilder scanProjBuilder) {
    parser = new FileMetadataColumnsParser(context.getFragmentContext().getOptionSet());
    parser.useLegacyWildcardExpansion(false);
    parser.setScanRootDir(null);
    scanProjBuilder.addParser(parser);
  }

  @Override
  protected void buildProjector(ScanLevelProjection scanProj) {
    FileMetadataProjection metadataPlan = parser.getProjection();
    scanProjector = new ScanProjector(context.getAllocator(), scanProj, metadataPlan, scanConfig.nullType());
  }

  @Override
  public RowBatchReader nextReader() {
    if (! scanConfig.readerFactory.hasNext()) {
      return null;
    }
    ManagedReader<SchemaNegotiator> reader = scanConfig.readerFactory.next();
    return new BasicReaderShim(this, reader);
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
