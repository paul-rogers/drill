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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumnsParser.FileMetadataProjection;
import org.apache.drill.exec.physical.impl.scan.managed.AbstractScanLifecycle;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjectionBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector;
import org.apache.hadoop.fs.Path;

public class FileScanLifecycle extends AbstractScanLifecycle {

  public static class FileScanConfig extends AbstractScanLifecycle.BasicScanConfig {
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

    // Temporary

    protected MajorType nullType() {
      return nullType;
    }
  }

  private FileScanConfig fileConfig;
  private FileMetadataColumnsParser parser;

  public FileScanLifecycle(FileScanLifecycle.FileScanConfig fileConfig) {
    this.fileConfig = fileConfig;
  }

  @Override
  protected AbstractScanLifecycle.BasicScanConfig scanConfig() { return fileConfig; }

  @Override
  protected void defineParsers(ScanProjectionBuilder scanProjBuilder) {
    parser = new FileMetadataColumnsParser(context.getFragmentContext().getOptionSet());
    parser.useLegacyWildcardExpansion(fileConfig.useLegacyWildcardExpansion);
    parser.setScanRootDir(fileConfig.scanRootDir);
    scanProjBuilder.addParser(parser);
  }

  // TODO: Temporary
  @Override
  protected void buildProjector(ScanLevelProjection scanProj) {
    FileMetadataProjection metadataPlan = parser.getProjection();
    scanProjector = new ScanProjector(context.getAllocator(), scanProj, metadataPlan, fileConfig.nullType());
  }
}
