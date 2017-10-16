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

import java.util.List;

import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.hadoop.fs.Path;

public class FileMetadataProjection2 {
  private final String partitionDesignator;
  private final boolean hasMetadata;
  private final boolean useLegacyWildcardExpansion;
  private final Path scanRootDir;
  private final List<FileMetadataColumnDefn> fileMetadataColDefns;

  public FileMetadataProjection(FileMetadataColumnsParser builder) {
    partitionDesignator = builder.partitionDesignator;
    fileMetadataColDefns = builder.implicitColDefns;
    hasMetadata = builder.hasMetadata;
    useLegacyWildcardExpansion = builder.useLegacyWildcardExpansion;
    scanRootDir = builder.scanRootDir;
  }

  public boolean hasMetadata() { return hasMetadata; }

  public boolean useLegacyWildcardPartition() { return useLegacyWildcardExpansion; }

  public FileMetadata fileMetadata(Path filePath) {
    return new FileMetadata(filePath, scanRootDir);
  }

  public FileLevelProjection resolve(ScanLevelProjection scanProj, Path filePath) {
    return FileLevelProjection.fromResolution(scanProj, this, fileMetadata(filePath));
  }

  public FileLevelProjection resolve(ScanLevelProjection scanProj, FileMetadata fileInfo) {
    return FileLevelProjection.fromResolution(scanProj, this, fileInfo);
  }

  // Temporary

  private ColumnsArrayProjection colArrayProj;
  public void bind(ColumnsArrayProjection proj) { colArrayProj = proj; }
  public ColumnsArrayProjection columnsArrayProjection() { return colArrayProj; }
}