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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;

public class FileMetadataColumnsParser implements ScanProjectionParser {

  // Internal

  private final FileMetadataManager metadataManager;
  private final Pattern partitionPattern;
  private ScanLevelProjection builder;

  // Output

  boolean hasMetadata;

  public FileMetadataColumnsParser(FileMetadataManager metadataManager) {
    this.metadataManager = metadataManager;
    partitionPattern = Pattern.compile(metadataManager.partitionDesignator + "(\\d+)", Pattern.CASE_INSENSITIVE);
  }

  @Override
  public void bind(ScanLevelProjection builder) {
    this.builder = builder;
  }

  @Override
  public boolean parse(SchemaPath inCol) {
    Matcher m = partitionPattern.matcher(inCol.rootName());
    if (m.matches()) {

      // Partition column

      builder.addProjectedColumn(new PartitionColumn(
          inCol.rootName(),
          Integer.parseInt(m.group(1))));
      hasMetadata = true;
      return true;
    }

    FileMetadataColumnDefn iCol = metadataManager.fileMetadataColIndex.get(inCol.rootName());
    if (iCol != null) {

      // File metadata (implicit) column

      builder.addProjectedColumn(new FileMetadataColumn(inCol.rootName(), iCol));
      hasMetadata = true;
      return true;
    }
    if (inCol.isWildcard() && metadataManager.useLegacyWildcardExpansion) {

      // Star column: this is a SELECT * query.

      // Old-style wildcard handling inserts all metadata columns in
      // the scanner, removes them in Project.
      // Fill in the file metadata columns. Can do here because the
      // set is constant across all files.

      hasMetadata = true;

      // Don't consider this a match.
    }
    return false;
  }

  @Override
  public void validate() { }

  @Override
  public void validateColumn(ColumnProjection outCol) {
    if (outCol.nodeType() == FileMetadataColumn.ID) {
      if (builder.hasWildcard()  &&  metadataManager.useLegacyWildcardExpansion) {
        throw new IllegalArgumentException("Cannot select file metadata columns and `*` together");
      }
    } else if (outCol.nodeType() == PartitionColumn.ID) {
      if (builder.hasWildcard()  &&  metadataManager.useLegacyWildcardExpansion) {
        throw new IllegalArgumentException("Cannot select partitions and `*` together");
      }
    }
  }

  @Override
  public void build() { }
}