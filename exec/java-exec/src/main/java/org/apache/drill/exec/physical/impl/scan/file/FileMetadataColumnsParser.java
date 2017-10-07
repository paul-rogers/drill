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
      if (builder.hasWildcard()) {
        wildcardAndMetadataError();
      }

      // Partition column

      builder.addMetadataColumn(
          new PartitionColumn(
            inCol.rootName(),
            Integer.parseInt(m.group(1))));
      hasMetadata = true;
      return true;
    }

    FileMetadataColumnDefn iCol = metadataManager.fileMetadataColIndex.get(inCol.rootName());
    if (iCol != null) {
      if (builder.hasWildcard()) {
        wildcardAndMetadataError();
      }

      // File metadata (implicit) column

      builder.addMetadataColumn(new FileMetadataColumn(inCol.rootName(), iCol));
      hasMetadata = true;
      return true;
    }
    if (inCol.isWildcard()) {
      if (hasMetadata) {
        wildcardAndMetadataError();
      }
      if (metadataManager.useLegacyWildcardExpansion) {

        // Star column: this is a SELECT * query.

        // Old-style wildcard handling inserts all metadata columns in
        // the scanner, removes them in Project.
        // Fill in the file metadata columns. Can do here because the
        // set is constant across all files.

        expandWildcard();
        hasMetadata = true;

        // Don't consider this a match.
      }
    }
    return false;
  }

  protected void expandWildcard() {

    // Legacy wildcard expansion: include the file metadata and
    // file partitions for this file.
    // This is a disadvantage for a * query: files at different directory
    // levels will have different numbers of columns. Would be better to
    // return this data as an array at some point.
    // Append this after the *, keeping the * for later expansion.

    for (FileMetadataColumnDefn iCol : metadataManager.fileMetadataColDefns()) {
      builder.addMetadataColumn(new FileMetadataColumn(
          iCol.colName(), iCol));
    }
    for (int i = 0; i < metadataManager.partitionCount(); i++) {
      builder.addMetadataColumn(new PartitionColumn(
          metadataManager.partitionName(i), i));
    }
  }

  @Override
  public void validate() { }

  @Override
  public void validateColumn(ColumnProjection outCol) { }

  private void wildcardAndMetadataError() {
    throw new IllegalArgumentException("Cannot select file metadata columns and `*` together");
  }

  @Override
  public void build() { }
}