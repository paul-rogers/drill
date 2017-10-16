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
import java.util.List;

import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ReaderLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.UnresolvedColumn;

/**
 * Represents a partially-resolved projection list with the per-file metadata
 * columns (if any) populated. Must be combined with the table schema to
 * produce the final, fully-resolved projection list.
 * <p>
 * Given an unresolved projection list, possibly with placeholder metadata
 * columns, produce a new, partially resolved list with the metadata
 * columns replaced by concrete versions with associated values. (Metadata
 * columns are constant for an entire file.)
 */

public class FileLevelProjection implements ReaderLevelProjection {
  private final FileMetadataManager metadataProj;
  private final FileMetadata fileInfo;
  private final List<ColumnProjection> outputCols;
  private final boolean isReresolution;

  public FileLevelProjection(ScanLevelProjection scanProjDefn, FileMetadataManager metadataProj, FileMetadata fileInfo) {
    this(scanProjDefn, metadataProj, scanProjDefn.outputCols(), fileInfo);
  }

  public FileLevelProjection(ScanLevelProjection scanProjDefn,
      FileMetadataManager metadataProj,
      List<ColumnProjection> inputCols,
      FileMetadata fileInfo) {
    this.metadataProj = metadataProj;
    isReresolution = inputCols != scanProjDefn.outputCols();
    this.fileInfo = fileInfo;

    // If the projection plan has file or partition metadata
    // columns, rewrite them with actual file information.

    if (metadataProj.hasMetadata()) {
      outputCols = new ArrayList<>();
      for (ColumnProjection col : inputCols) {
        translate(col);
      }
    } else {

      // No file or partition columns, just use the unresolved
      // query projection plan as-is.

      outputCols = inputCols;
    }
  }

  private void translate(ColumnProjection col) {
    switch (col.nodeType()) {
    case UnresolvedColumn.WILDCARD:
      outputCols.add(col);
      expandWildcard((UnresolvedColumn) col);
      break;

    case PartitionColumn.ID:
      outputCols.add(((PartitionColumn) col).resolve(fileInfo));
      break;

    case FileMetadataColumn.ID:
      outputCols.add(((FileMetadataColumn) col).resolve(fileInfo));
      break;

    default:
      outputCols.add(col);
    }
  }

  protected void expandWildcard(UnresolvedColumn col) {

    // Skip wildcard expansion if not legacy or if the data source
    // does not provide file information.

    if (! metadataProj.useLegacyWildcardPartition() || ! fileInfo.isSet()) {
      return;
    }

    // Legacy wildcard expansion: include the file metadata and
    // file partitions for this file.
    // This is a disadvantage for a * query: files at different directory
    // levels will have different numbers of columns. Would be better to
    // return this data as an array at some point.
    // Append this after the *, keeping the * for later expansion.

    for (FileMetadataColumnDefn iCol : metadataProj.fileMetadataColDefns()) {
      outputCols.add(new FileMetadataColumn(
          iCol.colName(), iCol, fileInfo));
    }
    for (int i = 0; i < fileInfo.dirPathLength(); i++) {
      outputCols.add(new PartitionColumn(
          metadataProj.partitionName(i), i, fileInfo));
    }
  }

  @Override
  public List<ColumnProjection> output() { return outputCols; }

//  // Temporary
//
//  public ColumnsArrayProjection columnsArrayProjection() { return metadataProj.columnsArrayProjection(); }

//  /**
//   * Create a fully-resolved projection plan given a file plan and a table
//   * schema
//   * @param tableSchema schema for the table (early-schema) or batch
//   * (late-schema)
//   * @return a fully-resolved projection plan
//   */
//
//  public TableLevelProjection resolve(TupleMetadata tableSchema) {
//   return new TableLevelProjection(this, tableSchema, isReresolution);
//  }

//  public static FileLevelProjection fromResolution(
//      ScanLevelProjection scanProj,
//      FileMetadataManager metadataProj,
//      FileMetadata fileMetadata) {
//    return new FileLevelProjection(scanProj, metadataProj, fileMetadata);
//  }
//
//  public static FileLevelProjection fromReresolution(
//      ScanLevelProjection scanProj,
//      FileMetadataManager metadataProj,
//      List<ColumnProjection> generatedSelect,
//      FileMetadata fileInfo) {
//    return new FileLevelProjection(scanProj, metadataProj, generatedSelect, fileInfo);
//  }
}
