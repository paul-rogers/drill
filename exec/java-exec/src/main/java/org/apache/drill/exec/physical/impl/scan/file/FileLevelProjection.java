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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayProjection;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedMetadataColumn.ResolvedFileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedMetadataColumn.ResolvedPartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.Exp.UnresolvedProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;
import org.apache.drill.exec.record.TupleMetadata;

/**
 * Represents a partially-resolved projection list with the per-file metadata
 * columns (if any) populated. Must be combined with the table schema to
 * produce the final, fully-resolved projection list.
 */

public class FileLevelProjection implements UnresolvedProjection {

  /**
   * Given an unresolved projection list, possibly with placeholder metadata
   * columns, produce a new, partially resolved list with the metadata
   * columns replaced by concrete versions with associated values. (Metadata
   * columns are constant for an entire file.)
   */

  private static class FileSchemaBuilder {
    private final FileMetadataProjection metadataProj;
    private final FileMetadata fileInfo;
    private final List<ColumnProjection> outputCols = new ArrayList<>();
    private final List<ResolvedMetadataColumn> metadataColumns = new ArrayList<>();

    public FileSchemaBuilder(FileMetadataProjection metadataProj, List<ColumnProjection> inputCols, FileMetadata fileInfo) {
      this.metadataProj = metadataProj;
      this.fileInfo = fileInfo;
      for (ColumnProjection col : inputCols) {
        translate(col);
      }
    }

    private void translate(ColumnProjection col) {
      switch (col.nodeType()) {
      case UnresolvedColumn.WILDCARD:
        outputCols.add(col);
        expandWildcard((UnresolvedColumn) col);
        break;

      case UnresolvedPartitionColumn.ID:
        addMetadataColumn(((UnresolvedPartitionColumn) col).resolve(fileInfo));
        break;

      case UnresolvedFileMetadataColumn.ID:
        addMetadataColumn(((UnresolvedFileMetadataColumn) col).resolve(fileInfo));
        break;

      default:
        outputCols.add(col);
      }
    }

    private void addMetadataColumn(ResolvedMetadataColumn col) {
      outputCols.add(col);
      metadataColumns.add(col);
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
        addMetadataColumn(new ResolvedFileMetadataColumn(
            iCol.colName(), col, iCol, fileInfo));
      }
      for (int i = 0; i < fileInfo.dirPathLength(); i++) {
        addMetadataColumn(new ResolvedPartitionColumn(
            metadataProj.partitionName(i), col, i, fileInfo));
      }
    }
  }

  private final UnresolvedProjection scanProjection;
  private final FileMetadataProjection metadataProj;
  private final List<ColumnProjection> outputCols;
  private final List<ResolvedMetadataColumn> metadataColumns;
  private final boolean isReresolution;

  private FileLevelProjection(ScanLevelProjection scanProjDefn, FileMetadataProjection metadataProj, FileMetadata fileInfo) {
    this(scanProjDefn, metadataProj, scanProjDefn.outputCols(), fileInfo);
  }

  private FileLevelProjection(UnresolvedProjection scanProjDefn,
      FileMetadataProjection metadataProj,
      List<ColumnProjection> inputCols,
      FileMetadata fileInfo) {
    this.scanProjection = scanProjDefn;
    this.metadataProj = metadataProj;
    isReresolution = inputCols != scanProjDefn.outputCols();

    // If the projection plan has file or partition metadata
    // columns, rewrite them with actual file information.

    if (metadataProj.hasMetadata()) {
      FileSchemaBuilder builder = new FileSchemaBuilder(metadataProj, inputCols, fileInfo);
      outputCols = builder.outputCols;
      metadataColumns = builder.metadataColumns;
    } else {

      // No file or partition columns, just use the unresolved
      // query projection plan as-is.

      outputCols = inputCols;
      metadataColumns = null;
    }
  }

  @Override
  public List<ColumnProjection> outputCols() { return outputCols; }

  public UnresolvedProjection scanProjection() { return scanProjection; }
  public boolean hasMetadata() { return metadataColumns != null && ! metadataColumns.isEmpty(); }
  public List<ResolvedMetadataColumn> metadataColumns() { return metadataColumns; }

  @Override
  public boolean projectAll() { return scanProjection.projectAll(); }

  @Override
  public MajorType nullType() { return scanProjection.nullType(); }

  // Temporary

  public ColumnsArrayProjection columnsArrayProjection() { return metadataProj.columnsArrayProjection(); }

  /**
   * Create a fully-resolved projection plan given a file plan and a table
   * schema
   * @param tableSchema schema for the table (early-schema) or batch
   * (late-schema)
   * @return a fully-resolved projection plan
   */

  public TableLevelProjection resolve(TupleMetadata tableSchema) {
   return new TableLevelProjection(this, tableSchema, isReresolution);
  }

//  @VisibleForTesting
//  public TupleMetadata outputSchema() {
//    return UnresolvedColumn.schema(output());
//  }

  public static FileLevelProjection fromResolution(
      ScanLevelProjection scanProj,
      FileMetadataProjection metadataProj,
      FileMetadata fileMetadata) {
    return new FileLevelProjection(scanProj, metadataProj, fileMetadata);
  }

  public static FileLevelProjection fromReresolution(
      ScanLevelProjection scanProj,
      FileMetadataProjection metadataProj,
      List<ColumnProjection> generatedSelect,
      FileMetadata fileInfo) {
    return new FileLevelProjection(scanProj, metadataProj, generatedSelect, fileInfo);
  }
}
