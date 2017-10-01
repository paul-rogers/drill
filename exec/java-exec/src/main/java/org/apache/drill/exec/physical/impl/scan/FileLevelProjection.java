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
package org.apache.drill.exec.physical.impl.scan;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.impl.scan.ScanLevelProjection.FileMetadata;
import org.apache.drill.exec.physical.impl.scan.ScanLevelProjection.FileMetadataColumnDefn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.MetadataColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.WildcardColumn;
import org.apache.drill.exec.record.TupleMetadata;

import com.google.common.annotations.VisibleForTesting;

/**
 * Represents a partially-resolved projection list with the per-file metadata
 * columns (if any) populated. Must be combined with the table schema to
 * produce the final, fully-resolved projection list.
 */

public class FileLevelProjection {

  /**
   * Given an unresolved projection list, possibly with placeholder metadata
   * columns, produce a new, partially resolved list with the metadata
   * columns replaced by concrete versions with associated values. (Metadata
   * columns are constant for an entire file.)
   */

  private static class FileSchemaBuilder extends ScanOutputColumn.Visitor {
    private ScanLevelProjection scanProj;
    private FileMetadata fileInfo;
    private List<ScanOutputColumn> outputCols = new ArrayList<>();
    private List<MetadataColumn> metadataColumns = new ArrayList<>();

    public FileSchemaBuilder(ScanLevelProjection plan, List<ScanOutputColumn> inputCols, FileMetadata fileInfo) {
      this.scanProj = plan;
      this.fileInfo = fileInfo;
      visit(inputCols);
    }

    @Override
    protected void visitPartitionColumn(int index, PartitionColumn col) {
      addMetadataColumn(col.cloneWithValue(fileInfo));
    }

    @Override
    protected void visitFileInfoColumn(int index, FileMetadataColumn col) {
      addMetadataColumn(col.cloneWithValue(fileInfo));
    }

    private void addMetadataColumn(MetadataColumn col) {
      outputCols.add(col);
      metadataColumns.add(col);
    }

    @Override
    protected void visitWildcard(int index, WildcardColumn col) {
      visitColumn(index, col);

      // Skip wildcard expansion if not legacy or if the data source
      // does not provide file information.

      if (! scanProj.useLegacyWildcardPartition() || ! fileInfo.isSet()) {
        return;
      }

      // Legacy wildcard expansion: include the file metadata and
      // file partitions for this file.
      // This is a disadvantage for a * query: files at different directory
      // levels will have different numbers of columns. Would be better to
      // return this data as an array at some point.
      // Append this after the *, keeping the * for later expansion.

      for (FileMetadataColumnDefn iCol : scanProj.fileMetadataColDefns()) {
        addMetadataColumn(FileMetadataColumn.fromWildcard(col.source(),
            iCol, fileInfo));
      }
      for (int i = 0; i < fileInfo.dirPathLength(); i++) {
        addMetadataColumn(PartitionColumn.fromWildcard(col.source(),
            scanProj.partitionName(i), i, fileInfo));
      }
    }

    @Override
    protected void visitColumn(int index, ScanOutputColumn col) {
      outputCols.add(col);
    }
  }

  private final ScanLevelProjection scanProjection;
  private final List<ScanOutputColumn> outputCols;
  private final List<MetadataColumn> metadataColumns;
  private final boolean isReresolution;

  private FileLevelProjection(ScanLevelProjection scanProjDefn, FileMetadata fileInfo) {
    this(scanProjDefn, scanProjDefn.outputCols(), fileInfo);
  }

  private FileLevelProjection(ScanLevelProjection scanProjDefn,
      List<ScanOutputColumn> inputCols, FileMetadata fileInfo) {
    this.scanProjection = scanProjDefn;
    isReresolution = inputCols != scanProjDefn.outputCols();

    // If the projection plan has file or partition metadata
    // columns, rewrite them with actual file information.

    if (scanProjDefn.hasMetadata()) {
      FileSchemaBuilder builder = new FileSchemaBuilder(scanProjDefn, inputCols, fileInfo);
      outputCols = builder.outputCols;
      metadataColumns = builder.metadataColumns;
    } else {

      // No file or partition columns, just use the unresolved
      // query projection plan as-is.

      outputCols = inputCols;
      metadataColumns = null;
    }
  }

  public ScanLevelProjection scanProjection() { return scanProjection; }
  public List<ScanOutputColumn> output() { return outputCols; }
  public boolean hasMetadata() { return metadataColumns != null && ! metadataColumns.isEmpty(); }
  public List<MetadataColumn> metadataColumns() { return metadataColumns; }

  /**
   * Create a fully-resolved projection plan given a file plan and a table
   * schema
   * @param tableSchema schema for the table (early-schema) or batch
   * (late-schema)
   * @return a fully-resolved projection plan
   */

  public TableLevelProjection resolve(TupleMetadata tableSchema) {
    if (isReresolution) {
      return TableLevelProjection.fromReresolution(this, tableSchema);
    } else {
      return TableLevelProjection.fromResolution(this, tableSchema);
    }
  }

  @VisibleForTesting
  public TupleMetadata outputSchema() {
    return ScanOutputColumn.schema(output());
  }

  public static FileLevelProjection fromResolution(
      ScanLevelProjection scanProj, FileMetadata fileMetadata) {
    return new FileLevelProjection(scanProj, fileMetadata);
  }

  public static FileLevelProjection fromReresolution(
      ScanLevelProjection scanProj, List<ScanOutputColumn> generatedSelect,
      FileMetadata fileInfo) {
    return new FileLevelProjection(scanProj, generatedSelect, fileInfo);
  }
}
