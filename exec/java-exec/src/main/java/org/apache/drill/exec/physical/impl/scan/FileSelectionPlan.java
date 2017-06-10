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

import org.apache.drill.exec.physical.impl.scan.OutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.MetadataColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ResolvedFileInfo;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.WildcardColumn;
import org.apache.drill.exec.physical.impl.scan.QuerySelectionPlan.FileMetadataColumnDefn;

/**
 * Represents a partially-resolved select list with the per-file metadata
 * columns (if any) populated. Must be combined with the table schema to
 * produce the final, fully-resolved selection list.
 */

public class FileSelectionPlan {

  /**
   * Given an unresolved select list, possibly with placeholder metadata
   * columns, produce a new, partially resolved list with the metadata
   * columns replaced by concrete versions with associated values. (Metadata
   * columns are constant for an entire file.)
   */

  private static class FileSchemaBuilder extends OutputColumn.Visitor {
    private QuerySelectionPlan plan;
    private ResolvedFileInfo fileInfo;
    private List<OutputColumn> outputCols = new ArrayList<>();
    private List<MetadataColumn> metadataColumns = new ArrayList<>();
    private int metadataProjection[];

    public FileSchemaBuilder(QuerySelectionPlan plan, ResolvedFileInfo fileInfo) {
      this.plan = plan;
      this.fileInfo = fileInfo;
      int mapLen = plan.outputCols().size();
      if (plan.useLegacyWildcardPartition()) {
        mapLen += fileInfo.dirPathLength() + plan.implicitColDefns().size();
      }
      metadataProjection = new int[mapLen];
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
      metadataProjection[metadataColumns.size()] = outputCols.size();
      outputCols.add(col);
      metadataColumns.add(col);
    }

    @Override
    protected void visitWildcard(int index, WildcardColumn col) {
      visitColumn(index, col);
      if (! plan.useLegacyWildcardPartition()) {
        return;
      }

      // Legacy wildcard expansion: include the file metadata and
      // file partitions for this file.
      // This is a disadvantage for a * query: files at different directory
      // levels will have different numbers of columns. Would be better to
      // return this data as an array at some point.
      // Append this after the *, keeping the * for later expansion.

      for (FileMetadataColumnDefn iCol : plan.implicitColDefns()) {
        addMetadataColumn(new FileMetadataColumn(col.source(),
            iCol, iCol.colName(), fileInfo));
      }
      for (int i = 0; i < fileInfo.dirPathLength(); i++) {
        addMetadataColumn(new PartitionColumn(col.source(), i,
            plan.partitionName(i), fileInfo));
      }
    }

    @Override
    protected void visitColumn(int index, OutputColumn col) {
      outputCols.add(col);
    }
  }

  private final QuerySelectionPlan selectionPlan;
  private final List<OutputColumn> outputCols;
  private final List<MetadataColumn> metadataColumns;
  private final int metadataProjection[];

  public FileSelectionPlan(QuerySelectionPlan selectionPlan, ResolvedFileInfo fileInfo) {
    this.selectionPlan = selectionPlan;
    if (selectionPlan.hasMetadata()) {
      FileSchemaBuilder builder = new FileSchemaBuilder(selectionPlan, fileInfo);
      builder.visit(selectionPlan);
      outputCols = builder.outputCols;
      metadataColumns = builder.metadataColumns;
      metadataProjection = builder.metadataProjection;
    } else {
      outputCols = selectionPlan.outputCols();
      metadataColumns = null;
      metadataProjection = null;
    }
  }

  public QuerySelectionPlan selectionPlan() { return selectionPlan; }
  public List<OutputColumn> output() { return outputCols; }
  public boolean hasMetadata() { return metadataColumns != null && ! metadataColumns.isEmpty(); }
  public List<MetadataColumn> metadataColumns() { return metadataColumns; }

  /**
   * Metadata projection map.
   * @return a map, indexed by metadata column positions (as defined by
   * {@link #metadataColumns()}, to the position in the full output schema
   * as defined by {@link #output()}
   */
  public int[] metadataProjection() { return metadataProjection; }

  /**
   * Create a fully-resolved selection plan given a file plan and a table
   * schema
   * @param tableSchema schema for the table (early-schema) or batch
   * (late-schema)
   * @return a fully-resolved projection plan
   */

  public TableProjectionPlan resolve(MaterializedSchema tableSchema) {
    return new TableProjectionPlan(this, tableSchema);
  }
}
