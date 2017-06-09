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

import org.apache.drill.exec.physical.impl.scan.OutputColumn.ExpectedTableColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ResolvedFileInfo;

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
    private OutputColumn.ResolvedFileInfo fileInfo;
    private List<OutputColumn> outputCols = new ArrayList<>();
    private List<String> tableSelection = new ArrayList<>();

    public FileSchemaBuilder(QuerySelectionPlan plan, ResolvedFileInfo fileInfo) {
      this.plan = plan;
      this.fileInfo = fileInfo;
    }

    public List<OutputColumn> build() {
      visit(plan);
      return outputCols;
    }

    @Override
    protected void visitPartitionColumn(OutputColumn.PartitionColumn col) {
      outputCols.add(col.cloneWithValue(fileInfo));
    }

    @Override
    protected void visitFileInfoColumn(OutputColumn.FileMetadataColumn col) {
      outputCols.add(col.cloneWithValue(fileInfo));
    }

    @Override
    protected void visitWildcard(OutputColumn.WildcardColumn col) {
      visitColumn(col);
      if (plan.useLegacyWildcardPartition()) {
        for (int i = 0; i < fileInfo.dirPathLength(); i++) {
          outputCols.add(new OutputColumn.PartitionColumn(col.source(), i,
              fileInfo.partition(i)));
        }
      }
    }

    @Override
    protected void visitTableColumn(ExpectedTableColumn col) {
      tableSelection.add(col.name());
      visitColumn(col);
    }

    @Override
    protected void visitColumn(OutputColumn col) {
      outputCols.add(col);
    }
  }

  private final QuerySelectionPlan selectionPlan;
  private final List<OutputColumn> outputCols;
  private final List<String> tableSelection;

  public FileSelectionPlan(QuerySelectionPlan selectionPlan, ResolvedFileInfo fileInfo) {
    this.selectionPlan = selectionPlan;
    FileSchemaBuilder builder = new FileSchemaBuilder(selectionPlan, fileInfo);
    outputCols = builder.build();
    tableSelection = builder.tableSelection;
  }

  public QuerySelectionPlan selectionPlan() { return selectionPlan; }
  public List<OutputColumn> output() { return outputCols; }
  public List<String> tableSelection() { return tableSelection; }

  public TableProjectionPlan resolve(MaterializedSchema tableSchema) {
    return new TableProjectionPlan(this, tableSchema);
  }
}
