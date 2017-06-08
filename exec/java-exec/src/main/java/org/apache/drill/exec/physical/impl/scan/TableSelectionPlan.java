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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ResolvedFileInfo;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.TableSchemaType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.ImplicitColumnExplorer;
import org.apache.hadoop.fs.Path;

/**
 * Computes the full output schema given a table (or batch)
 * schema. Takes the original, unresolved output list from the selection
 * plan, merges it with the file, directory and table schema information,
 * and produces a partially or fully resolved output list.
 * <p>
 * A "resolved" select list is a list of concrete columns: table
 * columns, nulls, file metadata or partition metadata. An unresolved list
 * has either table column names, but no match, or a wildcard column.
 * <p>
 * The idea is that the selection list moves through stages of resolution
 * depending on which information is available. An "early schema" table
 * provides schema information up front, and so allows fully resolving
 * the select list on table open. A "late schema" table allows only a
 * partially resolves select list, with the remainder of resolution
 * happening on the first (or perhaps every) batch.
 * <p>
 * Data source (table) schema can be of two forms:
 * <ul>
 * <li>Early schema: the schema is known before reading data. A JDBC data
 * source is an example, as is a CSV reader for a file with headers.</li>
 * <li>Late schema: the schema is not known until data is read, and is
 * discovered on the fly. Example: JSON, which declares values as maps
 * without an up-front schema.</li>
 * </ul>
 * These two forms give rise to distinct ways of planning the projection.
 * <p>
 * The final result of the projection is a set of "output" columns: a set
 * of columns that, taken together, defines the row (bundle of vectors) that
 * the scan operator produces. Columns are ordered: the order specified here
 * must match the order that columns appear in the result set loader and the
 * vector container so that code can access columns by index as well as name.
 *
 * @see {@link ImplicitColumnExplorer}, the class from which this class
 * evolved
 */

public class TableSelectionPlan {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableSelectionPlan.class);

  /**
   * Build a table selection plan given a query selection plan, file
   * metadata, and either an early- or late- table schema.
   */

  public static class TableSelectionBuilder {

    SelectionListPlan selectionPlan;
    private ResolvedFileInfo fileInfo;
    private TableSchemaType tableSchemaType = TableSchemaType.LATE;
    MaterializedSchema tableSchema;
    boolean isResolved;
    List<OutputColumn> outputSchema;

    public TableSelectionBuilder(SelectionListPlan selectionPlan) {
      this.selectionPlan = selectionPlan;
    }

    public void setFileInfo(Path fileName, Path rootDir) {
      fileInfo = new ResolvedFileInfo(fileName, rootDir);
    }

    public void setFileInfo(Path fileName, String rootDir) {
      fileInfo = new ResolvedFileInfo(fileName, rootDir);
    }

    public void setTableSchemaType(TableSchemaType schemaType) {
      tableSchemaType = schemaType;
    }

    public void setTableSchema(MaterializedSchema tableSchema) {
      this.tableSchema = tableSchema;
      tableSchemaType = TableSchemaType.EARLY;
    }

    public TableSelectionPlan build() {
      TableSelectionPlan.FileSchemaBuilder fsBuilder = new TableSelectionPlan.FileSchemaBuilder(selectionPlan, fileInfo);
      List<OutputColumn> fileExpansion = fsBuilder.build();
      switch (selectionPlan.selectType()) {
      case COLUMNS_ARRAY:
        outputSchema = planColumnsArray(fileExpansion);
        break;
      case LIST:
        outputSchema = planSelectList(fileExpansion);
        break;
      case WILDCARD:
        outputSchema = planWildcard(fileExpansion);
        break;
      default:
        throw new IllegalStateException("Unexpected selection type: " + selectionPlan.selectType());
      }
      return new TableSelectionPlan(this);
    }

    private List<OutputColumn> planColumnsArray(List<OutputColumn> input) {

      // All columns must be required Varchar: the type of the array elements

      for (int i = 0; i < tableSchema.size(); i++) {
        if (! isArrayCompatible(tableSchema.column(i))) {
          throw UserException.validationError()
              .addContext("SELECT `columns`, but column is not Required Varchar", tableSchema.column(i).getName())
              .build(logger);
        }
      }
      isResolved = true;
      return input;
    }

    private boolean isArrayCompatible(MaterializedField column) {
      MajorType type = column.getType();
      if (type.getMode() != DataMode.REQUIRED) {
        return false;
      }
      return type.getMinorType() == MinorType.VARCHAR;
    }

    private List<OutputColumn> planSelectList(List<OutputColumn> input) {
      isResolved = tableSchemaType == TableSchemaType.EARLY;
      return input;
    }

    private List<OutputColumn> planWildcard(List<OutputColumn> input) {
      isResolved = tableSchemaType == TableSchemaType.EARLY;
      if (! isResolved) {
        return input;
      }
      TableSelectionPlan.WildcardExpander expander = new TableSelectionPlan.WildcardExpander(tableSchema);
      return expander.build(input);
    }
  }

  /**
   * Given an unresolved select list, possibly with placeholder metadata
   * columns, produce a new, partially resolved list with the metadata
   * columns replaced by concrete versions with associated values. (Metadata
   * columns are constant for an entire file.)
   */

  private static class FileSchemaBuilder extends OutputColumn.Visitor {
    SelectionListPlan plan;
    OutputColumn.ResolvedFileInfo fileInfo;
    List<OutputColumn> outputCols = new ArrayList<>();

    public FileSchemaBuilder(SelectionListPlan plan,
        OutputColumn.ResolvedFileInfo fileInfo) {
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
    protected void visitColumn(OutputColumn col) {
      outputCols.add(col);
    }
  }

  /**
   * Given a partially-resolved select list, possibly with a wildcard
   * column, produce a new select list with the wildcard columns
   * replaced with the actual list of table columns,
   * in table schema order.
   */

  private static class WildcardExpander extends OutputColumn.Visitor {
    MaterializedSchema tableSchema;
    List<OutputColumn> outputCols = new ArrayList<>();

    public WildcardExpander(MaterializedSchema tableSchema) {
      this.tableSchema = tableSchema;
    }

    public List<OutputColumn> build(List<OutputColumn> input) {
      visit(input);
      return outputCols;
    }

    @Override
    protected void visitWildcard(OutputColumn.WildcardColumn col) {
      for (int i = 0; i < tableSchema.size(); i++) {
        outputCols.add(new OutputColumn.ProjectedColumn(col.inCol, i,
            tableSchema.column(i)));
      }
    }

    @Override
    protected void visitColumn(OutputColumn col) {
      outputCols.add(col);
    }
  }

  /**
   * Given a partially resolved select list, with table column references
   * (column names), create a new, fully resolved select list with the
   * column references replaced with either table columns references or
   * null columns (if no such column exists in the table.)
   */

  public static class TableSchemaProjection extends OutputColumn.Visitor {
    MaterializedSchema tableSchema;
    List<OutputColumn> outputCols = new ArrayList<>();
    boolean tableColumnProjected[];

    public TableSchemaProjection(MaterializedSchema tableSchema) {
      this.tableSchema = tableSchema;
      tableColumnProjected = new boolean[tableSchema.size()];
    }

    public List<OutputColumn> build(List<OutputColumn> input) {
      visit(input);
      return outputCols;
    }

    public boolean[] projectionMap() { return tableColumnProjected; }

    public int[] logicalToPhysicalMap() {
      int lToPMap[] = new int[tableColumnProjected.length];
      int physicalIndex = 0;
      for (int i = 0; i < tableColumnProjected.length; i++) {
        lToPMap[i] = tableColumnProjected[i] ? physicalIndex++ : -1;
      }
      return lToPMap;
    }

    @Override
    protected void visitTableColumn(OutputColumn.ExpectedTableColumn col) {
      int index = tableSchema.index(col.name());
      if (index == -1) {
        outputCols.add(new OutputColumn.NullColumn(col.inCol));
      } else {
        outputCols.add(new OutputColumn.ProjectedColumn(col.inCol, index, tableSchema.column(index)));
        tableColumnProjected[index] = true;
      }
    }

    @Override
    protected void visitColumn(OutputColumn col) {
      outputCols.add(col);
    }
  }

  private SelectionListPlan selectionPlan;
  private MaterializedSchema tableSchema;
  private boolean isResolved;
  private List<OutputColumn> outputSchema;

  public TableSelectionPlan(
      TableSelectionPlan.TableSelectionBuilder builder) {
    this.selectionPlan = builder.selectionPlan;
    this.tableSchema = builder.tableSchema;
    this.isResolved = builder.isResolved;
    this.outputSchema = builder.outputSchema;
  }

  public TableSelectionPlan(TableSelectionPlan base, List<OutputColumn> newSchema) {
    this.selectionPlan = base.selectionPlan;
    this.tableSchema = base.tableSchema;
    this.isResolved = true;
    this.outputSchema = newSchema;
  }

  /**
   * If the table selection plan is partially resolved, create a new, fully
   * resolved plan using the schema provided. This step is used on schema
   * changes for late-schema tables.
   * @param newSchema the table schema
   * @return a new, fully-resolved table selection plan
   */

  public TableSelectionPlan replan(MaterializedSchema newSchema) {
    assert ! isResolved;
    switch (selectionPlan.selectType()) {
    case LIST:
      TableSelectionPlan.TableSchemaProjection schemaProj = new TableSelectionPlan.TableSchemaProjection(tableSchema);
      return new TableSelectionPlan(this, schemaProj.build(outputSchema));
    case WILDCARD:
      TableSelectionPlan.WildcardExpander expander = new TableSelectionPlan.WildcardExpander(tableSchema);
      return new TableSelectionPlan(this, expander.build(outputSchema));
    default:
      throw new IllegalStateException("Unexpected selection type: " + selectionPlan.selectType());
    }
  }

  public boolean isResolved() { return isResolved; }
  public List<OutputColumn> output() { return outputSchema; }
}