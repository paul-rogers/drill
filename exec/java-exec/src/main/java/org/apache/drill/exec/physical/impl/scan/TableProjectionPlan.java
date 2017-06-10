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
import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ColumnsArrayColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.NullColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ProjectedColumn;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.ImplicitColumnExplorer;

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

public class TableProjectionPlan {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableProjectionPlan.class);

  private static class BaseTableColumnVisitor extends OutputColumn.Visitor {

    protected MaterializedSchema tableSchema;
    protected List<OutputColumn> outputCols = new ArrayList<>();
    protected boolean selectionMap[];
    protected int logicalToPhysicalMap[];
    protected int projectionMap[];
    protected int selectionCount;

    public BaseTableColumnVisitor(MaterializedSchema tableSchema) {
      this.tableSchema = tableSchema;
    }

    @Override
    protected void visitColumn(int index, OutputColumn col) {
      outputCols.add(col);
    }

    protected void addTableColumn(int colIndex, ProjectedColumn col) {
      projectionMap[colIndex] = outputCols.size();
      selectionMap[colIndex] = true;
      selectionCount++;
      outputCols.add(col);
    }
  }

  /**
   * Given a partially-resolved select list, possibly with a wildcard
   * column, produce a new select list with the wildcard columns
   * replaced with the actual list of table columns,
   * in table schema order. Note that the list may contain metadata
   * columns, either before the wildcard, after, or both.
   */

  private static class WildcardExpander extends BaseTableColumnVisitor {

    public WildcardExpander(MaterializedSchema tableSchema) {
      super(tableSchema);
      selectionMap = new boolean[tableSchema.size()];
      projectionMap = new int[tableSchema.size()];

      // For SELECT *, columns in the output are the full set of
      // table columns, in table column order.

      logicalToPhysicalMap = new int[tableSchema.size()];
      for (int i = 0; i < logicalToPhysicalMap.length; i++) {
        logicalToPhysicalMap[i] = i;
      }
    }

    @Override
    protected void visitWildcard(int index, OutputColumn.WildcardColumn col) {
      for (int i = 0; i < tableSchema.size(); i++) {
        addTableColumn(i, new ProjectedColumn(col.inCol,
            tableSchema.column(i), tableSchema.column(i).getName()));
      }
    }
  }

  /**
   * Given a partially resolved select list, with table column references
   * (column names), create a new, fully resolved select list with the
   * column references replaced with either table columns references or
   * null columns (if no such column exists in the table.)
   */

  public static class TableSchemaProjection extends BaseTableColumnVisitor {

    protected List<NullColumn> nullCols = new ArrayList<>();
    protected int nullProjectionMap[];

    public TableSchemaProjection(MaterializedSchema tableSchema) {
      super(tableSchema);
      selectionMap = new boolean[tableSchema.size()];
      logicalToPhysicalMap = new int[tableSchema.size()];
      Arrays.fill(logicalToPhysicalMap, -1);
      projectionMap = new int[tableSchema.size()];
      Arrays.fill(projectionMap, -1);
    }

    @Override
    public void visit(List<OutputColumn> input) {
      nullProjectionMap = new int[input.size()];
      super.visit(input);

      // Construct the logical (full table schema) to physical
      // (selected columns only) map.

      int physicalIndex = 0;
      for (int i = 0; i < selectionMap.length; i++) {
        logicalToPhysicalMap[i] = selectionMap[i] ? physicalIndex++ : -1;
      }
    }

    @Override
    protected void visitTableColumn(int index, OutputColumn.ExpectedTableColumn col) {
      int tableColIndex = tableSchema.index(col.name());
      if (tableColIndex == -1) {
        NullColumn nullCol = new NullColumn(col.inCol);
        nullProjectionMap[nullCols.size()] = outputCols.size();
        outputCols.add(nullCol);
        nullCols.add(nullCol);
      } else {
        addTableColumn(tableColIndex, new ProjectedColumn(
              col.inCol, tableSchema.column(tableColIndex), col.inCol.name()));
      }
    }
  }

  /**
   * Turn the "columns" column into a made-up table column (that nonetheless
   * appears in the table loader.) The list may also contain metadata columns.
   */

  public static class ColumnsArrayProjection extends BaseTableColumnVisitor {

    public ColumnsArrayProjection(MaterializedSchema tableSchema) {
      super(tableSchema);
      selectionMap = new boolean[] { true };
      logicalToPhysicalMap = new int[] { 0 };
      projectionMap = new int[1];
   }

    @Override
    protected void visitColumnsArray(int index, ColumnsArrayColumn col) {
      addTableColumn(0, new ProjectedColumn(
          col.inCol, MajorType.newBuilder()
              .setMinorType(MinorType.VARCHAR)
              .setMode(DataMode.REPEATED)
              .build()));
    }
  }

  private final QuerySelectionPlan selectionPlan;
  private final MaterializedSchema tableSchema;
  private final List<OutputColumn> outputCols;

  /**
   * Map, in table schema order, indicating which columns are selected.
   * The number of entries is the same as the table schema size.
   */

  private final boolean[] selectionMap;

  /**
   * Map, in table schema order, indicating the physical column position.
   * Differs from the logical position when a subset of columns are selected.
   */

  private final int[] logicalToPhysicalMap;

  /**
   * Map, in physical table schema order, of the output column position for each
   * selected table column. The number of valid entries is the physical table
   * schema size.
   */

  private final int[] tableColumnProjectionMap;
  private final int[] nullProjectionMap;

  protected final List<NullColumn> nullCols;

  public TableProjectionPlan(FileSelectionPlan filePlan, MaterializedSchema tableSchema) {
    selectionPlan = filePlan.selectionPlan();
    this.tableSchema = tableSchema;
    BaseTableColumnVisitor baseBuilder;
    switch (selectionPlan.selectType()) {

    // SELECT a, b, c

    case LIST:
      TableSchemaProjection schemaProj = new TableSchemaProjection(tableSchema);
      schemaProj.visit(filePlan.output());
      nullCols = schemaProj.nullCols;
      nullProjectionMap = schemaProj.nullProjectionMap;
      baseBuilder = schemaProj;
      break;

    // SELECT *

    case WILDCARD:
      WildcardExpander expander = new WildcardExpander(tableSchema);
      expander.visit(filePlan.output());
      nullCols = null;
      nullProjectionMap = null;
      baseBuilder = expander;
      break;

    // SELECT columns

    case COLUMNS_ARRAY:
      validateColumnsArray(filePlan.output());
      ColumnsArrayProjection colArrayProj = new ColumnsArrayProjection(tableSchema);
      colArrayProj.visit(filePlan.output());
      nullCols = null;
      nullProjectionMap = null;
      baseBuilder = colArrayProj;
      break;
    default:
      throw new IllegalStateException("Unexpected selection type: " + selectionPlan.selectType());
    }
    outputCols = baseBuilder.outputCols;
    selectionMap = baseBuilder.selectionMap;
    logicalToPhysicalMap = baseBuilder.logicalToPhysicalMap;
    tableColumnProjectionMap = baseBuilder.projectionMap;
  }

  public QuerySelectionPlan selectionPlan() { return selectionPlan; }
  public List<OutputColumn> output() { return outputCols; }
  public boolean[] selectionMap() { return selectionMap; }
  public int[] logicalToPhysicalMap() { return logicalToPhysicalMap; }
  public int[] tableColumnProjectionMap() { return tableColumnProjectionMap; }
  public boolean hasNullColumns() { return nullCols != null && ! nullCols.isEmpty(); }
  public List<NullColumn> nullColumns() { return nullCols; }
  public int[] nullProjectionMap() { return nullProjectionMap; }

  public List<String> selectedTableColumns() {
    List<String> selection = new ArrayList<>();
    for (int i = 0; i < selectionMap.length; i++) {
      if (selectionMap[i]) {
        selection.add(tableSchema.column(i).getName());
      }
    }
    return selection;
  }

  private void validateColumnsArray(List<OutputColumn> input) {

    // All columns must be required Varchar: the type of the array elements

    for (int i = 0; i < tableSchema.size(); i++) {
      if (! isArrayCompatible(tableSchema.column(i))) {
        throw UserException.validationError()
            .addContext("SELECT `columns`, but column is not Required Varchar", tableSchema.column(i).getName())
            .build(logger);
      }
    }
  }

  private boolean isArrayCompatible(MaterializedField column) {
    MajorType type = column.getType();
    if (type.getMode() != DataMode.REQUIRED) {
      return false;
    }
    return type.getMinorType() == MinorType.VARCHAR;
  }
}
