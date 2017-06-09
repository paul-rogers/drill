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
            tableSchema.column(i), tableSchema.column(i).getName()));
      }
    }

    @Override
    protected void visitColumn(OutputColumn col) {
      outputCols.add(col);
    }

    public boolean[] projectionMap() {
      boolean map[] = new boolean[tableSchema.size()];
      Arrays.fill(map, true);
      return map;
    }

    public int[] logicalToPhysicalMap() {
      int map[] = new int[tableSchema.size()];
      for (int i = 0; i < map.length; i++) {
        map[i] = i;
      }
      return map;
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
        outputCols.add(new OutputColumn.ProjectedColumn(col.inCol, index, tableSchema.column(index), col.inCol.name()));
        tableColumnProjected[index] = true;
      }
    }

    @Override
    protected void visitColumn(OutputColumn col) {
      outputCols.add(col);
    }
  }

  private final QuerySelectionPlan selectionPlan;
  private final MaterializedSchema tableSchema;
  private final List<OutputColumn> outputCols;
  private final boolean[] selectionMap;
  private final int[] logicalToPhysicalMap;

  public TableProjectionPlan(FileSelectionPlan filePlan, MaterializedSchema tableSchema) {
    selectionPlan = filePlan.selectionPlan();
    this.tableSchema = tableSchema;
    switch (selectionPlan.selectType()) {
    case LIST:
      TableSchemaProjection schemaProj = new TableSchemaProjection(tableSchema);
      outputCols = schemaProj.build(filePlan.output());
      selectionMap = schemaProj.projectionMap();
      logicalToPhysicalMap = schemaProj.logicalToPhysicalMap();
      break;
    case WILDCARD:
      WildcardExpander expander = new WildcardExpander(tableSchema);
      outputCols = expander.build(filePlan.output());
      selectionMap = expander.projectionMap();
      logicalToPhysicalMap = expander.logicalToPhysicalMap();
      break;
    case COLUMNS_ARRAY:
      validateColumnsArray(filePlan.output());
      outputCols = filePlan.output();
      selectionMap = new boolean[] { true };
      logicalToPhysicalMap = new int[] { 0 };
      break;
    default:
      throw new IllegalStateException("Unexpected selection type: " + selectionPlan.selectType());
    }
  }

  public QuerySelectionPlan selectionPlan() { return selectionPlan; }
  public List<OutputColumn> output() { return outputCols; }
  protected boolean[] selectionMap() { return selectionMap; }
  protected int[] logicalToPhysicalMap() { return logicalToPhysicalMap; }

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
