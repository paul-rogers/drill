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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanLevelProjection.ProjectionType;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.ColumnsArrayColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.MetadataColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.NullColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.ProjectedColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.RequestedTableColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.WildcardColumn;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;

import com.google.common.annotations.VisibleForTesting;

/**
 * Computes the full output schema given a table (or batch)
 * schema. Takes the original, unresolved output list from the projection
 * definition, merges it with the file, directory and table schema information,
 * and produces a partially or fully resolved output list.
 * <p>
 * A "resolved" projection list is a list of concrete columns: table
 * columns, nulls, file metadata or partition metadata. An unresolved list
 * has either table column names, but no match, or a wildcard column.
 * <p>
 * The idea is that the projection list moves through stages of resolution
 * depending on which information is available. An "early schema" table
 * provides schema information up front, and so allows fully resolving
 * the projection list on table open. A "late schema" table allows only a
 * partially resolves projection list, with the remainder of resolution
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

public class TableLevelProjection {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableLevelProjection.class);

  private static class BaseTableColumnVisitor extends ScanOutputColumn.Visitor {

    protected TupleMetadata tableSchema;
    protected List<ScanOutputColumn> outputCols = new ArrayList<>();
    protected boolean columnIsProjected[];
    protected int logicalToPhysicalMap[];
    protected int projectionMap[];
    protected int selectedTableColumnCount;
    protected int metadataColumnCount;
    private int metadataProjection[];

    public BaseTableColumnVisitor(TupleMetadata tableSchema) {
      this.tableSchema = tableSchema;
    }

    @Override
    public void visit(List<ScanOutputColumn> cols) {
      metadataProjection = new int[cols.size()];
      super.visit(cols);
    }

    @Override
    protected void visitPartitionColumn(int index, PartitionColumn col) {
      addMetadataColumn(col);
    }

    @Override
    protected void visitFileInfoColumn(int index, FileMetadataColumn col) {
      addMetadataColumn(col);
    }

    private void addMetadataColumn(MetadataColumn col) {
      metadataProjection[metadataColumnCount++] = outputCols.size();
      outputCols.add(col);
    }

    @Override
    protected void visitColumn(int index, ScanOutputColumn col) {
      outputCols.add(col);
    }

    protected void addTableColumn(ProjectedColumn col) {
      int colIndex = col.columnIndex();
      projectionMap[colIndex] = outputCols.size();
      columnIsProjected[colIndex] = true;
      selectedTableColumnCount++;
      outputCols.add(col);
    }
  }

  /**
   * Given a partially-resolved projection list, possibly with a wildcard
   * column, produce a new projection list with the wildcard columns
   * replaced with the actual list of table columns,
   * in table schema order. Note that the list may contain metadata
   * columns, either before the wildcard, after, or both.
   */

  private static class WildcardExpander extends BaseTableColumnVisitor {

    public WildcardExpander(TupleMetadata tableSchema) {
      super(tableSchema);
      columnIsProjected = new boolean[tableSchema.size()];
      projectionMap = new int[tableSchema.size()];

      // For SELECT *, columns in the output are the full set of
      // table columns, in table column order.

      logicalToPhysicalMap = new int[tableSchema.size()];
      for (int i = 0; i < logicalToPhysicalMap.length; i++) {
        logicalToPhysicalMap[i] = i;
      }
    }

    @Override
    protected void visitWildcard(int index, WildcardColumn col) {
      for (int i = 0; i < tableSchema.size(); i++) {
        addTableColumn(ProjectedColumn.fromWildcard(col,
            i, tableSchema.column(i)));
      }
    }
  }

  /**
   * Given a partially resolved projection list, with table column references
   * (column names), create a new, fully resolved projection list with the
   * column references replaced with either table columns references or
   * null columns (if no such column exists in the table.)
   */

  public static class TableSchemaProjection extends BaseTableColumnVisitor {

    protected List<NullColumn> nullCols = new ArrayList<>();
    protected int nullProjectionMap[];

    public TableSchemaProjection(TupleMetadata tableSchema) {
      super(tableSchema);
      columnIsProjected = new boolean[tableSchema.size()];
      logicalToPhysicalMap = new int[tableSchema.size()];
      Arrays.fill(logicalToPhysicalMap, -1);
      projectionMap = new int[tableSchema.size()];
      Arrays.fill(projectionMap, -1);
    }

    @Override
    public void visit(List<ScanOutputColumn> input) {
      nullProjectionMap = new int[input.size()];
      super.visit(input);

      // Construct the logical (full table schema) to physical
      // (projected columns only) map.

      int physicalIndex = 0;
      for (int i = 0; i < columnIsProjected.length; i++) {
        logicalToPhysicalMap[i] = columnIsProjected[i] ? physicalIndex++ : -1;
      }
    }

    @Override
    protected void visitTableColumn(int index, RequestedTableColumn col) {
      int tableColIndex = tableSchema.index(col.name());
      if (tableColIndex == -1) {
        NullColumn nullCol = NullColumn.fromResolution(col);
        nullProjectionMap[nullCols.size()] = outputCols.size();
        outputCols.add(nullCol);
        nullCols.add(nullCol);
      } else {
        addTableColumn(
            ProjectedColumn.fromResolution(col, tableColIndex,
                tableSchema.column(tableColIndex)));
      }
    }
  }

  /**
   * Turn the "columns" column into a made-up table column (that nonetheless
   * appears in the table loader.) The list may also contain metadata columns.
   */

  public static class ColumnsArrayProjection extends BaseTableColumnVisitor {

    public ColumnsArrayProjection(TupleMetadata tableSchema) {
      super(tableSchema);
      columnIsProjected = new boolean[] { true };
      logicalToPhysicalMap = new int[] { 0 };
      projectionMap = new int[1];
   }

    @Override
    protected void visitColumnsArray(int index, ColumnsArrayColumn col) {
      addTableColumn(ProjectedColumn.fromColumnsArray(col));
    }
  }

  private final FileLevelProjection fileProjection;
  private final TupleMetadata tableSchema;
  private final List<ScanOutputColumn> outputCols;

  /**
   * Map, in table schema order, indicating which columns are selected.
   * The number of entries is the same as the table schema size.
   */

  private final boolean[] projectionMap;

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

  private final int metadataProjectionMap[];
  private final List<NullColumn> nullCols;

  private TableLevelProjection(FileLevelProjection fileProj,
                    TupleMetadata tableSchema, boolean reresolve) {
    fileProjection = fileProj;
    this.tableSchema = tableSchema;
    BaseTableColumnVisitor baseBuilder;
    ProjectionType projType = reresolve ? ProjectionType.LIST
                                        : fileProj.scanProjection().projectType();
    switch (projType) {

    // SELECT a, b, c

    case LIST:
      TableSchemaProjection schemaProj = new TableSchemaProjection(tableSchema);
      schemaProj.visit(fileProj.output());
      nullCols = schemaProj.nullCols;
      nullProjectionMap = schemaProj.nullProjectionMap;
      baseBuilder = schemaProj;
      break;

    // SELECT *

    case WILDCARD:
      WildcardExpander expander = new WildcardExpander(tableSchema);
      expander.visit(fileProj.output());
      nullCols = null;
      nullProjectionMap = null;
      baseBuilder = expander;
      break;

    // SELECT columns

    case COLUMNS_ARRAY:
      validateColumnsArray(fileProj.output());
      ColumnsArrayProjection colArrayProj = new ColumnsArrayProjection(tableSchema);
      colArrayProj.visit(fileProj.output());
      nullCols = null;
      nullProjectionMap = null;
      baseBuilder = colArrayProj;
      break;
    default:
      throw new IllegalStateException("Unexpected selection type: " +
                fileProj.scanProjection().projectType());
    }
    outputCols = baseBuilder.outputCols;
    projectionMap = baseBuilder.columnIsProjected;
    logicalToPhysicalMap = baseBuilder.logicalToPhysicalMap;
    tableColumnProjectionMap = baseBuilder.projectionMap;
    metadataProjectionMap = baseBuilder.metadataProjection;
  }

  public ScanLevelProjection scanProjection() { return fileProjection.scanProjection(); }
  public FileLevelProjection fileProjection() { return fileProjection; }
  public List<ScanOutputColumn> output() { return outputCols; }
  public boolean[] projectionMap() { return projectionMap; }
  public int[] logicalToPhysicalMap() { return logicalToPhysicalMap; }
  public int[] tableColumnProjectionMap() { return tableColumnProjectionMap; }
  public boolean hasNullColumns() { return nullCols != null && ! nullCols.isEmpty(); }
  public List<NullColumn> nullColumns() { return nullCols; }
  public int[] nullProjectionMap() { return nullProjectionMap; }

  /**
   * Metadata projection map.
   * @return a map, indexed by metadata column positions (as defined by
   * {@link #metadataColumns()}, to the position in the full output schema
   * as defined by {@link #output()}
   */
  public int[] metadataProjection() { return metadataProjectionMap; }

  public List<SchemaPath> projectedTableColumns() {
    List<SchemaPath> projection = new ArrayList<>();
    for (int i = 0; i < projectionMap.length; i++) {
      if (projectionMap[i]) {
        projection.add(SchemaPath.getSimplePath(tableSchema.column(i).getName()));
      }
    }
    return projection;
  }

  @VisibleForTesting
  public TupleMetadata outputSchema() {
    return ScanOutputColumn.schema(output());
  }

  private void validateColumnsArray(List<ScanOutputColumn> input) {

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

  /**
   * Resolve a file-level projection definition by resolving each table-column
   * reference and/or expanding a wildcard.
   *
   * @param fileProj the file-level projection definition
   * @param tableSchema the current table (or batch) schema
   * @return the fully resolved table-level projection
   */

  public static TableLevelProjection fromResolution(
      FileLevelProjection fileProj, TupleMetadata tableSchema) {
    return new TableLevelProjection(fileProj, tableSchema, false);
  }

  /**
   * Re-resolve a projection created by "unresolving" a prior wildcard
   * projection. This is a special case: we resolve as if the original projection
   * were of the form SELECT a, b, c, but we use the names of the
   * @param fileProj
   * @param tableSchema
   * @return
   */
  public static TableLevelProjection fromReresolution(
      FileLevelProjection fileProj, TupleMetadata tableSchema) {
    return new TableLevelProjection(fileProj, tableSchema, true);
  }
}
