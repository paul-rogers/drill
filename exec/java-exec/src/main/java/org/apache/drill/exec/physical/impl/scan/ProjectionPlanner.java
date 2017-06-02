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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.*;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.TableSchemaType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.ImplicitColumnExplorer;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;
import org.apache.hadoop.fs.Path;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Fluent builder for the projection mapping. Accepts the inputs needed
 * to plan a projection, builds the mappings, and constructs the final
 * mapping object.
 * Builds the per-scan projection plan given a set of selected columns,
 * data source columns and implicit columns. Determines the output schema,
 * which columns to project from the data source, which to fill with nulls,
 * which are implicit, and so on.
 * <p>
 * Mappings can be based on three primary use cases:
 * <ul>
 * <li>SELECT *: Select all data source columns, whatever they happen
 * to be. Create columns using names from the data source. The data source
 * also determines the order of columns within the row.</li>
 * <li>SELECT columns: Similar to SELECT * in that it selects all columns
 * from the data source, in data source order. But, rather than creating
 * individual output columns for each data source column, creates a single
 * column which is an array of Varchars which holds the (text form) of
 * each column as an array element.</li>
 * <li>SELECT a, b, c, ...: Select a specific set of columns, identified by
 * case-insensitive name. The output row uses the names from the SELECT list,
 * but types from the data source. Columns appear in the row in the order
 * specifed by the SELECT.</li>
 * </ul>
 * Names in the SELECT list can reference any of four distinct types of output
 * columns:
 * <ul>
 * <li>Data source columns: columns from the underlying table.</i>
 * <li>Implicit columns: fqn, filename, filepath and suffix. These reference
 * parts of the name of the file being scanned.</li>
 * <li>Partition columns: dir0, dir1, ...: These reference parts of the path
 * name of the file.</li>
 * <li>Null column: a name in the SELECT list which maps to none of the above.
 * Such a name is not an error; it simply becomes a column which is always
 * NULL.</li>
 * </ul>
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

public class ProjectionPlanner {

  // Config

  private final String partitionDesignator;
  private final Pattern partitionPattern;
  private List<ImplicitColumnDefn> implicitColDefns = new ArrayList<>();;
  private Map<String, ImplicitColumnDefn> implicitColIndex = CaseInsensitiveMap.newHashMap();
  private boolean useLegacyStarPlan = true;

  // Input

  List<SelectColumn> queryCols = new ArrayList<>();
  private MajorType nullColType;
  List<TableColumn> tableCols;
  private Map<String, TableColumn> tableIndex = CaseInsensitiveMap.newHashMap();
  private Path filePath;
  private String[] dirPath;

  // Output

  protected ColumnsArrayColumn columnsArrayCol;
  protected List<ProjectedColumn> projectedCols = new ArrayList<>();
  protected List<NullColumn> nullCols = new ArrayList<>();
  protected List<FileInfoColumn> implicitCols = new ArrayList<>();
  protected List<PartitionColumn> partitionCols = new ArrayList<>();
  protected List<StaticColumn> staticCols = new ArrayList<>();
  protected List<OutputColumn> outputCols = new ArrayList<>();
  protected SelectType selectType;
  protected TableSchemaType tableType;
  private SelectColumn starColumn;

  public ProjectionPlanner(OptionSet optionManager) {
    partitionDesignator = optionManager.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR);
    partitionPattern = Pattern.compile(partitionDesignator + "(\\d+)", Pattern.CASE_INSENSITIVE);
    for (ImplicitFileColumns e : ImplicitFileColumns.values()) {
      OptionValue optionValue = optionManager.getOption(e.optionName());
      if (optionValue != null) {
        ImplicitColumnDefn defn = new ImplicitColumnDefn(optionValue.string_val, e);
        implicitColDefns.add(defn);
        implicitColIndex.put(defn.colName, defn);
      }
    }
  }

  /**
   * Indicate a SELECT * query.
   *
   * @return this builder
   */
  public ProjectionPlanner selectAll() {
    return queryCols(Lists.newArrayList(new SchemaPath[] {SchemaPath.getSimplePath("*")}));
  }

  /**
   * Specifies whether to plan based on the legacy meaning of "*". See
   * <a href="https://issues.apache.org/jira/browse/DRILL-5542">DRILL-5542</a>.
   * If true, then the star column <i>includes</i> implicit and partition
   * columns. If false, then star matches <i>only</i> table columns.
   * @param flag true to use the legacy plan, false to use the revised
   * semantics
   * @return this builder
   */

  public ProjectionPlanner useLegacyStarPlan(boolean flag) {
    useLegacyStarPlan = flag;
    return this;
  }

  /**
   * Specify the set of columns in the SELECT list. Since the column list
   * comes from the query planner, assumes that the planner has checked
   * the list for syntax and uniqueness.
   *
   * @param queryCols list of columns in the SELECT list in SELECT list order
   * @return this builder
   */
  public ProjectionPlanner queryCols(List<SchemaPath> queryCols) {
    assert this.queryCols.isEmpty();
    this.queryCols = new ArrayList<>();
    for (SchemaPath col : queryCols) {
      SelectColumn sCol = new SelectColumn(col);
      this.queryCols.add(sCol);
    }
    return this;
  }

  public void selection(List<SelectColumn> queryCols) {
    assert this.queryCols.isEmpty();
    this.queryCols.addAll(queryCols);
  }

  /**
   * Specify the set of table columns when known "early": during the projection
   * planning process.
   *
   * @param cols list of table columns in table schema order
   * @return this builder
   */

  public ProjectionPlanner tableColumns(List<MaterializedField> cols) {
    return tableColumns(cols);
  }

  public ProjectionPlanner tableColumns(Iterator<MaterializedField> iter) {
    tableCols = new ArrayList<>();
    while (iter.hasNext()) {
      MaterializedField col = iter.next();
      if (tableIndex.containsKey(col.getName())) {
        throw new IllegalStateException("Duplicate selection column: " + col.getName());
      }
      TableColumn dsCol = new TableColumn(tableCols.size(), col);
      tableCols.add(dsCol);
      tableIndex.put(col.getName(), dsCol);
    }
    return this;
  }

  public void tableSchema(List<TableColumn> schema) {
    tableCols = schema;
    for (TableColumn col : tableCols) {
      if (tableIndex.containsKey(col.name())) {
        throw new IllegalStateException("Duplicate selection column: " + col.name());
      }
      tableIndex.put(col.name(), col);
    }
  }

  public ProjectionPlanner tableColumns(BatchSchema schema) {
    return tableColumns(schema.iterator());
  }

  /**
   * Specify the file name and optional selection root. If the selection root
   * is provided, then partitions are defined as the portion of the file name
   * that is not also part of the selection root. That is, if selection root is
   * /a/b and the file path is /a/b/c/d.csv, then dir0 is c.
   *
   * @return map with columns names as keys and their values
   */

  public ProjectionPlanner setSource(Path filePath, String selectionRoot) {
    this.filePath = filePath;
    if (selectionRoot == null) {
      return this;
    }

    // Result of splitting /x/y is ["", "x", "y"], so ignore first.

    String[] r = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString().split("/");

    // Result of splitting "/x/y/z.csv" is ["", "x", "y", "z.csv"], so ignore first and last

    String[] p = Path.getPathWithoutSchemeAndAuthority(filePath).toString().split("/");

    if (p.length - 1 < r.length) {
      throw new IllegalArgumentException("Selection root of \"" + selectionRoot +
                                      "\" is shorter than file path of \"" + filePath.toString() + "\"");
    }
    for (int i = 1; i < r.length; i++) {
      if (! r[i].equals(p[i])) {
        throw new IllegalArgumentException("Selection root of \"" + selectionRoot +
            "\" is not a leading path of \"" + filePath.toString() + "\"");
      }
    }
    dirPath = ArrayUtils.subarray(p, r.length, p.length - 1);
    return this;
  }

  /**
   * Perform projection planning and return the final projection plan.
   * @return the finalized projection plan
   */

  public ScanProjection build() {
    mapSelectedCols();
    if (selectType == SelectType.ALL && useLegacyStarPlan) {
      selectAllPartitions();
      selectAllImplicitCols();
    }
    mapImplicitCols();
    mapPartitionCols();

    if (selectType == SelectType.ALL) {
      if (tableCols == null) {
        // SELECT *: is late schema

        tableType = TableSchemaType.LATE;
      } else {
        // Table columns provided: early schema

        tableType = TableSchemaType.EARLY;
      }
    } else if (columnsArrayCol != null) {

      // Has special `columns` column, is early schema because all columns
      // go into the one, known, array.

     tableType = TableSchemaType.EARLY;
    } else if (tableCols != null) {
      // Table columns provided: early schema

      tableType = TableSchemaType.EARLY;
    } else {
      // Is late schema.

      tableType = TableSchemaType.LATE;
    }
    return new ScanProjection(this);
  }

  private void selectAllPartitions() {
    if (dirPath == null) {
      return;
    }

    // If have a partition path, select the dir<n> columns as well.

    for (int i = 0; i < dirPath.length; i++) {
      PartitionColumn partCol = new PartitionColumn(partitionDesignator, outputCols.size(), i);
      partCol.setValue(dirPath[i]);
      partitionCols.add(partCol);
      staticCols.add(partCol);
      outputCols.add(partCol);
    }
  }

  private void selectAllImplicitCols() {
    if (filePath == null) {
      return;
    }
    for (ImplicitColumnDefn iCol : implicitColDefns) {
      FileInfoColumn outCol = new FileInfoColumn(starColumn, outputCols.size(), iCol);
      outCol.setValue(filePath);
      implicitCols.add(outCol);
      staticCols.add(outCol);
      outputCols.add(outCol);
    }
  }

  private void mapSelectedCols() {
    selectType = SelectType.LIST;
    for (SelectColumn inCol : queryCols) {
      mapColumn(inCol);
    }
  }

  private MajorType getNullColumnType() {
    if (nullColType == null) {
      nullColType = MajorType.newBuilder()
          .setMinorType(MinorType.NULL)
          .setMode(DataMode.OPTIONAL)
          .build();
    }
    return nullColType;
  }

  /**
   * Map the column into one of six categories.
   * <ol>
   * <li>Star column (to designate SELECT *)</li>
   * <li>Partition file column (dir0, dir1, etc.)</li>
   * <li>Implicit column (fqn, filepath, filename, suffix)</li>
   * <li>Special <tt>columns</tt> column which holds all columns as
   * an array.</li>
   * <li>Table column (if no schema provided or if the column matches
   * a column in the schema.</li>
   * <li>Null column for which no match can be found.</li>
   * </ol>
   *
   * @param index
   * @param inCol
   * @return
   */

  private void mapColumn(SelectColumn inCol) {
    if (inCol.isStar()) {

      // Star column: this is a SELECT * query.

      mapStarColumn(inCol);
      return;
    }
    Matcher m = partitionPattern.matcher(inCol.name());
    if (m.matches()) {

      // Partition column

      mapPartitionColumn(inCol, Integer.parseInt(m.group(1)));
      return;
    }
    ImplicitColumnDefn iCol = implicitColIndex.get(inCol.name());
    if (iCol != null) {

      // Implicit column

      mapImplicitColumn(iCol, inCol);
      return;
    }

    // The column is a table-like column. Not compatibile with SELECT *.

    if (selectType == SelectType.ALL) {
      throw new IllegalArgumentException("Cannot list table columns and `*` together: " + inCol.name());
    }

    if (inCol.name().equalsIgnoreCase("columns")) {

      // Special `columns` array column.

      mapColumnsArrayColumn(inCol);
      return;
    }

    // True table column. Not compatible with `columns` column.

    if (columnsArrayCol != null) {
      throw new IllegalArgumentException("Cannot combine columns[] with other columns: " + inCol.name());
    }

    if (tableCols == null) {
      // If a schema is not known, assume all columns are projected.
      // The scan operator must sort out actual schema batch-by-batch.

      mapLateProjectColumn(inCol);
      return;
    }

    TableColumn dsCol = tableIndex.get(inCol.name());
    if (dsCol != null) {
      // Early schema, found a table column match. This is a projected column.

      mapEarlyProjectedColumn(dsCol, inCol);
      return;
    } else {

      // Early schema, but not match. This is a null column.

      mapNullColumn(inCol);
      return;
    }
  }

  private void mapStarColumn(SelectColumn inCol) {
    if (! projectedCols.isEmpty()) {
      throw new IllegalArgumentException("Cannot list table columns and `*` together");
    }
    if (starColumn != null) {
      throw new IllegalArgumentException("Duplicate * entry in select list");
    }
    selectType = SelectType.ALL;
    starColumn = inCol;

    // Select 'em if we got 'em.

    if (tableCols != null) {
      for (TableColumn col : tableCols) {
        col.projection = new EarlyProjectedColumn(starColumn, outputCols.size(), col);
        projectedCols.add(col.projection);
        outputCols.add(col.projection);
      }
    }
  }

  private void mapPartitionColumn(SelectColumn inCol, int partition) {
    PartitionColumn outCol = new PartitionColumn(inCol, outputCols.size(), partition);
    partitionCols.add(outCol);
    staticCols.add(outCol);
    outputCols.add(outCol);
    inCol.projection = outCol;
  }

  private void mapImplicitColumn(ImplicitColumnDefn iCol, SelectColumn inCol) {
    FileInfoColumn outCol = new FileInfoColumn(inCol, outputCols.size(), iCol);
    implicitCols.add(outCol);
    staticCols.add(outCol);
    outputCols.add(outCol);
    inCol.projection = outCol;
  }

  private void mapColumnsArrayColumn(SelectColumn inCol) {

    if (! projectedCols.isEmpty() || ! nullCols.isEmpty()) {
      throw new IllegalArgumentException("Cannot combine columns[] with other columns");
    }
    if (columnsArrayCol != null) {
      throw new IllegalArgumentException("Duplicate columns[] column");
    }
    if (tableCols != null) {
      throw new IllegalArgumentException("Cannot specify `columns` with a table schema");
    }

    MaterializedField colSchema = MaterializedField.create("columns",
          MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.REPEATED)
            .build());
    TableColumn tableCol = new TableColumn(0, colSchema);
    tableCols = new ArrayList<>();
    tableCols.add(tableCol);

    columnsArrayCol = new ColumnsArrayColumn(inCol, outputCols.size(), tableCol);
    outputCols.add(columnsArrayCol);
    inCol.projection = columnsArrayCol;
    tableCol.projection = columnsArrayCol;
    projectedCols.add(columnsArrayCol);
  }

  private void mapLateProjectColumn(SelectColumn inCol) {
    ProjectedColumn outCol = new LateProjectedColumn(inCol, outputCols.size());
    projectedCols.add(outCol);
    outputCols.add(outCol);
    inCol.projection = outCol;
  }

  private void mapEarlyProjectedColumn(TableColumn dsCol, SelectColumn inCol) {
    ProjectedColumn outCol = new EarlyProjectedColumn(inCol, outputCols.size(), dsCol);
    dsCol.projection = outCol;
    projectedCols.add(outCol);
    outputCols.add(outCol);
    inCol.projection = outCol;
  }

  private void mapNullColumn(SelectColumn inCol) {
    NullColumn outCol = new NullColumn(inCol, outputCols.size(), MaterializedField.create(inCol.name(), getNullColumnType()));
    nullCols.add(outCol);
    staticCols.add(outCol);
    outputCols.add(outCol);
    inCol.projection = outCol;
  }

  private void mapImplicitCols() {
    if (filePath == null) {
      return;
    }

    for (FileInfoColumn col : implicitCols) {
      col.setValue(filePath);
    }
  }

  private void mapPartitionCols() {
    if (dirPath == null) {
      return;
    }
    for (PartitionColumn col : partitionCols) {
      if (col.partition() < dirPath.length) {
        col.setValue(dirPath[col.partition()]);
      }
    }
  }
}
