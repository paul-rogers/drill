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
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.ImplicitColumnExplorer;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;
import org.apache.hadoop.fs.Path;

/**
 * Builds the per-scan projection plan given a set of selected columns,
 * data source columns and implicit columns. Determines the output schema,
 * which columns to project from the data source, which to fill with nulls,
 * which are implicit, and so on.
 *
 * @see {@link ImplicitColumnExplorer}, the class from which this class
 * evolved
 */

public class ProjectionPlanner {

  /**
   * Definition of a column from the SELECT list. This is
   * the column before semantic analysis is done to determine
   * the meaning of the column name.
   */

  public static class SelectColumn {
    private final String name;
    protected OutputColumn projection;

    public SelectColumn(SchemaPath col) {
      this.name = col.getAsUnescapedPath();
    }

    public String name() { return name; }

    /**
     * Return the output column to which this input column is projected.
     *
     * @return the corresponding output column
     */

    public OutputColumn projection() {
      return projection;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
        .append("[InputColumn name=\"")
        .append(name)
        .append("\", projection=");
      if (projection == null) {
        buf.append("null");
      } else {
        buf.append(projection.name());
      }
      buf.append("]");
      return buf.toString();
    }
  }

  /**
   * Definition of an implicit column for this query. Provides the static definition, along
   * with the name set for the implicit column in the session options for the query.
   */

  public static class ImplicitColumnDefn {
    public final ImplicitFileColumns defn;
    public final String colName;

    public ImplicitColumnDefn(String colName, ImplicitFileColumns defn) {
      this.colName = colName;
      this.defn = defn;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
        .append("[InputColumn name=\"")
        .append(colName)
        .append("\", defn=")
        .append(defn)
        .append("]");
      return buf.toString();
    }
  }

  /**
   * Definition of a column from the table. Used to match up the SELECT list
   * to the available table columns.
   */

  public static class TableColumn {
    private final int index;
    private final MaterializedField schema;
    protected ProjectedColumn projection;

    public TableColumn(int index, MaterializedField schema) {
      this.index = index;
      this.schema = schema;
    }

    public int index() { return index; }
    public MaterializedField schema() { return schema; }
    public String name() { return schema.getName(); }

    /**
     * Return the output column to which this data source column is
     * mapped. Not all data source columns are mapped in all cases.
     *
     * @return the corresponding output column, if any
     */

    public ProjectedColumn projection() { return projection; }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append("[DataSourceColumn index=")
          .append(index)
          .append(", schema=")
          .append(schema)
          .append(", projection=");
      if (projection == null) {
        buf.append("null");
      } else {
        buf.append(projection.name());
      }
      buf.append("]");
      return buf.toString();
    }
  }

  /**
   * Represents a column in the output rows (record batch, result set) from
   * the scan operator. Each column has an index, which is the column's position
   * within the output tuple.
   */

  public abstract static class OutputColumn {
    public enum ColumnType { DATA_SOURCE, NULL, IMPLICIT, PARTITION, COLUMNS }

    protected final int index;
    private final SelectColumn queryCol;
    protected final MaterializedField schema;

    public OutputColumn(SelectColumn queryCol, int index, MaterializedField schema) {
      this.queryCol = queryCol;
      this.index = index;
      this.schema = schema;
    }

    public abstract ColumnType columnType();
    public MaterializedField schema() { return schema; }
    public int index() { return index; }

    /**
     * The SELECT column that gave rise to this output column. Will be
     * null for a SELECT * query.
     *
     * @return the corresponding SELECT column
     */

    public SelectColumn projection() { return queryCol; }

    public String name() {
      return queryCol != null ? queryCol.name() : schema.getName();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append("[")
          .append(getClass().getSimpleName())
          .append(" index=")
          .append(index)
          .append(", schema=")
          .append(schema)
          .append(", projection=");
      if (queryCol == null) {
        buf.append("null");
      } else {
        buf.append(queryCol.name());
      }
      buildString(buf);
      buf.append("]");
      return buf.toString();
    }

    protected void buildString(StringBuilder buf) { }
  }

  /**
   * Output column which projects a table column. The type of the output
   * is the same as the table column. The name matches the input when the query
   * provides a SELECT list, else matches that from the table. The two will differ
   * if the table column is, say, a and the SELECT list uses A.
   */

  public static class ProjectedColumn extends OutputColumn {
    private final TableColumn source;

    public ProjectedColumn(SelectColumn inCol, int index, TableColumn dsCol) {
      super(inCol, index, dsCol.schema);
      source = dsCol;
    }

    @Override
    public ColumnType columnType() { return ColumnType.DATA_SOURCE; }

    /**
     * The data source column to which this projected column is mapped.
     *
     * @return the non-null data source column
     */

    public TableColumn source() { return source; }

    @Override
    protected void buildString(StringBuilder buf) {
      buf.append(", source=\"")
         .append(source.name())
         .append("\"");
    }
  }

  /**
   * Represents Drill's special "columns" column which holds all actual columns
   * as an array of Varchars. There can be only one such column in the SELECT
   * list.
   */

  public static class ColumnsArrayColumn extends OutputColumn {
    public ColumnsArrayColumn(SelectColumn inCol, int index) {
      super(inCol, index, MaterializedField.create("columns",
          MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.REPEATED)
            .build()));
    }

    @Override
    public ColumnType columnType() { return ColumnType.COLUMNS; }

    @Override
    public String name() { return schema.getName(); }
  }

  /**
   * Base class for the various static (implicit) columns. Holds the
   * value of the column.
   */

  public abstract static class StaticColumn extends OutputColumn {

    protected String value;

    public StaticColumn(SelectColumn queryCol, int index,
        MaterializedField schema) {
      super(queryCol, index, schema);
    }

    public String value() { return value; }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", value=");
      if (value == null) {
        buf.append("null");
      } else {
        buf.append("\"")
           .append(value)
           .append("\"");
      }
    }
  }

  /**
   * Represents a null column: one that appears in the SELECT, but does not
   * match a table column, implicit column or partition. The output value is
   * always a null Varchar. (Would be better to just be NULL with no type,
   * but Drill does not have that concept.)
   */

  public static class NullColumn extends StaticColumn {

    public NullColumn(SelectColumn inCol, int index, MaterializedField schema) {
      super(inCol, index, schema);
    }

    @Override
    public ColumnType columnType() { return ColumnType.NULL; }
  }

  /**
   * Represents an output column created from an implicit column. Since
   * values are known before reading data, the value is provided
   * along with the column definition.
   */

  public static class ImplicitColumn extends StaticColumn {
    private final ImplicitColumnDefn defn;

    public ImplicitColumn(SelectColumn inCol, int index, ImplicitColumnDefn defn) {
      super(inCol, index, MaterializedField.create(defn.colName,
          MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.REQUIRED)
            .build()));
      this.defn = defn;
    }

    @Override
    public ColumnType columnType() { return ColumnType.IMPLICIT; }

    public void setValue(Path path) {
      value = defn.defn.getValue(path);
    }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", defn=")
         .append(defn);
    }
  }

  /**
   * Partition output column for "dir<n>" for some n.
   * Data type is optional because some files may be more deeply
   * nested than others, so some files may have, say a dir2
   * while others do not.
   * <p>
   * The "dir" portion is customizable via a session option.
   * <p>
   * The value of the partition is known up front, and so the value
   * is stored in this column definition.
   */

  public static class PartitionColumn extends StaticColumn {
    private final int partition;

    public PartitionColumn(SelectColumn inCol, int index, int partition) {
      super(inCol, index, makeSchema(inCol.name));
      this.partition = partition;
    }

    public PartitionColumn(String baseName, int index, int partition) {
      super(null, index, makeSchema(baseName + partition));
      this.partition = partition;
    }

    private static MaterializedField makeSchema(String name) {
      return MaterializedField.create(name,
          MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.OPTIONAL)
            .build());
    }

    @Override
    public ColumnType columnType() { return ColumnType.PARTITION; }

    public void setValue(String value) {
      this.value = value;
    }

    @Override
    protected void buildString(StringBuilder buf) {
      super.buildString(buf);
      buf.append(", partition=")
         .append(partition);
    }
  }

  /**
   * Fluent builder for the projection mapping. Accepts the inputs needed
   * to plan a projection, builds the mappings, and constructs the final
   * mapping object.
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
   */

  public static class ColumnProjectionBuilder {

    // Config

    private final String partitionDesignator;
    private final Pattern partitionPattern;
    private List<ImplicitColumnDefn> implicitColDefns = new ArrayList<>();;
    private Map<String, ImplicitColumnDefn> implicitColIndex = CaseInsensitiveMap.newHashMap();

    // Input

    private boolean selectAll = true;
    private List<SelectColumn> queryCols;
    private MajorType nullColType;
    private List<TableColumn> tableCols;
    private Map<String, TableColumn> tableIndex = CaseInsensitiveMap.newHashMap();
    private Path filePath;
    private String[] dirPath;

    // Output

    private ColumnsArrayColumn columnsArrayCol;
    private List<ProjectedColumn> projectedCols = new ArrayList<>();
    private List<NullColumn> nullCols = new ArrayList<>();
    private List<ImplicitColumn> implicitCols = new ArrayList<>();
    private List<PartitionColumn> partitionCols = new ArrayList<>();
    private List<StaticColumn> staticCols = new ArrayList<>();
    private List<OutputColumn> outputCols = new ArrayList<>();

    public ColumnProjectionBuilder(OptionSet optionManager) {
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
    public ColumnProjectionBuilder selectAll() {
      selectAll = true;
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
    public ColumnProjectionBuilder queryCols(List<SchemaPath> queryCols) {
      selectAll = false;
      this.queryCols = new ArrayList<>();
      for (SchemaPath col : queryCols) {
        this.queryCols.add(new SelectColumn(col));
      }
      return this;
    }

    /**
     * Specify the set of table columns when known "early": during the projection
     * planning process.
     *
     * @param cols list of table columns in table schema order
     * @return this builder
     */

    public ColumnProjectionBuilder tableColumns(List<MaterializedField> cols) {
      tableCols = new ArrayList<>();
      for (MaterializedField col : cols) {
        if (tableIndex.containsKey(col.getName())) {
          throw new IllegalStateException("Duplicate selection column: " + col.getName());
        }
        TableColumn dsCol = new TableColumn(tableCols.size(), col);
        tableCols.add(dsCol);
        tableIndex.put(col.getName(), dsCol);
      }
      return this;
    }

    /**
     * Specify the file name and optional selection root. If the selection root
     * is provided, then partitions are defined as the portion of the file name
     * that is not also part of the selection root. That is, if selection root is
     * /a/b and the file path is /a/b/c/d.csv, then dir0 is c.
     *
     * @return map with columns names as keys and their values
     */

    public ColumnProjectionBuilder setSource(Path filePath, String selectionRoot) {
      this.filePath = filePath;
      if (selectionRoot == null) {
        return this;
      }

      // Result of splitting /x/y is ["", "x", "y"], so ignore first.

      String[] r = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString().split("/");

      // Result of splitting "/x/y/z.csv" is ["", "x", "y", "z.csv"], so ignore first and last

      String[] p = Path.getPathWithoutSchemeAndAuthority(filePath).toString().split("/");

      if (p.length - 1 < r.length) {
        throw new IllegalStateException("Selection root of \"" + selectionRoot +
                                        "\" is shorter than file path of \"" + filePath.toString());
      }
      for (int i = 1; i < r.length; i++) {
        if (! r[i].equals(p[i])) {
          throw new IllegalStateException("Selection root of \"" + selectionRoot +
              "\" is leading path of \"" + filePath.toString());
        }
      }
      dirPath = ArrayUtils.subarray(p, r.length, p.length - 1);
      return this;
   }

    /**
     * Perform projection planning and return the final projection plan.
     * @return the finalized projection plan
     */

    public ProjectionPlanner build() {
      if (selectAll) {
        selectAllTableCols();
        selectAllPartitions();
        selectAllImplicitCols();
      } else {
        mapSelectedCols();
        fillImplicitCols();
        fillPartitionCols();
      }
      return new ProjectionPlanner(this);
    }

    private void selectAllTableCols() {
      for (TableColumn col : tableCols) {
        col.projection = new ProjectedColumn(null, outputCols.size(), col);
        projectedCols.add(col.projection);
        outputCols.add(col.projection);
      }
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
        ImplicitColumn outCol = new ImplicitColumn(null, outputCols.size(), iCol);
        outCol.setValue(filePath);
        implicitCols.add(outCol);
        staticCols.add(outCol);
        outputCols.add(outCol);
      }
    }

    private void mapSelectedCols() {
      if (nullColType == null) {
        nullColType = MajorType.newBuilder()
            .setMinorType(MinorType.INT)
            .setMode(DataMode.OPTIONAL)
            .build();
      }
      int index = 0;
      for (SelectColumn inCol : queryCols) {
        OutputColumn outCol = mapColumn(index++, inCol);
        outputCols.add(outCol);
        inCol.projection = outCol;
      }
    }

    /**
     * Map the column into one of five categories.
     * <ol>
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

    private OutputColumn mapColumn(int index, SelectColumn inCol) {
      Matcher m = partitionPattern.matcher(inCol.name);
      if (m.matches()) {
        PartitionColumn outCol = new PartitionColumn(inCol, index, Integer.parseInt(m.group(1)));
        partitionCols.add(outCol);
        staticCols.add(outCol);
        return outCol;
      }
      ImplicitColumnDefn iCol = implicitColIndex.get(inCol.name);
      if (iCol != null) {
        ImplicitColumn outCol = new ImplicitColumn(inCol, index, iCol);
        implicitCols.add(outCol);
        staticCols.add(outCol);
        return outCol;
      }

      if (inCol.name.equalsIgnoreCase("columns")) {
        if (! projectedCols.isEmpty() || ! nullCols.isEmpty()) {
          throw new IllegalArgumentException("Cannot combine columns[] with other columns");
        }
        columnsArrayCol = new ColumnsArrayColumn(inCol, index);
        return columnsArrayCol;
      }

      if (columnsArrayCol != null) {
        throw new IllegalArgumentException("Cannot combine columns[] with other columns: " + inCol.name);
      }

      // If a schema is not known, assume all columns are projected.

      if (tableCols.isEmpty()) {
        ProjectedColumn outCol = new ProjectedColumn(inCol, index, null);
        projectedCols.add(outCol);
        return outCol;
      }

      TableColumn dsCol = tableIndex.get(inCol.name);
      if (dsCol != null) {
        ProjectedColumn outCol = new ProjectedColumn(inCol, index, dsCol);
        dsCol.projection = outCol;
        projectedCols.add(outCol);
        return outCol;
      } else {
        NullColumn outCol = new NullColumn(inCol, index, MaterializedField.create(inCol.name, nullColType));
        nullCols.add(outCol);
        staticCols.add(outCol);
        return outCol;
      }
    }

    private void fillImplicitCols() {
      if (filePath == null) {
        return;
      }

      for (ImplicitColumn col : implicitCols) {
        col.setValue(filePath);
      }
    }

    private void fillPartitionCols() {
      if (dirPath == null) {
        return;
      }
      for (PartitionColumn col : partitionCols) {
        if (col.partition < dirPath.length) {
          col.setValue(dirPath[col.partition]);
        }
      }
    }
  }

  private final boolean selectAll;
  private final List<SelectColumn> queryCols;
  private final List<TableColumn> tableCols;
  private final ColumnsArrayColumn columnsCol;
  private final List<ProjectedColumn> projectedCols;
  private final List<NullColumn> nullCols;
  private final List<ImplicitColumn> implicitCols;
  private final List<PartitionColumn> partitionCols;
  private final List<StaticColumn> staticCols;
  private final List<OutputColumn> outputCols;

  public static ColumnProjectionBuilder builder(OptionSet options) {
    return new ColumnProjectionBuilder(options);
  }

  public ProjectionPlanner(ColumnProjectionBuilder builder) {
    selectAll = builder.selectAll;
    queryCols = builder.queryCols;
    tableCols = builder.tableCols;
    columnsCol = builder.columnsArrayCol;
    projectedCols = builder.projectedCols;
    nullCols = builder.nullCols;
    implicitCols = builder.implicitCols;
    partitionCols = builder.partitionCols;
    staticCols = builder.staticCols;
    outputCols = builder.outputCols;
  }

  /**
   * Return whether this is a SELECT * query
   * @return true if this is a SELECT * query
   */
  public boolean isSelectAll() { return selectAll; }
  /**
   * Return the set of columns from the SELECT list
   * @return the SELECT list columns, in SELECT list order,
   * or null if this is a SELECT * query
   */
  public List<SelectColumn> queryCols() { return queryCols; }
  public boolean hasEarlySchema() { return tableCols != null; }
  /**
   * Return the columns available in the table, in the order defined
   * by the table
   * @return the set of table columns if this is an early-schema plan,
   * or null if the table schema is not known at projection plan time
   */
  public List<TableColumn> tableCols() { return tableCols; }
  /**
   * Return the subset of output columns that are projected from
   * the input table
   * @return the set of projected table columns, in output order
   */
  public List<ProjectedColumn> projectedCols() { return projectedCols; }
  /**
   * Return the subset of output columns that were listed in the SELECT
   * list, but which matched no table, implicit or partition columns
   * @return the set of null columns, in output order
   */
  public List<NullColumn> nullCols() { return nullCols; }
  /**
   * Return the subset of output columns which are implicit
   * @return the implicit columns, in output order
   */
  public List<ImplicitColumn> implicitCols() { return implicitCols; }
  /**
   * Return the subset of output columns which hold partition (directory)
   * values
   * @return the partition columns, in output order
   */
  public List<PartitionColumn> partitionCols() { return partitionCols; }
  /**
   * Return the one and only "columns" array column, if selected in
   * this query
   * @return the columns array column, or null if "columns" was not
   * selected in the query
   */
  public ColumnsArrayColumn columnsCol() { return columnsCol; }
  /**
   * Return the list of static columns: those for which the value is pre-defined
   * at plan time.
   * @return this list of static columns, in output order
   */
  public List<StaticColumn> staticCols() { return staticCols; }
  /**
   * The entire set of output columns, in output order. Output order is
   * that specified in the SELECT (for an explicit list of columns) or
   * table order (for SELECT * queries).
   * @return the set of output columns in output order
   */
  public List<OutputColumn> outputCols() { return outputCols; }
}
