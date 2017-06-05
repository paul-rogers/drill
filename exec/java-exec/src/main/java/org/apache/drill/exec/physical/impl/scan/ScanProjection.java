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

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.TableSchemaType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;
import org.apache.hadoop.fs.Path;

public class ScanProjection {

  public enum SelectType { ALL, LIST }

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

    /**
     * Return if this column is the special "*" column which means to select
     * all table columns.
     *
     * @return true if the column is "*"
     */

    public boolean isStar( ) { return name.equals("*"); }

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
    public enum ColumnType { EARLY_TABLE, LATE_TABLE, NULL, IMPLICIT, PARTITION, COLUMNS }

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
      if (queryCol == null  ||  queryCol.isStar()) {
        return schema.getName();
      } else {
        return queryCol.name();
      }
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

  public static abstract class ProjectedColumn extends OutputColumn {

    public ProjectedColumn(SelectColumn queryCol, int index, MaterializedField schema) {
      super(queryCol, index, schema);
    }

    /**
     * The data source column to which this projected column is mapped.
     *
     * @return the non-null data source column
     */

    public abstract TableColumn source();
  }

  /**
   * Output column which projects a table column. The type of the output
   * is the same as the table column. The name matches the input when the query
   * provides a SELECT list, else matches that from the table. The two will differ
   * if the table column is, say, a and the SELECT list uses A.
   */

  public static class EarlyProjectedColumn extends ProjectedColumn {
    private final TableColumn source;

    public EarlyProjectedColumn(SelectColumn inCol, int index, TableColumn dsCol) {
      super(inCol, index, dsCol.schema);
      source = dsCol;
    }

    @Override
    public ColumnType columnType() { return ColumnType.EARLY_TABLE; }

    @Override
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

  public static class ColumnsArrayColumn extends EarlyProjectedColumn {

    public ColumnsArrayColumn(SelectColumn inCol, int index, TableColumn dsCol) {
      super(inCol, index, dsCol);
    }

    @Override
    public ColumnType columnType() { return ColumnType.COLUMNS; }
  }

  public static class LateProjectedColumn extends ProjectedColumn {

    public LateProjectedColumn(SelectColumn queryCol, int index) {
      super(queryCol, index, null);
    }

    @Override
    public TableColumn source() { return null; }

    @Override
    public ColumnType columnType() { return ColumnType.LATE_TABLE; }
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

  public static class FileInfoColumn extends StaticColumn {
    private final ImplicitColumnDefn defn;

    public FileInfoColumn(SelectColumn inCol, int index, ImplicitColumnDefn defn) {
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

    public int partition() { return partition; }
  }

  private final ScanProjection.SelectType selectType;
  private final TableSchemaType tableType;
  private final List<SelectColumn> queryCols;
  private final List<TableColumn> tableCols;
  private final ColumnsArrayColumn columnsCol;
  private final List<ProjectedColumn> projectedCols;
  private final List<NullColumn> nullCols;
  private final List<FileInfoColumn> implicitCols;
  private final List<PartitionColumn> partitionCols;
  private final List<StaticColumn> staticCols;
  private final List<OutputColumn> outputCols;

  public static ProjectionPlanner builder(OptionSet options) {
    return new ProjectionPlanner(options);
  }

  public ScanProjection(ProjectionPlanner builder) {
    selectType = builder.selectType;
    tableType = builder.tableType;
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
  public boolean isSelectAll() { return selectType == SelectType.ALL; }

  public TableSchemaType tableSchemaType() { return tableType; }

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
  public List<FileInfoColumn> fileInfoCols() { return implicitCols; }
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