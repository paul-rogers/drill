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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.TableSchemaType;
import org.apache.drill.exec.physical.impl.scan.SelectionListPlan.FileInfoColumn;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

public class ScanProjection {
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
    for (OutputColumn.PartitionColumn col : partitionCols) {
      if (col.partition() < dirPath.length) {
        col.setValue(dirPath[col.partition()]);
      }
    }
  }

  if (tableCols != null) {
    throw new IllegalArgumentException("Cannot specify `columns` with a table schema");
  }
  private final TableSchemaType tableType;
  private final List<TableColumn> tableCols;
  private final ColumnsArrayColumn columnsCol;
  private final List<ProjectedColumn> projectedCols;
  private final List<NullColumn> nullCols;
  private final List<FileInfoColumn> implicitCols;
  private final List<OutputColumn.PartitionColumn> partitionCols;
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

  public TableSchemaType tableSchemaType() { return tableType; }

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

  @VisibleForTesting
  public BatchSchema outputSchema() {
    List<MaterializedField> fields = new ArrayList<>();
    for (OutputColumn col : outputCols) {
      fields.add(col.schema);
    }
    return new BatchSchema(SelectionVectorMode.NONE, fields);
  }
  /**
   * Return the subset of output columns which are implicit
   * @return the implicit columns, in output order
   */

  public List<OutputColumn.FileMetadataColumn> fileInfoCols() { return implicitCols; }
  /**
   * Return the subset of output columns which hold partition (directory)
   * values
   * @return the partition columns, in output order
   */
  public List<OutputColumn.PartitionColumn> partitionCols() { return partitionCols; }
  /**
   * Return the list of static columns: those for which the value is pre-defined
   * at plan time.
   * @return this list of static columns, in output order
   */
  public List<StaticColumn> staticCols() { return staticCols; }
}