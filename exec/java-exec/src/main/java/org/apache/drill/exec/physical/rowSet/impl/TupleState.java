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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.BaseMapColumnState;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter.TupleWriterListener;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;

/**
 * Represents the loader state for a tuple: a row or a map. This is "state" in
 * the sense of variables that are carried along with each tuple. Handles
 * write-time issues such as defining new columns, allocating memory, handling
 * overflow, assembling the output version of the map, and so on. Each
 * row and map in the result set has a tuple state instances associated
 * with it.
 * <p>
 * Here, by "tuple" we mean a container of vectors, each of which holds
 * a variety of values. So, the "tuple" here is structural, not a specific
 * set of values, but rather the collection of vectors that hold tuple
 * values.
 */

public abstract class TupleState extends ContainerState
  implements TupleWriterListener {

  /**
   * Handles the details of the top-level tuple, the data row itself.
   * Note that by "row" we mean the set of vectors that define the
   * set of rows.
   */

  public static class RowState extends TupleState {

    /**
     * The row-level writer for stepping through rows as they are written,
     * and for accessing top-level columns.
     */

    private final RowSetLoaderImpl writer;

    public RowState(ResultSetLoaderImpl rsLoader, ResultVectorCache vectorCache) {
      super(rsLoader, vectorCache, rsLoader.projectionSet);
      writer = new RowSetLoaderImpl(rsLoader, schema);
      writer.bindListener(this);
    }

    public RowSetLoaderImpl rootWriter() { return writer; }

    @Override
    public AbstractTupleWriter writer() { return writer; }

    @Override
    public int innerCardinality() { return resultSetLoader.targetRowCount();}

    public ResultVectorCache vectorCache() { return vectorCache; }
  }

  /**
   * Represents a tuple defined as a Drill map: single or repeated. Note that
   * the map vector does not exist here; it is assembled only when "harvesting"
   * a batch. This design supports the obscure case in which a new column
   * is added during an overflow row, so exists within this abstraction,
   * but is not published to the map that makes up the output.
   */

  public static class MapState extends TupleState {

    protected final BaseMapColumnState mapColumnState;
    protected int outerCardinality;

    public MapState(ResultSetLoaderImpl rsLoader,
        ResultVectorCache vectorCache,
        BaseMapColumnState mapColumnState,
        ProjectionSet projectionSet) {
      super(rsLoader, vectorCache, projectionSet);
      this.mapColumnState = mapColumnState;
      mapColumnState.writer().bindListener(this);
    }

    /**
     * Return the tuple writer for the map. If this is a single
     * map, then it is the writer itself. If this is a map array,
     * then the tuple is nested inside the array.
     */

    @Override
    public AbstractTupleWriter writer() {
      AbstractObjectWriter objWriter = mapColumnState.writer();
      TupleWriter tupleWriter;
      if (objWriter.type() == ObjectType.ARRAY) {
        tupleWriter = objWriter.array().tuple();
      } else {
        tupleWriter = objWriter.tuple();
      }
      return (AbstractTupleWriter) tupleWriter;
    }

    /**
     * In order to allocate the correct-sized vectors, the map must know
     * its member cardinality: the number of elements in each row. This
     * is 1 for a single map, but may be any number for a map array. Then,
     * this value is recursively pushed downward to compute the cardinality
     * of lists of maps that contains lists of maps, and so on.
     */

    @Override
    public void updateCardinality(int outerCardinality) {
      this.outerCardinality = outerCardinality;
      super.updateCardinality(outerCardinality);
    }

    @Override
    public int innerCardinality() {
      return outerCardinality * mapColumnState.schema().expectedElementCount();
    }

    @Override
    public void dump(HierarchicalFormatter format) {
      format
        .startObject(this)
        .attribute("column", mapColumnState.schema().name())
        .attribute("cardinality", outerCardinality)
        .endObject();
    }
  }

  protected final List<ColumnState> columns = new ArrayList<>();
  protected final TupleSchema schema = new TupleSchema();

  protected TupleState(ResultSetLoaderImpl rsLoader, ResultVectorCache vectorCache, ProjectionSet projectionSet) {
    super(rsLoader, vectorCache, projectionSet);
  }

  /**
   * Returns an ordered set of the columns which make up the tuple.
   * Column order is the same as that defined by the map's schema,
   * to allow indexed access. New columns always appear at the end
   * of the list to preserve indexes.
   *
   * @return ordered list of column states for the columns within
   * this tuple
   */

  public List<ColumnState> columns() { return columns; }

  public TupleMetadata schema() { return writer().schema(); }

  public abstract AbstractTupleWriter writer();

  @Override
  public ObjectWriter addColumn(TupleWriter tupleWriter, MaterializedField column) {
    return addColumn(tupleWriter, TupleSchema.fromField(column));
  }

  @Override
  protected void addColumn(ColumnState colState) {
    columns.add(colState);
  }

  @Override
  public boolean isProjected(String columnName) {
    return super.isProjected(columnName);
  }

  @Override
  public ObjectWriter addColumn(TupleWriter tupleWriter, ColumnMetadata columnSchema) {

    // Verify name is not a (possibly case insensitive) duplicate.

    TupleMetadata tupleSchema = schema();
    String colName = columnSchema.name();
    if (tupleSchema.column(colName) != null) {
      throw new IllegalArgumentException("Duplicate column: " + colName);
    }

    return addColumn(columnSchema).writer();
  }

  /**
   * When creating a schema up front, provide the schema of the desired tuple,
   * then build vectors and writers to match. Allows up-front schema definition
   * in addition to on-the-fly schema creation handled elsewhere.
   *
   * @param schema desired tuple schema to be materialized
   */

  public void buildSchema(TupleMetadata schema) {
    for (int i = 0; i < schema.size(); i++) {
      ColumnMetadata colSchema = schema.metadata(i);
      writer().addColumnWriter(buildColumn(colSchema));
    }
  }

  public void updateCardinality(int cardinality) {
    for (ColumnState colState : columns) {
      colState.updateCardinality(cardinality);
    }
  }
  public boolean hasProjections() {
    for (ColumnState colState : columns) {
      if (colState.isProjected()) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected Collection<ColumnState> columnStates() {
    return columns;
  }

  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attributeArray("columns");
    for (int i = 0; i < columns.size(); i++) {
      format.element(i);
      columns.get(i).dump(format);
    }
    format
      .endArray()
      .endObject();
  }
}
