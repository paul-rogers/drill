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
import java.util.List;

import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.BaseMapColumnState;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.MapArrayColumnState;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.MapColumnState;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.PrimitiveColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter.TupleWriterListener;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;
import org.apache.drill.exec.vector.accessor.writer.ColumnWriterFactory;

/**
 * Manages a map or row, including load state, adding new columns,
 * overflow, etc.
 */

public abstract class TupleState implements TupleWriterListener {

  public static class RowState extends TupleState {

    /**
     * The row-level writer for stepping through rows as they are written,
     * and for accessing top-level columns.
     */

    private final RowSetLoaderImpl writer;

    public RowState(ResultSetLoaderImpl rsLoader) {
      super(rsLoader, rsLoader.projectionSet);
      writer = new RowSetLoaderImpl(rsLoader, schema);
      writer.bindListener(this);
    }

    public RowSetLoaderImpl rootWriter() { return writer; }

    @Override
    public AbstractTupleWriter writer() { return writer; }

    @Override
    public int innerCardinality() { return resultSetLoader.targetRowCount();}
  }

  public static class MapState extends TupleState {

    protected final BaseMapColumnState mapColumnState;
    protected int outerCardinality;

    public MapState(ResultSetLoaderImpl rsLoader,
        BaseMapColumnState mapColumnState,
        ProjectionSet projectionSet) {
      super(rsLoader, projectionSet);
      this.mapColumnState = mapColumnState;
      mapColumnState.writer().bindListener(this);
    }

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

  protected final ResultSetLoaderImpl resultSetLoader;
  protected final List<ColumnState> columns = new ArrayList<>();
  protected final TupleSchema schema = new TupleSchema();
  protected final ProjectionSet projectionSet;

  protected TupleState(ResultSetLoaderImpl rsLoader, ProjectionSet projectionSet) {
    this.resultSetLoader = rsLoader;
    this.projectionSet = projectionSet;
  }

  public abstract int innerCardinality();

  public List<ColumnState> columns() { return columns; }

  public TupleMetadata schema() { return writer().schema(); }

  public abstract AbstractTupleWriter writer();

  @Override
  public ObjectWriter addColumn(TupleWriter tupleWriter, MaterializedField column) {
    return addColumn(tupleWriter, TupleSchema.fromField(column));
  }

  @Override
  public ObjectWriter addColumn(TupleWriter tupleWriter, ColumnMetadata columnSchema) {

    // Verify name is not a (possibly case insensitive) duplicate.

    TupleMetadata tupleSchema = schema();
    String colName = columnSchema.name();
    if (tupleSchema.column(colName) != null) {
      throw new IllegalArgumentException("Duplicate column: " + colName);
    }

    return addColumn(columnSchema);
  }

  private AbstractObjectWriter addColumn(ColumnMetadata columnSchema) {

    // Indicate projection in the metadata.

    ((AbstractColumnMetadata) columnSchema).setProjected(
        projectionSet.isProjected(columnSchema.name()));

    // Build the column

    ColumnState colState;
    if (columnSchema.isMap()) {
      colState = buildMap(columnSchema);
    } else {
      colState = buildPrimitive(columnSchema);
    }
    columns.add(colState);
    columnAdded(colState);
    colState.updateCardinality(innerCardinality());
    if (resultSetLoader.writeable()) {
      colState.allocateVectors();
    }
    return colState.writer();
  }

  protected void columnAdded(ColumnState colState) { }

  @SuppressWarnings("resource")
  private ColumnState buildPrimitive(ColumnMetadata columnSchema) {
    ValueVector vector;
    if (columnSchema.isProjected()) {

      // Create the vector for the column.

      ResultVectorCache cache = resultSetLoader.vectorCache();
      vector = cache.addOrGet(columnSchema.schema());

      // In permissive mode, the mode or precision of the vector may differ
      // from that requested. Update the schema to match.

      if (cache.isPermissive() && ! vector.getField().isEquivalent(columnSchema.schema())) {
        columnSchema = ((PrimitiveColumnMetadata) columnSchema).mergeWith(vector.getField());
      }
    } else {

      // Column is not projected. No materialized backing for the column.

      vector = null;
    }

    // Create the writer. Will be returned to the tuple writer.

    AbstractObjectWriter colWriter = ColumnWriterFactory.buildColumnWriter(columnSchema, vector);

    if (columnSchema.isArray()) {
      return PrimitiveColumnState.newPrimitiveArray(resultSetLoader, vector, colWriter);
    } else {
      return PrimitiveColumnState.newPrimitive(resultSetLoader, vector, colWriter);
    }
  }

  private ColumnState buildMap(ColumnMetadata columnSchema) {

    // When dynamically adding columns, must add the (empty)
    // map by itself, then add columns to the map via separate
    // calls.

    assert columnSchema.isMap();
    assert columnSchema.mapSchema().size() == 0;

    // Create the writer. Will be returned to the tuple writer.

    ProjectionSet childProjection = projectionSet.mapProjection(columnSchema.name());
    if (columnSchema.isArray()) {
      return MapArrayColumnState.build(resultSetLoader,
          columnSchema,
          childProjection);
    } else {
      return new MapColumnState(resultSetLoader,
          columnSchema,
          childProjection);
    }
  }

  public void buildSchema(TupleMetadata schema) {
    for (int i = 0; i < schema.size(); i++) {
      ColumnMetadata colSchema = schema.metadata(i);
      AbstractObjectWriter colWriter;
      if (colSchema.isMap()) {
        colWriter = addColumn(colSchema.cloneEmpty());
        BaseMapColumnState mapColState = (BaseMapColumnState) columns.get(columns.size() - 1);
        mapColState.mapState().buildSchema(colSchema.mapSchema());
      } else {
        colWriter = addColumn(colSchema);
      }
      writer().addColumnWriter(colWriter);
    }
  }

  public void updateCardinality(int cardinality) {
    for (ColumnState colState : columns) {
      colState.updateCardinality(cardinality);
    }
  }

  /**
   * A column within the row batch overflowed. Prepare to absorb the rest of the
   * in-flight row by rolling values over to a new vector, saving the complete
   * vector for later. This column could have a value for the overflow row, or
   * for some previous row, depending on exactly when and where the overflow
   * occurs.
   *
   * @param overflowIndex
   *          the index of the row that caused the overflow, the values of which
   *          should be copied to a new "look-ahead" vector
   */

  public void rollover() {
    for (ColumnState colState : columns) {
      colState.rollover();
    }
  }

  /**
   * Writing of a row batch is complete, and an overflow occurred. Prepare the
   * vector for harvesting to send downstream. Set aside the look-ahead vector
   * and put the full vector buffer back into the active vector.
   */

  public void harvestWithLookAhead() {
    for (ColumnState colState : columns) {
      colState.harvestWithLookAhead();
    }
  }

  /**
   * Start a new batch by shifting the overflow buffers back into the main
   * write vectors and updating the writers.
   */

  public void startBatch(boolean schemaOnly) {
    for (ColumnState colState : columns) {
      colState.startBatch(schemaOnly);
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

  /**
   * Clean up state (such as backup vectors) associated with the state
   * for each vector.
   */

  public void close() {
    for (ColumnState colState : columns) {
      colState.close();
    }
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
