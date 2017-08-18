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

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.BuildStateVisitor;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.UpdateCardinalityVisitor;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.TupleCoordinator;
import org.apache.drill.exec.physical.rowSet.model.single.AllocationVisitor;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.MapColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.accessor.writer.ObjectArrayWriter;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

public abstract class TupleState implements TupleCoordinator {

  public static class RowState extends TupleState {

    @SuppressWarnings("unused")
    private final SingleRowSetModel row;

    public RowState(ResultSetLoaderImpl rsLoader, SingleRowSetModel row) {
      super(rsLoader);
      this.row = row;
    }

    @Override
    public int innerCardinality() { return resultSetLoader.targetRowCount();}

    @Override
    public void dump(HierarchicalFormatter format) {
      format.startObject(this).endObject();
    }
  }

  public static class MapState extends TupleState {

    private final MapColumnModel mapColumn;
    protected int outerCardinality;

    public MapState(ResultSetLoaderImpl rsLoader, MapColumnModel mapColumn) {
      super(rsLoader);
      this.mapColumn = mapColumn;
    }

    public void setCardinality(int outerCardinality) {
      this.outerCardinality = outerCardinality;
    }

    @Override
    public int innerCardinality() {
      return outerCardinality * mapColumn.schema().expectedElementCount();
    }

    @Override
    public void dump(HierarchicalFormatter format) {
      format
        .startObject(this)
        .attribute("column", mapColumn.schema().name())
        .attribute("cardinality", outerCardinality)
        .endObject();
    }
  }

  protected final ResultSetLoaderImpl resultSetLoader;

  protected TupleState(ResultSetLoaderImpl rsLoader) {
    this.resultSetLoader = rsLoader;
  }

  @Override
  public void columnAdded(AbstractSingleTupleModel tuple,
      AbstractSingleColumnModel column) {

    // Columns must be added via the writer.

    assert false;
  }

  public abstract int innerCardinality();

  @Override
  public ObjectWriter columnAdded(
      AbstractSingleTupleModel tupleModel,
      TupleWriter tupleWriter, ColumnMetadata columnSchema) {

    // Verify name is not a (possibly case insensitive) duplicate.

    TupleMetadata tupleSchema = tupleModel.schema();
    String colName = columnSchema.name();
    if (tupleSchema.column(colName) != null) {
      throw new IllegalArgumentException("Duplicate column: " + colName);
    }

    if (columnSchema.isMap()) {
      return buildMap(tupleModel, columnSchema);
    } else {
      return buildPrimitive(tupleModel, columnSchema);
    }
  }

  @SuppressWarnings("resource")
  private AbstractObjectWriter buildPrimitive(AbstractSingleTupleModel tupleModel,
      ColumnMetadata columnSchema) {

    // Create the vector for the column.

    ValueVector vector = resultSetLoader.vectorCache().addOrGet(columnSchema.schema());

    // Create the column writer and add it.

    PrimitiveColumnModel colModel = new PrimitiveColumnModel(columnSchema, vector);
    tupleModel.addColumnImpl(colModel);

    // Create the writer. Will be returned to the tuple writer.

    AbstractObjectWriter colWriter = ColumnAccessorFactory.buildColumnWriter(vector);

    // Bind the writer to the model.

    colModel.bindWriter(colWriter);

    prepareColumn(colModel);
    return colWriter;
  }

  @SuppressWarnings("resource")
  private AbstractObjectWriter buildMap(AbstractSingleTupleModel tupleModel,
      ColumnMetadata columnSchema) {

    // When dynamically adding columns, must add the (empty)
    // map by itself, then add columns to the map via separate
    // calls.

    assert columnSchema.isMap();
    assert columnSchema.mapSchema().size() == 0;

    // Don't get the map vector from the vector cache. Map vectors may
    // have content that varies from batch to batch. Only the leaf
    // vectors can be cached.

    AbstractMapVector mapVector = (AbstractMapVector) TypeHelper.getNewVector(
        columnSchema.schema(),
        resultSetLoader.allocator(),
        null);

    // Creating the vector cloned the schema. Replace the
    // field in the column metadata to match the one in the vector.
    // Doing so is an implementation hack, so access a method on the
    // implementation class.

    ((AbstractColumnMetadata) columnSchema).replaceField(mapVector.getField());

    // Build the map model from a matching triple of schema, container and
    // column models.

    MapModel mapModel = new MapModel((TupleSchema) columnSchema.mapSchema(), mapVector);

    // Create the map model with all the pieces.

    MapColumnModel mapColModel = new MapColumnModel((MapColumnMetadata) columnSchema, mapVector, mapModel);
    tupleModel.addColumnImpl(mapColModel);

    // Create the writer. Will be returned to the tuple writer.

    AbstractObjectWriter mapWriter = MapWriter.build(columnSchema, mapVector);
    if (columnSchema.isArray()) {
      mapWriter = ObjectArrayWriter.build((RepeatedMapVector) mapVector, mapWriter);
    }

    // Bind the writer to the model.

    mapColModel.bindWriter(mapWriter);
    mapModel.bindWriter(mapWriter);

    prepareColumn(mapColModel);
    return mapWriter;
  }

  private void prepareColumn(AbstractSingleColumnModel colModel) {

    // Use visitors to build the state, define cardinality and allocate
    // vectors.

    colModel.visit(new BuildStateVisitor(resultSetLoader), null);
    colModel.visit(new UpdateCardinalityVisitor(), innerCardinality());
    colModel.visit(new AllocationVisitor(), innerCardinality());
  }
}
