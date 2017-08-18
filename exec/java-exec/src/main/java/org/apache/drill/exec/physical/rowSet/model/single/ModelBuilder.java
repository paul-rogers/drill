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
package org.apache.drill.exec.physical.rowSet.model.single;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.NullResultVectorCacheImpl;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.ColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.MapColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

/**
 * Constructs all or part of a model from a schema. Builds the vectors,
 * vector container and the model given a schema using a depth-first
 * traversal of the schema. Also used to build parts of a model
 * incrementally.
 */

public class ModelBuilder {

  private final ResultVectorCache vectorCache;

  public ModelBuilder(BufferAllocator allocator) {
    this.vectorCache = new NullResultVectorCacheImpl(allocator);
  }

  public ModelBuilder(ResultVectorCache vectorCache) {
    this.vectorCache = vectorCache;
  }

  /**
   * Build a row set model given a metadata description of the row set.
   *
   * @param schema the metadata description
   * @return a materialized row set model containing the vectors that
   * implement the model
   */

  public SingleRowSetModel buildModel(TupleMetadata schema) {
    VectorContainer container = new VectorContainer(vectorCache.allocator());
    List<ColumnModel> columns = new ArrayList<>();
    for (int i = 0; i < schema.size(); i++) {
      AbstractSingleColumnModel colModel = buildColumn(schema.metadata(i));
      columns.add(colModel);
      container.add(colModel.vector());
    }

    // Build the row set from a matching triple of schema, container and
    // column models.

    container.buildSchema(SelectionVectorMode.NONE);
    return new SingleRowSetModel(schema, container, columns);
  }

  /**
   * Build a column including the backing vector.
   *
   * @param schema schema description of the column
   * @return column model with a backing vector
   */

  @SuppressWarnings("resource")
  public AbstractSingleColumnModel buildColumn(ColumnMetadata schema) {
    if (schema.isMap()) {
      return buildMap(schema);
    } else {
      ValueVector vector = vectorCache.addOrGet(schema.schema());
      return new PrimitiveColumnModel(schema, vector);
    }
  }

  /**
   * Build a map column including the members of the map given a map
   * column schema.
   *
   * @param schema the schema of the map column
   * @return the completed map vector column model
   */

  @SuppressWarnings("resource")
  private AbstractSingleColumnModel buildMap(ColumnMetadata schema) {

    // Creating the map vector will create its contained vectors if we
    // give it a materialized field with children. So, instead pass a clone
    // without children so we can add them.

    MaterializedField emptyClone = schema.schema().clone();

    // Don't get the map vector from the vector cache. Map vectors may
    // have content that varies from batch to batch. Only the leaf
    // vectors can be cached.

    AbstractMapVector mapVector = (AbstractMapVector) TypeHelper.getNewVector(emptyClone, vectorCache.allocator(), null);

    // Creating the vector cloned the schema a second time. Replace the
    // field in the column metadata to match the one in the vector.
    // Doing so is an implementation hack, so acess a method on the
    // implementation class.

    ((AbstractColumnMetadata) schema).replaceField(mapVector.getField());

    // Create the contents building the model as we go.

    TupleMetadata mapSchema = schema.mapSchema();
    List<ColumnModel> columns = new ArrayList<>();
    for (int i = 0; i < mapSchema.size(); i++) {
      AbstractSingleColumnModel colModel = buildColumn(mapSchema.metadata(i));
      columns.add(colModel);
      mapVector.putChild(colModel.schema().name(), colModel.vector());
    }

    // Build the map model from a matching triple of schema, container and
    // column models.

    MapModel mapModel = new MapModel((TupleSchema) mapSchema, mapVector, columns);

    // Create the map model with all the pieces.

    return new MapColumnModel((MapColumnMetadata) schema, mapVector, mapModel);
  }
}
