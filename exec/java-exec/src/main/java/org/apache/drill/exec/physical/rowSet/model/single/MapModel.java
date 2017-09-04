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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

/**
 * Represents the tuple structure of a map column.
 */

public class MapModel extends AbstractSingleTupleModel {
  private final AbstractMapVector vector;
  private ObjectWriter writer;

  /**
   * Construct a new, empty map model using the tuple schema provided
   * (which is generally provided by the map column schema.)
   *
   * @param schema empty tuple schema
   * @param vector empty map vector
   */

  public MapModel(TupleSchema schema, AbstractMapVector vector) {
    super(schema, new ArrayList<ColumnModel>());
    this.vector = vector;
    assert schema.size() == 0;
  }

  /**
   * Build the map model given the map vector itself and the
   * list of column models that represent the map contents, and the tuple
   * metadata that describes the columns. The same {@link MaterializedField}
   * must back both the vector and tuple metadata.
   *
   * @param schema metadata for the map
   * @param vector vector which holds the map
   * @param columns column models for each column in the map
   */
  public MapModel(TupleSchema schema, AbstractMapVector vector, List<ColumnModel> columns) {
    super(schema, columns);
    this.vector = vector;
    assert vector.size() == schema.size();
  }

  @Override
  public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
    if (schema.parent().isArray()) {
      return visitor.visitMapArray(this, arg);
    } else {
      return visitor.visitMap(this, arg);
    }
  }

  public AbstractMapVector vector() { return vector; }

  @Override
  public BufferAllocator allocator() {
    return vector.getAllocator();
  }

  @Override
  public void addColumnImpl(AbstractSingleColumnModel colModel) {
    addBaseColumn(colModel);
    vector.putChild(colModel.schema().name(), colModel.vector());
    assert vector.size() == columns.size();
  }

  public void bindWriter(ObjectWriter writer) {
    this.writer = writer;
  }

  public ObjectWriter writer() { return writer; }

  @Override
  public void dump(HierarchicalFormatter format) {
    format.extend();
    super.dump(format);
    format
      .attributeIdentity("vector", vector)
      .attribute("schema", vector.getField())
      .attributeIdentity("writer", writer)
      .endObject();
  }
}
