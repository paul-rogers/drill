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

import org.apache.drill.exec.physical.rowSet.model.TupleModel;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.MapColumnMetadata;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

/**
 * Map column definition. Represents both single and array maps. Maps
 * contain a nested tuple structure. (Not that, in this model, map
 * columns, themselves, are not maps. Rather, map columns contain a
 * tuple. This makes the class structure much simpler.)
 */

public class MapColumnModel extends AbstractSingleColumnModel {
  private final MapModel mapModel;
  private final AbstractMapVector vector;

  /**
   * Construct a map column model from a schema, vector and map which are
   * all already populated.
   *
   * @param schema metadata description of the map column
   * @param vector vector that backs the column
   * @param mapModel tuple model for the columns within the map
   */
  public MapColumnModel(MapColumnMetadata schema, AbstractMapVector vector, MapModel mapModel) {
    super(schema);
    this.vector = vector;
    this.mapModel = mapModel;
    assert schema.mapSchema() == mapModel.schema();
    assert schema.schema() == vector.getField();
    assert vector.size() == schema.mapSchema().size();
  }

  /**
   * Create a new, empty map column using the vector and metadata provided.
   *
   * @param schema metadata description of the map column
   * @param vector vector that backs the column
   */

  public MapColumnModel(ColumnMetadata schema, AbstractMapVector vector) {
    super(schema);
    this.vector = vector;
    mapModel = new MapModel((TupleSchema) schema.mapSchema(), vector);
    assert schema.mapSchema() == mapModel.schema();
    assert schema.schema() == vector.getField();
    assert vector.size() == 0;
    assert schema.mapSchema().size() == 0;
    assert vector.getField().getChildren().size() == 0;
  }

  @Override
  public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
    if (schema.isArray()) {
      return visitor.visitMapArrayColumn(this, arg);
    } else {
      return visitor.visitMapColumn(this, arg);
    }
  }

  @Override
  public TupleModel mapModel() { return mapModel; }

  public MapModel mapModelImpl() { return mapModel; }

  @Override
  public AbstractMapVector vector() { return vector; }

  public void allocateArray(int valueCount) {
    ((RepeatedMapVector) vector).getOffsetVector().allocateNew(valueCount);
  }

  @Override
  public void clear() {
    if (vector != null) {
      vector.clear();
    }
  }
}
