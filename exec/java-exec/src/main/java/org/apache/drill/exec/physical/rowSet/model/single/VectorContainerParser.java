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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.ColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.MapColumnMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

/**
 * Build a row set model from a {@link VectorContainer} which already contains
 * a set of vectors. Builds the model using a depth-first traversal of the
 * columns in the vector container.
 */

public class VectorContainerParser {

  public SingleRowSetModel buildModel(VectorContainer container) {
    SingleRowSetModel rowModel = new SingleRowSetModel(container);
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      rowModel.addColumnImpl(buildColumn(vector));
    }
    return rowModel;
  }

  private AbstractSingleColumnModel buildColumn(ValueVector vector) {
    if (vector.getField().getType().getMinorType() == MinorType.MAP) {
      return buildMapColumn((AbstractMapVector) vector);
    } else {
      ColumnMetadata colSchema = TupleSchema.fromField(vector.getField());
      return new PrimitiveColumnModel(colSchema, vector);
    }
  }

  private AbstractSingleColumnModel buildMapColumn(AbstractMapVector vector) {
    MapModel mapModel = buildMap(vector);
    MapColumnMetadata schema = TupleSchema.newMap(vector.getField(), (TupleSchema) mapModel.schema());
    return new MapColumnModel(schema, vector, mapModel);
  }

  /**
   * Build the map tuple from the vectors contained in the map vector.
   *
   * @param vector the map vector
   * @return the corresponding map tuple
   */

  private MapModel buildMap(AbstractMapVector vector) {
    List<ColumnModel> columns = new ArrayList<>();
    List<ColumnMetadata> colMetadata = new ArrayList<>();
    for (ValueVector child : vector) {
      AbstractSingleColumnModel colModel = buildColumn(child);
      columns.add(colModel);
      colMetadata.add(colModel.schema());
    }
    TupleSchema schema = TupleSchema.fromColumns(colMetadata);
    return new MapModel(schema, vector, columns);
  }
}
