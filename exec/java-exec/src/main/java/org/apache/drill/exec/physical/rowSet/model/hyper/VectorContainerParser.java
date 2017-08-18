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
package org.apache.drill.exec.physical.rowSet.model.hyper;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.ColumnModel;
import org.apache.drill.exec.physical.rowSet.model.hyper.AbstractHyperTupleModel.AbstractHyperColumnModel;
import org.apache.drill.exec.physical.rowSet.model.hyper.HyperRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.hyper.HyperRowSetModel.MapModel;
import org.apache.drill.exec.physical.rowSet.model.hyper.HyperRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.MaterializedField;
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

  public HyperRowSetModel buildModel(VectorContainer container) {
    List<ColumnModel> columns = new ArrayList<>();
    List<ColumnMetadata> colMetadata = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      AbstractHyperColumnModel colModel = buildColumn((HyperVectorWrapper<? extends ValueVector>) container.getValueVector(i));
      columns.add(colModel);
      colMetadata.add(colModel.schema());
    }
    TupleSchema schema = TupleSchema.fromColumns(colMetadata);
    return new HyperRowSetModel(container, schema, columns);
  }

  @SuppressWarnings("unchecked")
  private AbstractHyperColumnModel buildColumn(HyperVectorWrapper<? extends ValueVector> vectors) {
    if (vectors.getField().getType().getMinorType() == MinorType.MAP) {
      return buildMapColumn((HyperVectorWrapper<? extends AbstractMapVector>) vectors);
    } else {
      ColumnMetadata colSchema = TupleSchema.fromField(vectors.getField());
      return new PrimitiveColumnModel(colSchema, vectors);
    }
  }

  private AbstractHyperColumnModel buildMapColumn(HyperVectorWrapper<? extends AbstractMapVector> vectors) {
    MapModel mapModel = buildMap(vectors);
    MapColumnMetadata schema = TupleSchema.newMap(vectors.getField(), (TupleSchema) mapModel.schema());
    return new MapColumnModel(schema, vectors, mapModel);
  }

  /**
   * Build the map tuple from the vectors contained in the map vector.
   *
   * @param vector the map vector
   * @return the corresponding map tuple
   */

  private MapModel buildMap(HyperVectorWrapper<? extends AbstractMapVector> vectors) {
    List<ColumnModel> columns = new ArrayList<>();
    List<ColumnMetadata> colMetadata = new ArrayList<>();
    MaterializedField mapField = vectors.getField();
    for (int i = 0; i < mapField.getChildren().size(); i++) {
      AbstractHyperColumnModel colModel = buildColumn((HyperVectorWrapper<? extends ValueVector>) vectors.getChildWrapper(new int[] {i}));
      columns.add(colModel);
      colMetadata.add(colModel.schema());
    }
    TupleSchema schema = TupleSchema.fromColumns(colMetadata);
    return new MapModel(schema, vectors, columns);
  }
}
