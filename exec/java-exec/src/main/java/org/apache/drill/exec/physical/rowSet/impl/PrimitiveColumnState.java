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

import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.ValuesVectorState;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;

/**
 * Represents a primitive (scalar) column: either a simple
 * scalar (optional or required), or an array of scalars
 * (repeated scalar.)
 */

public class PrimitiveColumnState extends ColumnState {

  protected final PrimitiveColumnModel columnModel;

  public PrimitiveColumnState(ResultSetLoaderImpl resultSetLoader,
      PrimitiveColumnModel columnModel,
      VectorState vectorState) {
    super(resultSetLoader, vectorState);
    this.columnModel = columnModel;
  }

  public static PrimitiveColumnState newSimplePrimitive(
      ResultSetLoaderImpl resultSetLoader,
      PrimitiveColumnModel columnModel) {
    return new PrimitiveColumnState(resultSetLoader, columnModel,
        new ValuesVectorState(
            columnModel.schema(),
            (AbstractScalarWriter) columnModel.writer().scalar(),
            columnModel.vector()));
  }

  public static PrimitiveColumnState newPrimitiveArray(
      ResultSetLoaderImpl resultSetLoader,
      PrimitiveColumnModel columnModel) {
    return new PrimitiveColumnState(resultSetLoader, columnModel,
        new RepeatedVectorState(columnModel));
  }

  @Override
  public void overflowed(AbstractSingleColumnModel model) {
    assert columnModel == model;
    resultSetLoader.overflowed();
  }

  public ValueVector vector() { return columnModel.vector(); }

  public ColumnMetadata schema() { return columnModel.schema(); }
}
