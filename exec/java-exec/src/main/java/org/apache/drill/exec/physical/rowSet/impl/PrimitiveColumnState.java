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
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;

public abstract class PrimitiveColumnState extends ColumnState {

  public static class SimplePrimitiveColumnState extends PrimitiveColumnState {
    public SimplePrimitiveColumnState(ResultSetLoaderImpl resultSetLoader,
        PrimitiveColumnModel columnModel) {
      super(resultSetLoader, columnModel,
          new ValuesVectorState(
              columnModel.schema(),
              (AbstractScalarWriter) columnModel.writer().scalar(),
              columnModel.vector()));
    }
  }

  public static class PrimitiveArrayColumnState extends PrimitiveColumnState {

    public PrimitiveArrayColumnState(ResultSetLoaderImpl resultSetLoader,
        PrimitiveColumnModel columnModel) {
      super(resultSetLoader, columnModel,
          new RepeatedVectorState(columnModel));
    }

    @Override
    protected void adjustWriterIndex(int newIndex) {
      ((AbstractArrayWriter) columnModel.writer().array()).resetElementIndex(newIndex);
    }
  }

  protected final PrimitiveColumnModel columnModel;

  public PrimitiveColumnState(ResultSetLoaderImpl resultSetLoader,
      PrimitiveColumnModel columnModel,
      VectorState vectorState) {
    super(resultSetLoader, vectorState);
    this.columnModel = columnModel;
  }

  @Override
  public void overflowed(AbstractSingleColumnModel model) {
    assert columnModel == model;
    resultSetLoader.overflowed();
  }

  public ValueVector vector() { return columnModel.vector(); }

  public ColumnMetadata schema() { return columnModel.schema(); }
}

