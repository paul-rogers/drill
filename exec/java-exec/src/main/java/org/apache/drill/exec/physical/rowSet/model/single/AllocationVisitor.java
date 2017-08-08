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

import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.*;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.RepeatedFixedWidthVectorLike;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedVariableWidthVectorLike;

/**
 * Walk the row set tree to allocate new vectors according to a given
 * row count and the size information provided in column metadata.
 * <p>
 * @see {@link AllocationHelper} - the class which this one replaces
 * @see {@link VectorInitializer} - an earlier cut at implementation
 * based on data from the {@link RecordBatchSizer}
 */

// TODO: Does not yet handle lists; lists are a simple extension
// of the array-handling logic below.

public class AllocationVisitor extends ModelVisitor<Void, Integer> {

  public void allocate(SingleRowSetModel rowModel, int rowCount) {
    rowModel.visit(this, rowCount);
  }

  @SuppressWarnings("resource")
  @Override
  public Void visitPrimitiveColumn(PrimitiveColumnModel column, Integer valueCount) {
    ColumnMetadata schema = column.schema();
    ValueVector vector = column.vector();
    if (schema.isVariableWidth()) {
      final int byteCount = valueCount * schema.expectedWidth();
      ((VariableWidthVector) vector).allocateNew(byteCount, valueCount);
    } else {
      ((FixedWidthVector) vector).allocateNew(valueCount);
    }
    return null;
  }

  @SuppressWarnings("resource")
  @Override
  protected Void visitPrimitiveArrayColumn(PrimitiveColumnModel column, Integer valueCount) {
    ColumnMetadata schema = column.schema();
    ValueVector vector = column.vector();
    int expectedElementCount = schema.expectedElementCount();
    if (schema.isVariableWidth()) {
      final int byteCount = expectedElementCount * schema.expectedWidth();
      ((RepeatedVariableWidthVectorLike) vector).allocateNew(byteCount, valueCount, expectedElementCount);
    } else {
      ((RepeatedFixedWidthVectorLike) vector).allocateNew(valueCount, expectedElementCount);
    }
    return null;
  }

  @Override
  protected Void visitMapArrayColumn(MapColumnModel column, Integer valueCount) {
    int expectedValueCount = valueCount = column.schema().expectedElementCount();
    ((RepeatedMapVector) column.vector()).getOffsetVector().allocateNew(expectedValueCount);
    column.mapModelImpl().visit(this, expectedValueCount);
    return null;
  }
}
