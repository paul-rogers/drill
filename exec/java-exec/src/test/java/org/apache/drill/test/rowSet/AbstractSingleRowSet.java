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
package org.apache.drill.test.rowSet;

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

public abstract class AbstractSingleRowSet extends AbstractRowSet implements SingleRowSet {

  protected final ValueVector[] valueVectors;

  public AbstractSingleRowSet(BufferAllocator allocator, RowSetSchema schema) {
    super(allocator, schema, new VectorContainer());

    valueVectors = new ValueVector[schema.count()];
    for (int i = 0; i < schema.count(); i++) {
      final MaterializedField field = schema.get(i);
      @SuppressWarnings("resource")
      ValueVector v = TypeHelper.getNewVector(field, allocator, callBack);
      valueVectors[i] = v;
      container.add(v);
    }
    container.buildSchema(SelectionVectorMode.NONE);
  }

  public AbstractSingleRowSet(BufferAllocator allocator, VectorContainer container) {
    super(allocator, new RowSetSchema(container.getSchema()), container);
    valueVectors = new ValueVector[container.getNumberOfColumns()];
    int i = 0;
    for (VectorWrapper<?> w : container) {
      valueVectors[i++] = w.getValueVector();
    }
  }

  public AbstractSingleRowSet(AbstractSingleRowSet rowSet) {
    super(rowSet.allocator, rowSet.schema, rowSet.container);
    valueVectors = rowSet.valueVectors;
  }

  @Override
  public ValueVector[] vectors() { return valueVectors; }

  @Override
  public int getSize() {
    RecordBatchSizer sizer = new RecordBatchSizer(container);
    return sizer.actualSize();
  }
}
