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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.TupleSchema.ColumnSchema;
import org.apache.drill.test.rowSet.TupleSchema.MapSchema;
import org.apache.drill.test.rowSet.TupleSchema.RowSetSchema;

public abstract class AbstractSingleRowSet extends AbstractRowSet implements SingleRowSet {

  public static class VectorBuilder {
    RowSetSchema schema;
    protected final BufferAllocator allocator;
    protected final ValueVector[] valueVectors;
    protected int vectorIndex;

    public VectorBuilder(BufferAllocator allocator, RowSetSchema schema) {
      this.allocator = allocator;
      this.schema = schema;
      valueVectors = new ValueVector[schema.count()];
    }

    public ValueVector[] buildContainer(VectorContainer container) {
      while (vectorIndex < schema.count()) {
        ColumnSchema colSchema = schema.getColumn(vectorIndex);
        @SuppressWarnings("resource")
        ValueVector v = TypeHelper.getNewVector(colSchema.field, allocator, null);
        valueVectors[vectorIndex++] = v;
        container.add(v);
        if (colSchema.field.getType().getMinorType() == MinorType.MAP) {
          buildMap((MapVector) v, colSchema.mapSchema);
        }
      }
      container.buildSchema(SelectionVectorMode.NONE);
      return valueVectors;
    }

    private void buildMap(MapVector mapVector, MapSchema mapSchema) {
      for (int i = 0; i < mapSchema.count(); i++) {
        ColumnSchema colSchema = mapSchema.getColumn(i);
        MajorType type = colSchema.field.getType();
        Class<? extends ValueVector> vectorClass = TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode());
        @SuppressWarnings("resource")
        ValueVector v = mapVector.addOrGet(colSchema.field.getName(), type, vectorClass);
        valueVectors[vectorIndex++] = v;
        if (type.getMinorType() == MinorType.MAP) {
          buildMap((MapVector) v, colSchema.mapSchema);
        }
      }
    }
  }

  protected final ValueVector[] valueVectors;

  public AbstractSingleRowSet(BufferAllocator allocator, BatchSchema schema) {
    super(allocator, schema, new VectorContainer());
    valueVectors = new VectorBuilder(allocator, super.schema).buildContainer(container);
  }

  public AbstractSingleRowSet(BufferAllocator allocator, VectorContainer container) {
    super(allocator, container.getSchema(), container);
    valueVectors = new ValueVector[container.getNumberOfColumns()];
    int i = 0;
    for (VectorWrapper<?> w : container) {
      valueVectors[i++] = w.getValueVector();
    }
  }

  public AbstractSingleRowSet(AbstractSingleRowSet rowSet) {
    super(rowSet.allocator, rowSet.schema.getSchema(), rowSet.container);
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
