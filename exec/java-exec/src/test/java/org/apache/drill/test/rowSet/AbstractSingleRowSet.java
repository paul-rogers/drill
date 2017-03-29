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
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.TupleAccessor.TupleSchema;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnReader;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.impl.TupleReaderImpl;
import org.apache.drill.exec.vector.accessor.impl.TupleWriterImpl;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetSchema.LogicalColumn;
import org.apache.drill.test.rowSet.RowSetSchema.PhysicalSchema;

public abstract class AbstractSingleRowSet extends AbstractRowSet implements SingleRowSet {

  public abstract static class StructureBuilder {
    protected final PhysicalSchema schema;
    protected final BufferAllocator allocator;
    protected final ValueVector[] valueVectors;
    protected final MapVector[] mapVectors;
    protected int vectorIndex;
    protected int mapIndex;

    public StructureBuilder(BufferAllocator allocator, RowSetSchema schema) {
      this.allocator = allocator;
      this.schema = schema.physical();
      valueVectors = new ValueVector[schema.access().count()];
      if (schema.access().mapCount() == 0) {
        mapVectors = null;
      } else {
        mapVectors = new MapVector[schema.access().mapCount()];
      }
    }
  }

  public static class VectorBuilder extends StructureBuilder {

    public VectorBuilder(BufferAllocator allocator, RowSetSchema schema) {
      super(allocator, schema);
    }

    public ValueVector[] buildContainer(VectorContainer container) {
      for (int i = 0; i < schema.count(); i++) {
        LogicalColumn colSchema = schema.column(i);
        @SuppressWarnings("resource")
        ValueVector v = TypeHelper.getNewVector(colSchema.field, allocator, null);
        container.add(v);
        if (colSchema.field.getType().getMinorType() == MinorType.MAP) {
          MapVector mv = (MapVector) v;
          mapVectors[mapIndex++] = mv;
          buildMap(mv, colSchema.mapSchema);
        } else {
          valueVectors[vectorIndex++] = v;
        }
      }
      container.buildSchema(SelectionVectorMode.NONE);
      return valueVectors;
    }

    private void buildMap(MapVector mapVector, PhysicalSchema mapSchema) {
      for (int i = 0; i < mapSchema.count(); i++) {
        LogicalColumn colSchema = mapSchema.column(i);
        MajorType type = colSchema.field.getType();
        Class<? extends ValueVector> vectorClass = TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode());
        @SuppressWarnings("resource")
        ValueVector v = mapVector.addOrGet(colSchema.field.getName(), type, vectorClass);
        if (type.getMinorType() == MinorType.MAP) {
          MapVector mv = (MapVector) v;
          mapVectors[mapIndex++] = mv;
          buildMap(mv, colSchema.mapSchema);
        } else {
          valueVectors[vectorIndex++] = v;
        }
      }
    }
  }

  public static class VectorMapper extends StructureBuilder {

    public VectorMapper(BufferAllocator allocator, RowSetSchema schema) {
      super(allocator, schema);
    }

    public ValueVector[] mapContainer(VectorContainer container) {
      for (VectorWrapper<?> w : container) {
        @SuppressWarnings("resource")
        ValueVector v = w.getValueVector();
        if (v.getField().getType().getMinorType() == MinorType.MAP) {
          MapVector mv = (MapVector) v;
          mapVectors[mapIndex++] = mv;
          buildMap(mv);
        } else {
          valueVectors[vectorIndex++] = v;
        }
      }
      return valueVectors;
    }

    private void buildMap(MapVector mapVector) {
      for (ValueVector v : mapVector) {
        if (v.getField().getType().getMinorType() == MinorType.MAP) {
          MapVector mv = (MapVector) v;
          mapVectors[mapIndex++] = mv;
          buildMap(mv);
        } else {
          valueVectors[vectorIndex++] = v;
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
    valueVectors = new VectorMapper(allocator, super.schema).mapContainer(container);
  }

  public AbstractSingleRowSet(AbstractSingleRowSet rowSet) {
    super(rowSet.allocator, rowSet.schema.batch(), rowSet.container);
    valueVectors = rowSet.valueVectors;
  }

  @Override
  public ValueVector[] vectors() { return valueVectors; }

  @Override
  public int getSize() {
    RecordBatchSizer sizer = new RecordBatchSizer(container);
    return sizer.actualSize();
  }

  protected RowSetReader buildReader(RowSetIndex rowIndex) {
    TupleSchema accessSchema = schema().access();
    ValueVector[] valueVectors = vectors();
    AbstractColumnReader[] readers = new AbstractColumnReader[valueVectors.length];
    for (int i = 0; i < readers.length; i++) {
      MinorType type = accessSchema.column(i).getType().getMinorType();
      if (type == MinorType.MAP) {
        readers[i] = null; // buildMapAccessor(i);
      } else if (type == MinorType.LIST) {
        readers[i] = null; // buildListAccessor(i);
      } else {
        readers[i] = ColumnAccessorFactory.newReader(valueVectors[i].getField().getType());
        readers[i].bind(rowIndex, valueVectors[i]);
      }
    }
    return new RowSetReaderImpl(accessSchema, rowIndex, new TupleReaderImpl(accessSchema, readers));
  }

  protected RowSetWriter buildWriter(RowSetIndex rowIndex) {
    ValueVector[] valueVectors = vectors();
    AbstractColumnWriter[] writers = new AbstractColumnWriter[valueVectors.length];
    int posn = 0;
    for (int i = 0; i < writers.length; i++) {
      writers[posn] = ColumnAccessorFactory.newWriter(valueVectors[i].getField().getType());
      writers[posn].bind(rowIndex, valueVectors[i]);
      posn++;
    }
    TupleSchema accessSchema = schema().access();
    return new RowSetWriterImpl(accessSchema, rowIndex, new TupleWriterImpl(accessSchema, writers));
  }
}
