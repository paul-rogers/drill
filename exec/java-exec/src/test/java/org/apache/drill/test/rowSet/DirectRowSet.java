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

import java.util.Collection;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.TupleAccessor.TupleSchema;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSetWriterImpl.WriterIndexImpl;

/**
 * Implementation of a single row set with no indirection (selection)
 * vector.
 */

public class DirectRowSet extends AbstractSingleRowSet implements ExtendableRowSet {

  /**
   * Reader index that points directly to each row in the row set.
   * This index starts with pointing to the -1st row, so that the
   * reader can require a <tt>next()</tt> for every row, including
   * the first. (This is the JDBC RecordSet convention.)
   */

  private static class DirectRowIndex extends BoundedRowIndex {

    public DirectRowIndex(int rowCount) {
      super(rowCount);
    }

    @Override
    public int vectorIndex() { return rowIndex; }

    @Override
    public int batchIndex() { return 0; }
  }

  private WriterIndexImpl index;

  public DirectRowSet(BufferAllocator allocator, BatchSchema schema) {
    super(allocator, schema);
  }

  public DirectRowSet(BufferAllocator allocator, VectorContainer container) {
    super(allocator, container);
  }

  public DirectRowSet(BufferAllocator allocator, VectorAccessible va) {
    super(allocator, toContainer(va, allocator));
  }

  private static VectorContainer toContainer(VectorAccessible va, BufferAllocator allocator) {
    VectorContainer container = VectorContainer.getTransferClone(va, allocator);
    container.buildSchema(SelectionVectorMode.NONE);
    container.setRecordCount(va.getRecordCount());
    return container;
  }

  @Override
  public void allocate(int recordCount) {
    for (final ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, recordCount, 50, 10);
    }
  }

  @Override
  public RowSetWriter writer() {
    return writer(true, 10);
  }

  @Override
  public RowSetWriter writer(boolean flatSchema, int initialRowCount) {
    if (container.hasRecordCount()) {
      throw new IllegalStateException("Row set already contains data");
    }
    allocate(initialRowCount);
    index = new WriterIndexImpl();
    if (flatSchema) {
      return buildFlattenedWriter(index);
    } else {
      return null;
    }
  }

  /**
   * Build writer objects for each column based on the column type.
   *
   * @param rowIndex the index which points to each row
   * @return an array of writers
   */

  protected RowSetWriter buildFlattenedWriter(WriterIndexImpl rowIndex) {
    ValueVector[] valueVectors = vectors();
    AbstractObjectWriter[] writers = new AbstractObjectWriter[valueVectors.length];
    for (int i = 0; i < writers.length; i++) {
      writers[i] = ColumnAccessorFactory.buildColumnWriter(rowIndex, valueVectors[i]);
    }
    TupleSchema accessSchema = schema().flatAccess();
    return new RowSetWriterImpl(this, accessSchema, rowIndex, writers);
  }

  protected RowSetWriter buildNestedWriter(WriterIndexImpl rowIndex) {
    VectorContainer container = container();
    int vectorCount = container.getNumberOfColumns();
    TupleSchema accessSchema = schema().hierarchicalAccess();
    AbstractObjectWriter[] writers = new AbstractObjectWriter[vectorCount];
    for (int i = 0; i < vectorCount;  i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      if (vector.getField().getType().getMinorType() == MinorType.MAP) {
        writers[i] = buildMapWriter(accessSchema.map(i), rowIndex, (AbstractMapVector) vector);
      } else {
        writers[i] = ColumnAccessorFactory.buildColumnWriter(rowIndex, vector);
      }
    }
    return new RowSetWriterImpl(this, accessSchema, rowIndex, writers);
  }

  private AbstractObjectWriter buildMapWriter(TupleSchema mapSchema, WriterIndexImpl rowIndex, AbstractMapVector mapVector) {
    Collection<MaterializedField> childSchemas = mapVector.getField().getChildren();
    int vectorCount = childSchemas.size();
    assert vectorCount == mapVector.size();
    AbstractObjectWriter[] writers = new AbstractObjectWriter[vectorCount];
    for (int i = 0; i < vectorCount;  i++) {
      @SuppressWarnings("resource")
      ValueVector vector = mapVector.getChildByOrdinal(i);
      if (vector.getField().getType().getMinorType() == MinorType.MAP) {
        writers[i] = buildMapWriter(mapSchema.map(i), rowIndex, (AbstractMapVector) vector);
      } else {
        writers[i] = ColumnAccessorFactory.buildColumnWriter(rowIndex, vector);
      }
    }
    MapWriter mapWriter = new MapWriter(mapSchema, writers);
    return new AbstractTupleWriter.TupleObjectWriter(mapWriter);
  }
  
  @Override
  public RowSetReader reader() {
    return buildReader(new DirectRowIndex(rowCount()));
  }

  @Override
  public boolean isExtendable() { return true; }

  @Override
  public boolean isWritable() { return true; }

  @Override
  public SelectionVectorMode indirectionType() { return SelectionVectorMode.NONE; }

  @Override
  public SingleRowSet toIndirect() {
    return new IndirectRowSet(this);
  }

  @Override
  public SelectionVector2 getSv2() { return null; }

  @Override
  public RowSet merge(RowSet other) {
    return new DirectRowSet(allocator, container().merge(other.container()));
  }
}
