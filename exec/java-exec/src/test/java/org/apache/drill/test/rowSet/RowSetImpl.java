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

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

public class RowSetImpl implements RowSet {

  private final BufferAllocator allocator;
  private SelectionVector2 sv2;
  private final RowSetSchema schema;
  private final ValueVector[] valueVectors;
  private final VectorContainer container;
  private SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  public RowSetImpl(BufferAllocator allocator, RowSetSchema schema) {
    this.allocator = allocator;
    this.schema = schema;
    valueVectors = new ValueVector[schema.count()];
    container = new VectorContainer();
    create();
  }

  public RowSetImpl(BufferAllocator allocator, VectorContainer container) {
    this.allocator = allocator;
    schema = new RowSetSchema(container.getSchema());
    this.container = container;
    valueVectors = new ValueVector[container.getNumberOfColumns()];
    int i = 0;
    for (VectorWrapper<?> w : container) {
      valueVectors[i++] = w.getValueVector();
    }
  }

  private void create() {
    for (int i = 0; i < schema.count(); i++) {
      final MaterializedField field = schema.get(i);
      @SuppressWarnings("resource")
      ValueVector v = TypeHelper.getNewVector(field, allocator, callBack);
      valueVectors[i] = v;
      container.add(v);
    }
    container.buildSchema(SelectionVectorMode.NONE);
  }

  @Override
  public VectorAccessible getVectorAccessible() { return container; }

  @Override
  public VectorContainer getContainer() { return container; }

  @Override
  public void makeSv2() {
    if (sv2 != null) {
      return;
    }
    sv2 = new SelectionVector2(allocator);
    if (!sv2.allocateNewSafe(rowCount())) {
      throw new OutOfMemoryException("Unable to allocate sv2 buffer");
    }
    for (int i = 0; i < rowCount(); i++) {
      sv2.setIndex(i, (char) i);
    }
    sv2.setRecordCount(rowCount());
    container.buildSchema(SelectionVectorMode.TWO_BYTE);
  }

  @Override
  public SelectionVector2 getSv2() { return sv2; }

  @Override
  public void allocate(int recordCount) {
    for (final ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, recordCount, 50, 10);
    }
  }

  @Override
  public void setRowCount(int rowCount) {
    container.setRecordCount(rowCount);
    for (VectorWrapper<?> w : container) {
      w.getValueVector().getMutator().setValueCount(rowCount);
    }
  }

  @Override
  public int rowCount() { return container.getRecordCount(); }

  @Override
  public RowSetWriter writer(int initialRowCount) {
    allocate(initialRowCount);
    return new RowSetWriterImpl(this);
  }

  @Override
  public RowSetReader reader() {
    return new RowSetReaderImpl(this);
  }

  @Override
  public void clear() {
    container.zeroVectors();
    if (sv2 != null) {
      sv2.clear();
    }
    container.setRecordCount(0);
    sv2 = null;
  }

  @Override
  public ValueVector[] vectors() { return valueVectors; }

  @Override
  public RowSetSchema schema() { return schema; }

  @Override
  public BufferAllocator getAllocator() { return allocator; }

  @Override
  public boolean hasSv2() { return sv2 != null; }

  @Override
  public void print() {
    new RowSetPrinter(this).print();
  }

  @Override
  public int getSize() {
    RecordBatchSizer sizer = new RecordBatchSizer(container, sv2);
    return sizer.actualSize();
  }

  @Override
  public BatchSchema getBatchSchema() {
    return container.getSchema();
  }
}
