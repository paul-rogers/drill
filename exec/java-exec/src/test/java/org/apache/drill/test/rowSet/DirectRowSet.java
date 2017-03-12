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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.rowSet.AbstractRowSetAccessor.AbstractRowIndex;
import org.apache.drill.test.rowSet.AbstractRowSetAccessor.BoundedRowIndex;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;

public class DirectRowSet extends AbstractSingleRowSet implements ExtendableRowSet {

  private static class DirectRowIndex extends BoundedRowIndex {

    public DirectRowIndex(int rowCount) {
      super(rowCount);
    }

    @Override
    public int index() { return rowIndex; }

    @Override
    public int batch() { return 0; }
  }

  private static class ExtendableRowIndex extends AbstractRowIndex {

    private final DirectRowSet rowSet;
    private final int maxSize;

    public ExtendableRowIndex(DirectRowSet rowSet, int maxSize) {
      this.rowSet = rowSet;
      this.maxSize = maxSize;
    }

    @Override
    public int index() { return rowIndex; }

    @Override
    public boolean next() {
      if (++rowIndex < maxSize ) {
        return true;
      } else {
        rowIndex--;
        return false;
      }
    }

    @Override
    public int size() { return rowIndex + 1; }

    @Override
    public boolean valid() { return rowIndex < maxSize; }

    @Override
    public void setRowCount() { rowSet.setRowCount(size()); }

    @Override
    public int batch() { return 0; }
  }

  public DirectRowSet(BufferAllocator allocator, RowSetSchema schema) {
    super(allocator, schema);
  }

  public DirectRowSet(BufferAllocator allocator, VectorContainer container) {
    super(allocator, container);
  }

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
  public RowSetWriter writer() {
    return new RowSetWriterImpl(this, new DirectRowIndex(rowCount()));
  }

  @Override
  public RowSetWriter writer(int initialRowCount) {
    if (container.hasRecordCount()) {
      throw new IllegalStateException("Row set already contains data");
    }
    allocate(initialRowCount);
    return new RowSetWriterImpl(this, new ExtendableRowIndex(this, Character.MAX_VALUE));
  }

  @Override
  public RowSetReader reader() {
    return new RowSetReaderImpl(this, new DirectRowIndex(rowCount()));
  }

  @Override
  public boolean isExtendable() { return true; }

  @Override
  public boolean isWritable() { return true; }

  @Override
  public SelectionVectorMode getIndirectionType() { return SelectionVectorMode.NONE; }

  @Override
  public SingleRowSet toIndirect() {
    return new IndirectRowSet(this);
  }

  @Override
  public SelectionVector2 getSv2() { return null; }
}
