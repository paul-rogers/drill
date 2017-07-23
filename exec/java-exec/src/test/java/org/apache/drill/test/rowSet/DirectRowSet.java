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
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
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

  private static class DirectRowIndex extends RowSetReaderIndex {

    public DirectRowIndex(int rowCount) {
      super(rowCount);
    }

    @Override
    public int vectorIndex() { return rowIndex; }

    @Override
    public int batchIndex() { return 0; }
  }

  public DirectRowSet(BufferAllocator allocator, BatchSchema schema) {
    super(allocator, TupleMetadata.fromFields(schema));
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
    rowStorage.allocate(allocator, recordCount);
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
    WriterIndexImpl index = new WriterIndexImpl();
    RowStorage writeStorage = flatSchema ? rowStorage.flatten() : rowStorage;
    return new RowSetWriterImpl(this, writeStorage.tupleSchema(), index, writeStorage.writers(index));
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
