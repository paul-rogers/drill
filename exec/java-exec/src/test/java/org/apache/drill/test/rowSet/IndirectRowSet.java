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
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.test.rowSet.AbstractRowSetAccessor.BoundedRowIndex;

public class IndirectRowSet extends AbstractSingleRowSet {

  private static class IndirectRowIndex extends BoundedRowIndex {

    private final SelectionVector2 sv2;

    public IndirectRowIndex(SelectionVector2 sv2) {
      super(sv2.getCount());
      this.sv2 = sv2;
    }

    @Override
    public int index() { return sv2.getIndex(rowIndex); }

    @Override
    public int batch() { return 0; }
  }

  private final SelectionVector2 sv2;

  public IndirectRowSet(BufferAllocator allocator, VectorContainer container) {
    super(allocator, container);
    sv2 = makeSv2(allocator, container);
  }

  private static SelectionVector2 makeSv2(BufferAllocator allocator, VectorContainer container) {
    int rowCount = container.getRecordCount();
    SelectionVector2 sv2 = new SelectionVector2(allocator);
    if (!sv2.allocateNewSafe(rowCount)) {
      throw new OutOfMemoryException("Unable to allocate sv2 buffer");
    }
    for (int i = 0; i < rowCount; i++) {
      sv2.setIndex(i, (char) i);
    }
    sv2.setRecordCount(rowCount);
    container.buildSchema(SelectionVectorMode.TWO_BYTE);
    return sv2;
  }

  public IndirectRowSet(DirectRowSet directRowSet) {
    super(directRowSet);
    sv2 = makeSv2(allocator, container);
  }

  @Override
  public SelectionVector2 getSv2() { return sv2; }

  @Override
  public void clear() {
    super.clear();
    getSv2().clear();
  }

  @Override
  public RowSetWriter writer() {
    return new RowSetWriterImpl(this, new IndirectRowIndex(getSv2()));
  }

  @Override
  public RowSetReader reader() {
    return new RowSetReaderImpl(this, new IndirectRowIndex(getSv2()));
  }

  @Override
  public boolean isExtendable() {return false;}

  @Override
  public boolean isWritable() { return true;}

  @Override
  public SelectionVectorMode getIndirectionType() { return SelectionVectorMode.TWO_BYTE; }

  @Override
  public SingleRowSet toIndirect() { return this; }
}
