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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.RowSetMutator;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.TupleSetImpl.ColumnImpl;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;

public class RowSetMutatorImpl implements RowSetMutator, WriterIndexImpl.OverflowListener {

  public static class MutatorOptions {
    public final int vectorSizeLimit;
    public final int rowCountLimit;
    public final boolean caseSensitive;

    public MutatorOptions() {
      vectorSizeLimit = ValueVector.MAX_BUFFER_SIZE;
      rowCountLimit = ValueVector.MAX_ROW_COUNT;
      caseSensitive = false;
    }
  }

  public static class VectorContainerBuilder {
    private final RowSetMutatorImpl rowSetMutator;
    private int lastUpdateVersion = -1;
    private VectorContainer container;

    public VectorContainerBuilder(RowSetMutatorImpl rowSetMutator) {
      this.rowSetMutator = rowSetMutator;
      container = new VectorContainer(rowSetMutator.allocator);
    }

    public void update() {
      if (lastUpdateVersion < rowSetMutator.schemaVersion()) {
        scanTuple(rowSetMutator.rootTuple);
        container.buildSchema(SelectionVectorMode.NONE);
        lastUpdateVersion = rowSetMutator.schemaVersion();
      }
      container.setRecordCount(rowSetMutator.rowCount());
    }

    private void scanTuple(TupleSetImpl tupleSet) {
      for (int i = 0; i < tupleSet.columnCount(); i++) {
        ColumnImpl colImpl = tupleSet.columnImpl(i);
        if (colImpl.addVersion <= lastUpdateVersion) {
          continue;
        }
        container.add(colImpl.vector);
      }
    }

    public VectorContainer container() { return container; }
  }

  private enum State { START, ACTIVE, OVERFLOW, HARVESTED, LOOK_AHEAD, CLOSED }

  private final MutatorOptions options;
  private RowSetMutatorImpl.State state = State.START;
  final BufferAllocator allocator;
  private int schemaVersion = 0;
  TupleSetImpl rootTuple;
  private final WriterIndexImpl writerIndex;
  private VectorContainerBuilder containerBuilder;
  private int previousBatchCount;
  private int previousRowCount;
  private int pendingRowCount;

  public RowSetMutatorImpl(BufferAllocator allocator, MutatorOptions options) {
    this(allocator);
  }

  public RowSetMutatorImpl(BufferAllocator allocator) {
    this.allocator = allocator;
    options = new MutatorOptions();
    writerIndex = new WriterIndexImpl(this);
    rootTuple = new TupleSetImpl(this);
  }

  public String toKey(String colName) {
    return options.caseSensitive ? colName : colName.toLowerCase();
  }

  public BufferAllocator allocator() { return allocator; }

  protected int bumpVersion() { return ++schemaVersion; }

  @Override
  public int schemaVersion() { return schemaVersion; }

  @Override
  public void start() {
    if (state != State.START && state != State.HARVESTED && state != State.LOOK_AHEAD) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    rootTuple.start();
    if (state != State.LOOK_AHEAD) {
      writerIndex.reset();
    }
    pendingRowCount = 0;
    state = State.ACTIVE;
  }

  @Override
  public TupleLoader writer() {
    if (state != State.ACTIVE) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    return rootTuple.loader();
  }

  @Override
  public void save() {
    if (state != State.ACTIVE) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    writerIndex.next();
  }

  @Override
  public boolean isFull() { return ! writerIndex.valid(); }

  @Override
  public int rowCount() {
    return state == State.ACTIVE ? writerIndex.size() + pendingRowCount : 0;
  }

  protected ColumnWriterIndex writerIndex() { return writerIndex; }

  @Override
  public int targetRowCount() { return options.rowCountLimit; }

  @Override
  public int targetVectorSize() { return options.vectorSizeLimit; }

  @Override
  public void overflowed() {
    if (state != State.ACTIVE) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    pendingRowCount = rowCount();
    rootTuple.rollOver(writerIndex.vectorIndex());
    writerIndex.reset();
    state = State.OVERFLOW;
  }

  @Override
  public VectorContainer harvest() {
    if (state != State.ACTIVE && state != State.OVERFLOW) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    rootTuple.harvest();
    if (containerBuilder == null) {
      containerBuilder = new VectorContainerBuilder(this);
    }
    containerBuilder.update();
    VectorContainer container = containerBuilder.container();
    previousBatchCount++;
    previousRowCount += container.getRecordCount();
    state = state == State.OVERFLOW ? State.LOOK_AHEAD : State.HARVESTED;
    return container;
  }

  @Override
  public void close() {
    if (state == State.CLOSED) {
      return;
    }
    rootTuple.close();
    // TODO Auto-generated method stub
    state = State.CLOSED;
  }

  @Override
  public int batchCount() {
    return previousBatchCount + (rowCount() == 0 ? 0 : 1);
  }

  @Override
  public int totalRowCount() {
    return previousRowCount + rowCount();
  }
}