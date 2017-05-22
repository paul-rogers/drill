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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Implementation of the result set loader.
 * @see {@link ResultSetLoader}
 */

public class ResultSetLoaderImpl implements ResultSetLoader, WriterIndexImpl.WriterIndexListener {

  public static class ResultSetOptions {
    public final int vectorSizeLimit;
    public final int rowCountLimit;
    public final boolean caseSensitive;

    public ResultSetOptions() {
      vectorSizeLimit = ValueVector.MAX_BUFFER_SIZE;
      rowCountLimit = ValueVector.MAX_ROW_COUNT;
      caseSensitive = false;
    }

    public ResultSetOptions(OptionBuilder builder) {
      this.vectorSizeLimit = builder.vectorSizeLimit;
      this.rowCountLimit = builder.rowCountLimit;
      this.caseSensitive = builder.caseSensitive;
    }
  }

  public static class OptionBuilder {
    public int vectorSizeLimit;
    public int rowCountLimit;
    public boolean caseSensitive;

    public OptionBuilder() {
      ResultSetOptions options = new ResultSetOptions();
      vectorSizeLimit = options.vectorSizeLimit;
      rowCountLimit = options.rowCountLimit;
      caseSensitive = options.caseSensitive;
    }

    public OptionBuilder setCaseSensitive(boolean flag) {
      caseSensitive = flag;
      return this;
    }

    public OptionBuilder setRowCountLimit(int limit) {
      rowCountLimit = Math.min(limit, ValueVector.MAX_ROW_COUNT);
      return this;
    }

    // TODO: No setter for vector length yet: is hard-coded
    // at present in the value vector.

    public ResultSetOptions build() {
      return new ResultSetOptions(this);
    }
  }

  public static class VectorContainerBuilder {
    private final ResultSetLoaderImpl rowSetMutator;
    private int lastUpdateVersion = -1;
    private VectorContainer container;

    public VectorContainerBuilder(ResultSetLoaderImpl rowSetMutator) {
      this.rowSetMutator = rowSetMutator;
      container = new VectorContainer(rowSetMutator.allocator);
    }

    public void update(int rowCount) {
      if (lastUpdateVersion < rowSetMutator.schemaVersion()) {
        rowSetMutator.rootTuple.buildContainer(this);
        container.buildSchema(SelectionVectorMode.NONE);
        lastUpdateVersion = rowSetMutator.schemaVersion();
      }
      container.setRecordCount(rowCount);
    }

    public VectorContainer container() { return container; }

    public int lastUpdateVersion() { return lastUpdateVersion; }

    public void add(ValueVector vector) {
      container.add(vector);
    }
  }

  private enum State {
    /**
     * Before the first batch.
     */
    START,
    /**
     * Writing to a batch normally.
     */
    ACTIVE,
    /**
     * Batch overflowed a vector while writing. Can continue
     * to write to a temporary "overflow" batch until the
     * end of the current row.
     */
    OVERFLOW,
    /**
     * Batch is full due to reaching the row count limit
     * when saving a row. Also, a vector overflowed and
     * next was called after writing the overflow row.
     * No more writes allowed until
     * harvesting the current batch.
     */
    FULL_BATCH,
    /**
     * Current batch was harvested: data is gone. A lookahead
     * row may exist for the next batch.
     */
    HARVESTED,
    /**
     * Mutator is closed: no more operations are allowed.
     */
    CLOSED
  }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResultSetLoaderImpl.class);

  private final ResultSetOptions options;
  private ResultSetLoaderImpl.State state = State.START;
  private final BufferAllocator allocator;
  private int activeSchemaVersion = 0;
  private int harvestSchemaVersion = 0;
  private TupleSetImpl rootTuple;
  private final WriterIndexImpl writerIndex;
  private VectorContainerBuilder containerBuilder;
  private int previousBatchCount;
  private int previousRowCount;
  private int pendingRowCount;

  public ResultSetLoaderImpl(BufferAllocator allocator, ResultSetOptions options) {
    this.allocator = allocator;
    this.options = options;
    writerIndex = new WriterIndexImpl(this, options.rowCountLimit);
    rootTuple = new TupleSetImpl(this);
  }

  public ResultSetLoaderImpl(BufferAllocator allocator) {
    this(allocator, new ResultSetOptions());
  }

  public String toKey(String colName) {
    return options.caseSensitive ? colName : colName.toLowerCase();
  }

  public BufferAllocator allocator() { return allocator; }

  protected int bumpVersion() { return ++activeSchemaVersion; }

  @Override
  public int schemaVersion() { return harvestSchemaVersion; }

  @Override
  public void startBatch() {
    if (state != State.START && state != State.HARVESTED) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    rootTuple.start();
    if (pendingRowCount == 0) {
      writerIndex.reset();
    }
    pendingRowCount = 0;
    state = State.ACTIVE;
  }

  @Override
  public TupleLoader writer() {
    if (state == State.CLOSED) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    return rootTuple.loader();
  }

  @Override
  public void startRow() {
    switch (state) {
    case ACTIVE:
      rootTuple.startRow();
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  @Override
  public void saveRow() {
    switch (state) {
    case ACTIVE:
      if (! writerIndex.next()) {
        state = State.FULL_BATCH;
      }
      break;
    case OVERFLOW:
      writerIndex.next();
      state = State.FULL_BATCH;
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  @Override
  public boolean isFull() {
    switch (state) {
    case ACTIVE:
      return ! writerIndex.valid();
    case OVERFLOW:
    case FULL_BATCH:
      return true;
    default:
      return false;
    }
  }

  @Override
  public boolean writeable() {
    return state == State.ACTIVE || state == State.OVERFLOW;
  }

  private boolean isBatchActive() {
    return state == State.ACTIVE || state == State.OVERFLOW || state == State.FULL_BATCH;
  }

  @Override
  public int rowCount() {
    if (isBatchActive()) {
      return writerIndex.size() + pendingRowCount;
    } else {
      return 0;
    }
  }

  protected WriterIndexImpl writerIndex() { return writerIndex; }

  @Override
  public int targetRowCount() { return options.rowCountLimit; }

  @Override
  public int targetVectorSize() { return options.vectorSizeLimit; }

  @Override
  public void overflowed() {
    if (state != State.ACTIVE) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    if (rowCount() == 0) {
      throw UserException
        .memoryError("A single column value is larger than the maximum allowed size of 16 MB")
        .build(logger);
    }
    pendingRowCount = rowCount();
    rootTuple.rollOver(writerIndex.vectorIndex());
    writerIndex.reset();
    harvestSchemaVersion = activeSchemaVersion;
    state = State.OVERFLOW;
  }

  @Override
  public VectorContainer harvest() {
    if (! isBatchActive()) {
      throw new IllegalStateException("Unexpected state: " + state);
    }

    if (state == State.ACTIVE) {
      harvestSchemaVersion = activeSchemaVersion;
    }

    // Wrap up the vectors: final fill-in, set value count, etc.

    rootTuple.harvest();

    // Row count is the number of items to be harvested. If overflow,
    // it is the number of rows in the saved vectors. Otherwise,
    // it is the number in the active vectors.

    int rowCount = pendingRowCount > 0 ? pendingRowCount : writerIndex.size();

    // Build the output container.

    if (containerBuilder == null) {
      containerBuilder = new VectorContainerBuilder(this);
    }
    containerBuilder.update(rowCount);
    VectorContainer container = containerBuilder.container();

    // Finalize: update counts, set state.

    previousBatchCount++;
    previousRowCount += rowCount;
    state = State.HARVESTED;
    return container;
  }

  @Override
  public void reset() {
    switch (state) {
    case HARVESTED:
    case START:
      break;
    case ACTIVE:
    case OVERFLOW:
    case FULL_BATCH:
      rootTuple.reset();
      state = State.HARVESTED;
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  @Override
  public void close() {
    if (state == State.CLOSED) {
      return;
    }
    rootTuple.close();
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
