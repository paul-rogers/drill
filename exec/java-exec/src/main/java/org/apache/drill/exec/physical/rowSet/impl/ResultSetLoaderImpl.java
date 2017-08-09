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

import java.util.Collection;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.scan.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.BaseTupleLoader.RootLoader;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.BuildContainerVisitor;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.RollOverVisitor;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.exec.physical.rowSet.model.single.WriterBuilderVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSetWriterImpl;

/**
 * Implementation of the result set loader.
 * @see {@link ResultSetLoader}
 */

public class ResultSetLoaderImpl implements ResultSetLoader {

  public static final int DEFAULT_INITIAL_ROW_COUNT = 4096;

  public static class ResultSetOptions {
    private final int vectorSizeLimit;
    private final int rowCountLimit;
    private final int initialRowCount;
    private final boolean caseSensitive;
    private final ResultVectorCache vectorCache;
    private final Collection<String> projection;
    private final TupleMetadata schema;

    public ResultSetOptions() {
      vectorSizeLimit = ValueVector.MAX_BUFFER_SIZE;
      rowCountLimit = ValueVector.MAX_ROW_COUNT;
      initialRowCount = DEFAULT_INITIAL_ROW_COUNT;
      caseSensitive = false;
      projection = null;
      vectorCache = null;
    }

    public ResultSetOptions(OptionBuilder builder) {
      this.vectorSizeLimit = builder.vectorSizeLimit;
      this.rowCountLimit = builder.rowCountLimit;
      this.initialRowCount = builder.initialRowCount;
      this.caseSensitive = builder.caseSensitive;
      this.projection = builder.projection;
      this.vectorCache = builder.inventory;
    }
  }

  public static class OptionBuilder {
    private int vectorSizeLimit;
    private int rowCountLimit;
    private int initialRowCount;
    private boolean caseSensitive;
    private Collection<String> projection;
    private ResultVectorCache inventory;
    private TupleMetadata schema;

    public OptionBuilder() {
      ResultSetOptions options = new ResultSetOptions();
      vectorSizeLimit = options.vectorSizeLimit;
      rowCountLimit = options.rowCountLimit;
      initialRowCount = options.initialRowCount;
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

    public OptionBuilder setProjection(Collection<String> projection) {
      this.projection = projection;
      return this;
    }

    public OptionBuilder setVectorCache(ResultVectorCache inventory) {
      this.inventory = inventory;
      return this;
    }

    public OptionBuilder setSchema(TupleMetadata schema) {
      this.schema = schema;
      return this;
    }

    // TODO: No setter for vector length yet: is hard-coded
    // at present in the value vector.

    public ResultSetOptions build() {
      return new ResultSetOptions(this);
    }
  }

  public static class RowSetWriterBuilder extends WriterBuilderVisitor {

    public RowSetLoaderImpl buildWriter(SingleRowSetModel rowModel, WriterIndexImpl index) {
      RowSetLoaderImpl writer = new RowSetLoaderImpl(rowModel, index, buildTuple(rowModel));
      rowModel.bindWriter(writer);
      return writer;
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

    public void update() {
      if (lastUpdateVersion < rowSetMutator.schemaVersion()) {
        new BuildContainerVisitor().apply(rowSetMutator.rootModel, this);
        container.buildSchema(SelectionVectorMode.NONE);
        lastUpdateVersion = rowSetMutator.schemaVersion();
      }
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
     * when saving a row.
     * No more writes allowed until harvesting the current batch.
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
  private final BufferAllocator allocator;
  private final SingleRowSetModel rootModel;
  private final WriterIndexImpl writerIndex;
  private final RowSetLoaderImpl rootWriter;
  private final ResultVectorCache vectorCache;
  private State state = State.START;
  private int activeSchemaVersion = 0;
  private int harvestSchemaVersion = 0;
  private VectorContainerBuilder containerBuilder;
  private int previousBatchCount;
  private int previousRowCount;
  private int pendingRowCount;

  public ResultSetLoaderImpl(BufferAllocator allocator, ResultSetOptions options) {
    this.allocator = allocator;
    this.options = options;
    writerIndex = new WriterIndexImpl(options.rowCountLimit);

    if (options.vectorCache == null) {
      vectorCache = new ResultVectorCache(allocator);
    } else {
      vectorCache = options.vectorCache;
    }

    // Build the row set model depending on whether a schema is provided.

    if (options.schema == null) {

      // No schema. Columns will be added incrementally as they
      // are discovered. Start with an empty model.

      rootModel = new SingleRowSetModel(allocator);
    } else {

      // Schema provided. Populate a model (and create vectors) for the
      // provided schema. The schema can be extended later, but normally
      // won't be if known up front.

      rootModel = SingleRowSetModel.fromSchema(allocator, options.schema);
    }
//    AbstractTupleLoader root = new RootLoader(this, writerIndex);
//    if (options.projection == null) {
//      rootWriter = root;
//    } else {
//      rootWriter = new LogicalTupleLoader(this, root, options.projection);
//    }

    // Define the writers. (May be only a root writer if no schema yet.)

    rootWriter = new RowSetWriterBuilder().buildWriter(rootModel, writerIndex);
  }

  public ResultSetLoaderImpl(BufferAllocator allocator) {
    this(allocator, new ResultSetOptions());
  }

  public String toKey(String colName) {
    return options.caseSensitive ? colName : colName.toLowerCase();
  }

  public BufferAllocator allocator() { return allocator; }

  protected int bumpVersion() {
    activeSchemaVersion++;
    if (state != State.OVERFLOW) {
      // If overflow row, don't advertise the version to the client
      // as the overflow schema is invisible to the client at this
      // point.

      harvestSchemaVersion = activeSchemaVersion;
    }
    return activeSchemaVersion;
  }

  @Override
  public int schemaVersion() { return harvestSchemaVersion; }

  @Override
  public void startBatch() {
    if (state != State.START && state != State.HARVESTED) {
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // Update the visible schema with any pending overflow batch
    // updates.

    harvestSchemaVersion = activeSchemaVersion;
    rootModel.writer().startWrite();
    if (pendingRowCount == 0) {
      writerIndex.reset();
    }
    pendingRowCount = 0;
    state = State.ACTIVE;
  }

  @Override
  public TupleWriter writer() {
    if (state == State.CLOSED) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    return rootModel.writer();
  }

  @Override
  public ResultSetLoader setRow(Object... values) {
    startRow();
    writer().setTuple(values);
    saveRow();
    return this;
  }

  @Override
  public void startRow() {
    switch (state) {
    case ACTIVE:
      rootModel.writer().startValue();
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  @Override
  public void saveRow() {
    switch (state) {
    case ACTIVE:
      rootModel.writer().endValue();
      if (! writerIndex.next()) {
        state = State.FULL_BATCH;
      }
      break;
    case OVERFLOW:
      rootModel.writer().endValue();
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
    return state == State.ACTIVE || state == State.OVERFLOW ||
           state == State.FULL_BATCH ;
  }

  @Override
  public int rowCount() {
    if (! isBatchActive()) {
      return 0;
    } else if (pendingRowCount > 0) {
      return pendingRowCount;
    } else {
      return writerIndex.size();
    }
  }

  protected WriterIndexImpl writerIndex() { return writerIndex; }

  @Override
  public int targetRowCount() { return options.rowCountLimit; }

  public int initialRowCount() { return options.initialRowCount; }

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
    new RollOverVisitor().apply(rootModel, writerIndex.vectorIndex());
    writerIndex.reset();
    harvestSchemaVersion = activeSchemaVersion;
    state = State.OVERFLOW;
  }

  @Override
  public VectorContainer harvest() {
    if (! isBatchActive()) {
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // Wrap up the vectors: final fill-in, set value count, etc.

    rootModel.writer().endWrite();
    VectorContainer container = outputContainer();

    // Row count is the number of items to be harvested. If overflow,
    // it is the number of rows in the saved vectors. Otherwise,
    // it is the number in the active vectors.

    int rowCount = pendingRowCount > 0 ? pendingRowCount : writerIndex.size();
    container.setRecordCount(rowCount);

    // Finalize: update counts, set state.

    previousBatchCount++;
    previousRowCount += rowCount;
    state = State.HARVESTED;
    return container;
  }

  @Override
  public VectorContainer outputContainer() {
    // Build the output container.

    if (containerBuilder == null) {
      containerBuilder = new VectorContainerBuilder(this);
    }
    containerBuilder.update();
    return containerBuilder.container();
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
      rootModel.writer().reset();
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
    rootModel.writer().close();
    state = State.CLOSED;
  }

  @Override
  public int batchCount() {
    return previousBatchCount + (rowCount() == 0 ? 0 : 1);
  }

  @Override
  public int totalRowCount() {
    int total = previousRowCount;
    if (isBatchActive()) {
      total += pendingRowCount + writerIndex.size();
    }
    return total;
  }

  public ResultVectorCache vectorInventory() { return vectorCache; }
}
