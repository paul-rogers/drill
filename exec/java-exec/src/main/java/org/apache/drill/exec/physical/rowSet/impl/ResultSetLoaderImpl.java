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
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.*;
import org.apache.drill.exec.physical.rowSet.model.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.model.single.NullResultVectorCacheImpl;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.exec.physical.rowSet.model.single.WriterBuilderVisitor;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Implementation of the result set loader.
 * @see {@link ResultSetLoader}
 */

public class ResultSetLoaderImpl implements ResultSetLoader {

  public static final int DEFAULT_INITIAL_ROW_COUNT = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  /**
   * Read-only set of options for the result set loader.
   */

  public static class ResultSetOptions {
    public final int vectorSizeLimit;
    public final int rowCountLimit;
    public final int initialRowCount;
    public final boolean caseSensitive;
    public final ResultVectorCache vectorCache;
    public final Collection<String> projection;
    public final TupleMetadata schema;

    public ResultSetOptions() {
      vectorSizeLimit = ValueVector.MAX_BUFFER_SIZE;
      rowCountLimit = ValueVector.MAX_ROW_COUNT;
      initialRowCount = DEFAULT_INITIAL_ROW_COUNT;
      caseSensitive = false;
      projection = null;
      vectorCache = null;
      schema = null;
    }

    public ResultSetOptions(OptionBuilder builder) {
      this.vectorSizeLimit = builder.vectorSizeLimit;
      this.rowCountLimit = builder.rowCountLimit;
      this.initialRowCount = builder.initialRowCount;
      this.caseSensitive = builder.caseSensitive;
      this.projection = builder.projection;
      this.vectorCache = builder.vectorCache;
      this.schema = builder.schema;
    }
  }

  /**
   * Builder for the options for the row set loader. Reasonable defaults
   * are provided for all options; use these options for test code or
   * for clients that don't need special settings.
   */

  public static class OptionBuilder {
    private int vectorSizeLimit;
    private int rowCountLimit;
    private int initialRowCount;
    private boolean caseSensitive;
    private Collection<String> projection;
    private ResultVectorCache vectorCache;
    private TupleMetadata schema;

    public OptionBuilder() {
      ResultSetOptions options = new ResultSetOptions();
      vectorSizeLimit = options.vectorSizeLimit;
      rowCountLimit = options.rowCountLimit;
      initialRowCount = options.initialRowCount;
      caseSensitive = options.caseSensitive;
    }

    // Nice idea, but is over-achieving, will be removed
    @Deprecated
    public OptionBuilder setCaseSensitive(boolean flag) {
      caseSensitive = flag;
      return this;
    }

    /**
     * Specify the maximum number of rows per batch. Defaults to
     * {@link BaseValueVector#INITIAL_VALUE_ALLOCATION}. Batches end either
     * when this limit is reached, or when a vector overflows, whichever
     * occurs first. The limit is capped at
     * {@link ValueVector#MAX_ROW_COUNT}.
     *
     * @param limit the row count limit
     * @return this builder
     */

    public OptionBuilder setRowCountLimit(int limit) {
      rowCountLimit = Math.min(limit, ValueVector.MAX_ROW_COUNT);
      return this;
    }

    /**
     * Record (batch) readers often read a subset of available table columns,
     * but want to use a writer schema that includes all columns for ease of
     * writing. (For example, a CSV reader must read all columns, even if the user
     * wants a subset. The unwanted columns are simply discarded.)
     * <p>
     * This option provides a projection list, in the form of column names, for
     * those columns which are to be projected. Only those columns will be
     * backed by value vectors; non-projected columns will be backed by "null"
     * writers that discard all values.
     *
     * @param projection the list of projected columns
     * @return this builder
     */

    // TODO: Use SchemaPath in place of strings.

    public OptionBuilder setProjection(Collection<String> projection) {
      this.projection = projection;
      return this;
    }

    /**
     * Downstream operators require "vector persistence": the same vector
     * must represent the same column in every batch. For the scan operator,
     * which creates multiple readers, this can be a challenge. The vector
     * cache provides a transparent mechanism to enable vector persistence
     * by returning the same vector for a set of independent readers. By
     * default, the code uses a "null" cache which creates a new vector on
     * each request. If a true cache is needed, the caller must provide one
     * here.
     */

    public OptionBuilder setVectorCache(ResultVectorCache vectorCache) {
      this.vectorCache = vectorCache;
      return this;
    }

    /**
     * Clients can use the row set builder in several ways:
     * <ul>
     * <li>Provide the schema up front, when known, by using this method to
     * provide the schema.</li>
     * <li>Discover the schema on the fly, adding columns during the write
     * operation. Leave this method unset to start with an empty schema.</li>
     * <li>A combination of the above.</li>
     * </ul>
     * @param schema the initial schema for the loader
     * @return this builder
     */

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

    public RowSetLoaderImpl buildWriter(ResultSetLoaderImpl rsLoader) {
      SingleRowSetModel rootModel = rsLoader.rootModel();
      RowSetLoaderImpl writer = new RowSetLoaderImpl(rsLoader, buildTuple(rootModel));
      rootModel.bindWriter(writer);
      return writer;
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
     * Current batch was harvested: data is gone. No lookahead
     * batch exists.
     */

    HARVESTED,

    /**
     * Current batch was harvested and its data is gone. However,
     * overflow occurred during that batch and the data exists
     * in the overflow vectors.
     */

    LOOK_AHEAD,

    /**
     * Mutator is closed: no more operations are allowed.
     */

    CLOSED
  }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResultSetLoaderImpl.class);

  /**
   * Options provided to this loader.
   */

  private final ResultSetOptions options;

  /**
   * Allocator for vectors created by this loader.
   */

  final BufferAllocator allocator;

  /**
   * Internal structure used to work with the vectors (real or dummy) used
   * by this loader.
   */

  final SingleRowSetModel rootModel;

  /**
   * Top-level writer index that steps through the rows as they are written.
   * When an overflow batch is in effect, indexes into that batch instead.
   * Since a batch is really a tree of tuples, in which some branches of
   * the tree are arrays, the root indexes here feeds into array indexes
   * within the writer structure that points to the current position within
   * an array column.
   */

  private final WriterIndexImpl writerIndex;

  /**
   * The row-level writer for stepping through rows as they are written,
   * and for accessing top-level columns.
   */

  private final RowSetLoaderImpl rootWriter;

  /**
   * Vector cache for this loader.
   * @see {@link OptionBuilder#setVectorCache()}.
   */

  private final ResultVectorCache vectorCache;

  /**
   * Tracks the state of the row set loader. Handling vector overflow requires
   * careful stepping through a variety of states as the write proceeds.
   */

  private State state = State.START;

  /**
   * Track the current schema as seen by the writer. Each addition of a column
   * anywhere in the schema causes the active schema version to increase by one.
   * This allows very easy checks for schema changes: save the prior version number
   * and compare it against the current version number.
   */

  private int activeSchemaVersion;

  /**
   * Track the current schema as seen by the consumer of the batches that this
   * loader produces. The harvest schema version can be behind the active schema
   * version in the case in which new columns are added to the overflow row.
   * Since the overflow row won't be visible to the harvested batch, that batch
   * sees the schema as it existed at a prior version: the harvest schema
   * version.
   */

  private int harvestSchemaVersion;

  /**
   * Builds the harvest vector container that includes only the columns that
   * are included in the harvest schema version. That is, it excludes columns
   * added while writing the overflow row.
   */

  private VectorContainerBuilder containerBuilder;

  /**
   * Counts the batches harvested (sent downstream) from this loader. Does
   * not include the current, in-flight batch.
   */

  private int harvestBatchCount;

  /**
   * Counts the rows included in previously-harvested batches. Does not
   * include the number of rows in the current batch.
   */

  private int previousRowCount;

  /**
   * Number of rows in the harvest batch. If an overflow batch is in effect,
   * then this is the number of rows in the "main" batch before the overflow;
   * that is the number of rows in the batch that will be harvested. If no
   * overflow row is in effect, then this number is undefined (and should be
   * zero.)
   */

  private int pendingRowCount;

  public ResultSetLoaderImpl(BufferAllocator allocator, ResultSetOptions options) {
    this.allocator = allocator;
    this.options = options;
    writerIndex = new WriterIndexImpl(options.rowCountLimit);

    if (options.vectorCache == null) {
      vectorCache = new NullResultVectorCacheImpl(allocator);
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

    // Create the loader state objects for each vector

    new BuildStateVisitor().apply(rootModel, this);

//    AbstractTupleLoader root = new RootLoader(this, writerIndex);
//    if (options.projection == null) {
//      rootWriter = root;
//    } else {
//      rootWriter = new LogicalTupleLoader(this, root, options.projection);
//    }

    // Define the writers. (May be only a root writer if no schema yet.)

    rootWriter = new RowSetWriterBuilder().buildWriter(this);
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
    switch (state) {
    case ACTIVE:
      break;
    case CLOSED:
      break;
    case FULL_BATCH:
      break;
    case HARVESTED:
      break;
    case OVERFLOW:
      break;
    case START:
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }

    new StartBatchVisitor().apply(rootModel);
    rootWriter.startWrite();

    // If a row overflowed, keep the writer index at its current value
    // as it points to the second row in the overflow batch. However,
    // if the previous batch ended without overflow, then we are starting
    // a new batch, so reset the write index to 0.

    if (pendingRowCount == 0) {
      writerIndex.reset();
    }
    pendingRowCount = 0;
    state = State.ACTIVE;
  }

  @Override
  public RowSetLoader writer() {
    if (state == State.CLOSED) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    return rootWriter;
  }

  @Override
  public ResultSetLoader setRow(Object... values) {
    startRow();
    writer().setTuple(values);
    saveRow();
    return this;
  }

  /**
   * Called before writing a new row. Implementation of
   * {@link RowSetLoader#start()}.
   */

  protected void startRow() {
    switch (state) {
    case ACTIVE:

      // Update the visible schema with any pending overflow batch
      // updates.

      harvestSchemaVersion = activeSchemaVersion;
      rootWriter.startValue();
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  /**
   * Finalize the current row. Implementation of
   * {@link RowSetLoader#save()}.
   */

  protected void saveRow() {
    switch (state) {
    case ACTIVE:
      rootWriter.endValue();
      if (! writerIndex.next()) {
        state = State.FULL_BATCH;
      }
      break;
    case OVERFLOW:
      rootWriter.endValue();
      writerIndex.next();
      state = State.FULL_BATCH;
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  /**
   * Implementation of {@link RowSetLoader#isFull()}
   * @return true if the batch is full (reached vector capacity or the
   * row count limit), false if more rows can be added
   */

  protected boolean isFull() {
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

  /**
   * Implementation for {#link {@link RowSetLoader#rowCount()}.
   *
   * @return the number of rows to be sent downstream for this
   * batch. Does not include the overflow row.
   */

  protected int rowCount() {
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

  protected void overflowed() {
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

    // The writer index is reset back to 0. Because of the above roll-over
    // processing, some vectors may now already have values in the 0 slot.
    // However, the vector that triggered overflow has not yet written to
    // the current record, and so will now write to position 0. After the
    // completion of the row, all 0-position values should be written (or
    // at least those provided by the client.)

    writerIndex.reset();
    state = State.OVERFLOW;
  }

  @Override
  public VectorContainer harvest() {
    if (! isBatchActive()) {
      throw new IllegalStateException("Unexpected state: " + state);
    }

    int rowCount;
    switch (state) {
    case ACTIVE:
    case FULL_BATCH:
      rowCount = harvestNormalBatch();
      break;
    case OVERFLOW:
      rowCount = harvestOverflowBatch();
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // Build the output container

    VectorContainer container = outputContainer();
    container.setRecordCount(rowCount);

    // Finalize: update counts, set state.

    harvestBatchCount++;
    previousRowCount += rowCount;
    return container;
  }

  private int harvestNormalBatch() {

    // Wrap up the vectors: final fill-in, set value count, etc.

    rootWriter.endWrite();
    state = State.HARVESTED;
    return writerIndex.size();
  }

  private int harvestOverflowBatch() {
    new HarvestOverflowVisitor().apply(rootModel);
    state = State.LOOK_AHEAD;
    return pendingRowCount;
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
  public TupleMetadata harvestSchema() {
    return containerBuilder.schema();
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
      new ResetVisitor().apply(rootModel);
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
    rootModel.close();

    // Do not close the vector cache; the caller owns that and
    // will, presumably, reuse those vectors for another writer.

    state = State.CLOSED;
  }

  @Override
  public int batchCount() {
    return harvestBatchCount + (rowCount() == 0 ? 0 : 1);
  }

  @Override
  public int totalRowCount() {
    int total = previousRowCount;
    if (isBatchActive()) {
      total += pendingRowCount + writerIndex.size();
    }
    return total;
  }

  public ResultVectorCache vectorCache() { return vectorCache; }

  protected SingleRowSetModel rootModel() { return rootModel; }
}
