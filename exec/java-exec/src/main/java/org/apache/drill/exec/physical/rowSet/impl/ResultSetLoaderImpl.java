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
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.BuildStateVisitor;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.HarvestOverflowVisitor;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.ResetVisitor;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.RollOverVisitor;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.StartBatchVisitor;
import org.apache.drill.exec.physical.rowSet.impl.LoaderVisitors.UpdateCardinalityVisitor;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.exec.physical.rowSet.model.single.WriterBuilderVisitor;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

/**
 * Implementation of the result set loader.
 * @see {@link ResultSetLoader}
 */

public class ResultSetLoaderImpl implements ResultSetLoader {

  /**
   * Read-only set of options for the result set loader.
   */

  public static class ResultSetOptions {
    public final int vectorSizeLimit;
    public final int rowCountLimit;
    public final ResultVectorCache vectorCache;
    public final Collection<String> projection;
    public final TupleMetadata schema;

    public ResultSetOptions() {
      vectorSizeLimit = ValueVector.MAX_BUFFER_SIZE;
      rowCountLimit = DEFAULT_ROW_COUNT;
      projection = null;
      vectorCache = null;
      schema = null;
    }

    public ResultSetOptions(OptionBuilder builder) {
      this.vectorSizeLimit = builder.vectorSizeLimit;
      this.rowCountLimit = builder.rowCountLimit;
      this.projection = builder.projection;
      this.vectorCache = builder.vectorCache;
      this.schema = builder.schema;
    }

    public void dump(HierarchicalFormatter format) {
      format
        .startObject(this)
        .attribute("vectorSizeLimit", vectorSizeLimit)
        .attribute("rowCountLimit", rowCountLimit)
        .attribute("projection", projection)
        .endObject();
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
    private Collection<String> projection;
    private ResultVectorCache vectorCache;
    private TupleMetadata schema;

    public OptionBuilder() {
      ResultSetOptions options = new ResultSetOptions();
      vectorSizeLimit = options.vectorSizeLimit;
      rowCountLimit = options.rowCountLimit;
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
      rowCountLimit = Math.max(1,
          Math.min(limit, ValueVector.MAX_ROW_COUNT));
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

  /**
   * The number of rows per batch. Starts with the configured amount. Can be
   * adjusted between batches, perhaps based on the actual observed size of
   * input data.
   */

  private int targetRowCount;

  public ResultSetLoaderImpl(BufferAllocator allocator, ResultSetOptions options) {
    this.allocator = allocator;
    this.options = options;
    targetRowCount = options.rowCountLimit;
    writerIndex = new WriterIndexImpl(this);

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

    // Define the writers. (May be only a root writer if no schema yet.)

    rootWriter = new RowSetWriterBuilder().buildWriter(this);

    // Create the loader state objects for each vector

    new BuildStateVisitor(this).apply(rootModel);

    // Provide the expected cardinality for the structure created
    // thus far.

    updateCardinality();
  }

  private void updateCardinality() {
    new UpdateCardinalityVisitor().apply(rootModel, targetRowCount());
  }

  public ResultSetLoaderImpl(BufferAllocator allocator) {
    this(allocator, new ResultSetOptions());
  }

  public BufferAllocator allocator() { return allocator; }

  protected int bumpVersion() {

    // Update the active schema version. We cannot update the published
    // schema version at this point because a column later in this same
    // row might cause overflow, and any new columns in this row will
    // be hidden until a later batch. But, if we are between batches,
    // then it is fine to add the column to the schema.

    activeSchemaVersion++;
    switch (state) {
    case HARVESTED:
    case START:
    case LOOK_AHEAD:
      harvestSchemaVersion = activeSchemaVersion;
      break;
    default:
      break;

    }
    return activeSchemaVersion;
  }

  @Override
  public int schemaVersion() { return harvestSchemaVersion; }

  @Override
  public void startBatch() {
    switch (state) {
    case HARVESTED:
    case START:
      updateCardinality();
      new StartBatchVisitor().apply(rootModel);

      // The previous batch ended without overflow, so start
      // a new batch, and reset the write index to 0.

      writerIndex.reset();
      rootWriter.startWrite();
      break;

    case LOOK_AHEAD:

      // A row overflowed so keep the writer index at its current value
      // as it points to the second row in the overflow batch. However,
      // the last write position of each writer must be restored on
      // a column-by-column basis, which is done by the visitor.

      new StartBatchVisitor().apply(rootModel);

      // Reset the writers to a new vector, but at a given position.

      rootWriter.startWriteAt(writerIndex.vectorIndex());
      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // Update the visible schema with any pending overflow batch
    // updates.

    harvestSchemaVersion = activeSchemaVersion;
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
      rootWriter.startRow();
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
      rootWriter.saveValue();
      rootWriter.saveRow();
      if (! writerIndex.next()) {
        state = State.FULL_BATCH;
      }

      // No overflow row. Advertise the schema version to the client.

      harvestSchemaVersion = activeSchemaVersion;
      break;

    case OVERFLOW:

      // End the value of the look-ahead row in the look-ahead vectors.

      rootWriter.saveValue();
      rootWriter.saveRow();

      // Advance the writer index relative to the look-ahead batch.

      writerIndex.next();

      // Stay in the overflow state. Doing so will cause the writer
      // to report that it is full.
      //
      // Also, do not change the harvest schema version. We will
      // expose to the downstream operators the schema in effect
      // at the start of the row. Columns added within the row won't
      // appear until the next batch.

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
    switch (state) {
    case ACTIVE:
    case FULL_BATCH:
      return writerIndex.size();
    case OVERFLOW:
      return pendingRowCount;
    default:
      return 0;
    }
  }

  protected WriterIndexImpl writerIndex() { return writerIndex; }

  @Override
  public void setTargetRowCount(int rowCount) {
    targetRowCount = Math.max(1, rowCount);
  }

  @Override
  public int targetRowCount() { return targetRowCount; }

  @Override
  public int targetVectorSize() { return options.vectorSizeLimit; }

  protected void overflowed() {

    // If we see overflow when we are already handling overflow, it means
    // that a single value is too large to fit into an entire vector.
    // Fail the query.
    //
    // Note that this is a judgment call. It is possible to allow the
    // vector to double beyond the limit, but that will require a bit
    // of thought to get right -- and, of course, completely defeats
    // the purpose of limiting vector size to avoid memory fragmentation...
    //
    // Individual columns handle the case in which overflow occurs on the
    // first row of the main batch. This check handles the pathological case
    // in which we successfully overflowed, but then another column
    // overflowed during the overflow row -- that indicates that that one
    // column can't fit in an empty vector. That is, this check is for a
    // second-order overflow.

    if (state == State.OVERFLOW) {
      throw UserException
          .memoryError("A single column value is larger than the maximum allowed size of 16 MB")
          .build(logger);
    }
    if (state != State.ACTIVE) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    pendingRowCount = writerIndex.vectorIndex();
    updateCardinality();
    new RollOverVisitor().apply(rootModel, pendingRowCount);

    // The writer index is reset back to 0. Because of the above roll-over
    // processing, some vectors may now already have values in the 0 slot.
    // However, the vector that triggered overflow has not yet written to
    // the current record, and so will now write to position 0. After the
    // completion of the row, all 0-position values should be written (or
    // at least those provided by the client.)
    //
    // For arrays, the writer might have written a set of values
    // (v1, v2, v3), and v4 might have triggered the overflow. In this case,
    // the array values have been moved, offset vectors adjusted, the
    // element writer adjusted, so that v4 will be written to index 3
    // to produce (v1, v2, v3, v4, v5, ...) in the look-ahead vector.
    //
    // Note that resetting of writers and their indexes was done bottom-up.
    // We SHOULD NOT attempt to reset them top down here, else we'll lose
    // knowledge of the roll-over array values.

    writerIndex.reset();

    // Remember that overflow is in effect.

    state = State.OVERFLOW;
  }

  protected boolean hasOverflow() { return state == State.OVERFLOW; }

  @Override
  public VectorContainer harvest() {
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

    rootWriter.endBatch();
    state = State.HARVESTED;
    harvestSchemaVersion = activeSchemaVersion;
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
    containerBuilder.update(harvestSchemaVersion);
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

  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attribute("options");
    options.dump(format);
    format
      .attribute("index", writerIndex.vectorIndex())
      .attribute("state", state)
      .attribute("activeSchemaVersion", activeSchemaVersion)
      .attribute("harvestSchemaVersion", harvestSchemaVersion)
      .attribute("pendingRowCount", pendingRowCount)
      .attribute("targetRowCount", targetRowCount)
      ;
    format.attribute("root");
    rootModel.dump(format);
  }
}
