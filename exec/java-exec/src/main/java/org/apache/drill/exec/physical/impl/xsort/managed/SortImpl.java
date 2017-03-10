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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperExecContext;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.physical.impl.xsort.MSortTemplate;
import org.apache.drill.exec.physical.impl.xsort.managed.SortMemoryManager.MergeTask;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;

/**
 * Implementation of the external sort which is wrapped into the Drill
 * "next" protocol by the {@link ExternalSortBatch} class.
 * <p>
 * Accepts incoming batches. Sorts each and will spill to disk as needed.
 * When all input is delivered, can either do an in-memory merge or a
 * merge from disk. If runs spilled, may have to do one or more "consolidation"
 * passes to reduce the number of runs to the level that will fit in memory.
 */

public class SortImpl {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSortBatch.class);

  /**
   * Iterates over the final sorted results. Implemented differently
   * depending on whether the results are in-memory or spilled to
   * disk.
   */

  public interface SortResults {
    boolean next();
    void close();
    int getBatchCount();
    int getRecordCount();
    SelectionVector4 getSv4();
  }

  private final SortConfig config;

  private final SortMetrics metrics;
  private final SortMemoryManager memManager;
  private VectorAccessible outputBatch;
  private OperExecContext context;

  /**
   * Memory allocator for this operator itself. Incoming batches are
   * transferred into this allocator. Intermediate batches used during
   * merge also reside here.
   */

  private final BufferAllocator allocator;

  SpilledRuns spilledRuns;

  private BufferedBatches bufferedBatches;

  public SortImpl(OperExecContext opContext, SpilledRuns spilledRuns, VectorAccessible batch) {
    this.context = opContext;
    outputBatch = batch;
    this.spilledRuns = spilledRuns;
    allocator = opContext.getAllocator();
    config = new SortConfig(opContext.getConfig());
    memManager = new SortMemoryManager(config, allocator.getLimit());
    metrics = new SortMetrics(opContext.getStats());
    bufferedBatches = new BufferedBatches(opContext);

    // Reset the allocator to allow a 10% safety margin. This is done because
    // the memory manager will enforce the original limit. Changing the hard
    // limit will reduce the probability that random chance causes the allocator
    // to kill the query because of a small, spurious over-allocation.

    allocator.setLimit((long)(allocator.getLimit() * 1.10));
  }

  public void setSchema(BatchSchema schema) {
    bufferedBatches.setSchema(schema);
    spilledRuns.setSchema(schema);
  }

  public boolean forceSpill() {
    if (bufferedBatches.size() < 2) {
      return false;
    }
    spillFromMemory();
    return true;
  }

  /**
   * Process the converted incoming batch by adding it to the in-memory store
   * of data, or spilling data to disk when necessary.
   * @param incoming
   */

  public void processBatch(RecordBatch incoming) {

    // Skip empty batches (such as the first one.)

    if (incoming.getRecordCount() == 0) {
      return;
    }

    // Determine actual sizes of the incoming batch before taking
    // ownership. Allows us to figure out if we need to spill first,
    // to avoid overflowing memory simply due to ownership transfer.

    RecordBatchSizer sizer = analyzeIncomingBatch(incoming);

    // The heart of the external sort operator: spill to disk when
    // the in-memory generation exceeds the allowed memory limit.
    // Preemptively spill BEFORE accepting the new batch into our memory
    // pool. The allocator will throw an OOM exception if we accept the
    // batch when we are near the limit - despite the fact that the batch
    // is already in memory and no new memory is allocated during the transfer.

    if ( isSpillNeeded(sizer.actualSize())) {
      spillFromMemory();
    }

    // Sanity check. We should now be below the buffer memory maximum.

    long startMem = allocator.getAllocatedMemory();
    bufferedBatches.add(incoming, sizer.netSize());

    // Compute batch size, including allocation of an sv2.

    long endMem = allocator.getAllocatedMemory();
    long batchSize = endMem - startMem;

    // Update the minimum buffer space metric.

    metrics.updateInputMetrics(sizer.rowCount(), sizer.actualSize());
    metrics.updateMemory(memManager.freeMemory(endMem));
    metrics.updatePeakBatches(bufferedBatches.size());

    // Update the size based on the actual record count, not
    // the effective count as given by the selection vector
    // (which may exclude some records due to filtering.)

    validateBatchSize(sizer.actualSize(), batchSize);
    memManager.updateEstimates((int) batchSize, sizer.netRowWidth(), sizer.rowCount());
  }

  /**
   * Scan the vectors in the incoming batch to determine batch size.
   *
   * @return an analysis of the incoming batch
   */

  private RecordBatchSizer analyzeIncomingBatch(RecordBatch incoming) {
    RecordBatchSizer sizer = new RecordBatchSizer(incoming);
    sizer.applySv2();
    if (metrics.getInputBatchCount() == 0) {
      logger.debug("{}", sizer.toString());
    }
    return sizer;
  }

  /**
   * Determine if spill is needed before receiving the new record batch.
   * Spilling is driven purely by memory availability (and an optional
   * batch limit for testing.)
   *
   * @return true if spilling is needed, false otherwise
   */

  private boolean isSpillNeeded(int incomingSize) {

    // Can't spill if less than two batches else the merge
    // can't make progress.

    if (bufferedBatches.size() < 2) {
      return false; }

    return memManager.isSpillNeeded(allocator.getAllocatedMemory(), incomingSize);
  }

  private void validateBatchSize(long actualBatchSize, long memoryDelta) {
    if (actualBatchSize != memoryDelta) {
      ExternalSortBatch.logger.debug("Memory delta: {}, actual batch size: {}, Diff: {}",
                   memoryDelta, actualBatchSize, memoryDelta - actualBatchSize);
    }
  }

  /**
   * This operator has accumulated a set of sorted incoming record batches.
   * We wish to spill some of them to disk. To do this, a "copier"
   * merges the target batches to produce a stream of new (merged) batches
   * which are then written to disk.
   * <p>
   * This method spills only half the accumulated batches
   * minimizing unnecessary disk writes. The exact count must lie between
   * the minimum and maximum spill counts.
   */

  private void spillFromMemory() {
    int startCount = bufferedBatches.size();
    List<BatchGroup> batchesToSpill = bufferedBatches.prepareSpill(config.spillFileSize());

    // Do the actual spill.

    logger.trace("Spilling {} of {} batches, memory = {}",
        batchesToSpill.size(), startCount,
        allocator.getAllocatedMemory());
    int spillBatchRowCount = memManager.getSpillBatchRowCount();
    spilledRuns.mergeAndSpill(batchesToSpill, spillBatchRowCount);
    metrics.incrSpillCount();
  }

  public SortMetrics getMetrics() { return metrics; }

  public SortResults startMerge(VectorContainer container) {
    if (metrics.getInputRowCount() == 0) {
      return null;
    }

    logger.debug("Completed load phase: read {} batches, spilled {} times, total input bytes: {}",
        metrics.getInputBatchCount(), spilledRuns.size(),
        metrics.getInputBytes());

    // Do the merge of the loaded batches. The merge can be done entirely in memory if
    // the results fit; else we have to do a disk-based merge of
    // pre-sorted spilled batches.

    if (canUseMemoryMerge()) {
      return mergeInMemory(container);
    } else {
      return mergeSpilledRuns(container);
    }
  }

  /**
   * All data has been read from the upstream batch. Determine if we
   * can use a fast in-memory sort, or must use a merge (which typically,
   * but not always, involves spilled batches.)
   *
   * @return whether sufficient resources exist to do an in-memory sort
   * if all batches are still in memory
   */

  private boolean canUseMemoryMerge() {
    if (spilledRuns.hasSpilled()) {
      return false; }

    // Do we have enough memory for MSorter (the in-memory sorter)?

    if (! memManager.hasMemoryMergeCapacity(allocator.getAllocatedMemory(), MSortTemplate.memoryNeeded(metrics.getInputRowCount()))) {
      return false; }

    // Make sure we don't exceed the maximum number of batches SV4 can address.

    if (bufferedBatches.size() > Character.MAX_VALUE) {
      return false; }

    // We can do an in-memory merge.

    return true;
  }

  /**
   * Perform an in-memory sort of the buffered batches. Obviously can
   * be used only for the non-spilling case.
   *
   * @return DONE if no rows, OK_NEW_SCHEMA if at least one row
   */

  private SortResults mergeInMemory(VectorContainer container) {
    logger.debug("Starting in-memory sort. Batches = {}, Records = {}, Memory = {}",
                 bufferedBatches.size(), metrics.getInputRowCount(),
                 allocator.getAllocatedMemory());

    // Note the difference between how we handle batches here and in the spill/merge
    // case. In the spill/merge case, this class decides on the batch size to send
    // downstream. However, in the in-memory case, we must pass along all batches
    // in a single SV4. Attempts to do paging will result in errors. In the memory
    // merge case, the downstream Selection Vector Remover will split the one
    // big SV4 into multiple smaller batches to send further downstream.

    // If the sort fails or is empty, clean up here. Otherwise, cleanup is done
    // by closing the resultsIterator after all results are returned downstream.

    MergeSortWrapper memoryMerge = new MergeSortWrapper(context);
    try {
      memoryMerge.merge(bufferedBatches.removeAll(), outputBatch, container);
    } catch (Throwable t) {
      memoryMerge.close();
      throw t;
    }
    if (! context.shouldContinue()) {
      memoryMerge.close();
      return null;
    }
    logger.debug("Completed in-memory sort. Memory = {}",
                 allocator.getAllocatedMemory());
    return memoryMerge;
  }

  /**
   * Perform merging of (typically spilled) batches. First consolidates batches
   * as needed, then performs a final merge that is read one batch at a time
   * to deliver batches to the downstream operator.
   *
   * @return an iterator over the merged batches
   */

  private SortResults mergeSpilledRuns(VectorContainer container) {
    logger.debug("Starting consolidate phase. Batches = {}, Records = {}, Memory = {}, In-memory batches {}, spilled runs {}",
                 metrics.getInputBatchCount(), metrics.getInputRowCount(),
                 allocator.getAllocatedMemory(),
                 bufferedBatches.size(), spilledRuns.size());

    // Consolidate batches to a number that can be merged in
    // a single last pass.

    while (consolidateBatches()) {
      ;
    }

    int mergeRowCount = memManager.getMergeBatchRowCount();
    return spilledRuns.finalMerge(bufferedBatches.removeAll(), container, mergeRowCount);
  }

  private boolean consolidateBatches() {

    MergeTask task = memManager.consolidateBatches(
                      allocator.getAllocatedMemory(),
                      bufferedBatches.size(),
                      spilledRuns.size());
    switch (task.action) {
    case SPILL:
      spillFromMemory();
      return true;
    case MERGE:
      mergeRuns(task.count);
      return true;
    case NONE:
      return false;
    default:
      throw new IllegalStateException("Unexpected action: " + task.action);
    }
  }

  private void mergeRuns(int targetCount) {
    long mergeMemoryPool = memManager.getMergeMemoryLimit();
    int spillBatchRowCount = memManager.getSpillBatchRowCount();
    spilledRuns.mergeRuns(targetCount, mergeMemoryPool, spillBatchRowCount);
    metrics.incrMergeCount();
  }

  public void close() {
    metrics.updateWriteBytes(spilledRuns.getWriteBytes());
    RuntimeException ex = null;
    try {
      spilledRuns.close();
    } catch (RuntimeException e) {
      ex = e;
    }
    try {
      bufferedBatches.close();
    } catch (RuntimeException e) {
      ex = e;
    }
    if (ex != null) {
      throw ex;
    }
  }
}
