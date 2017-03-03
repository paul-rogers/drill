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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;

public class SortMemoryManager {

  /**
   * Maximum memory this operator may use. Usually comes from the
   * operator definition, but may be overridden by a configuration
   * parameter for unit testing.
   */

  private long memoryLimit;

  /**
   * Estimated size of the records for this query, updated on each
   * new batch received from upstream.
   */

  private int estimatedRowWidth;

  /**
   * Size of the merge batches that this operator produces. Generally
   * the same as the merge batch size, unless low memory forces a smaller
   * value.
   */

  private long targetMergeBatchSize;

  /**
   * Estimate of the input batch size based on the largest batch seen
   * thus far.
   */
  private int estimatedInputBatchSize;

  /**
   * Maximum memory level before spilling occurs. That is, we can buffer input
   * batches in memory until we reach the level given by the buffer memory pool.
   */

  private long bufferMemoryPool;

  /**
   * Maximum memory that can hold batches during the merge
   * phase.
   */

  private long mergeMemoryPool;

  /**
   * The target size for merge batches sent downstream.
   */

  private long preferredMergeBatchSize;

  private BufferAllocator allocator;

  /**
   * The configured size for each spill batch.
   */
  private Long preferredSpillBatchSize;

  /**
   * Estimated number of rows that fit into a single spill batch.
   */

  private int spillBatchRowCount;

  /**
   * The estimated actual spill batch size which depends on the
   * details of the data rows for any particular query.
   */

  private int targetSpillBatchSize;

  /**
   * The number of records to add to each output batch sent to the
   * downstream operator or spilled to disk.
   */

  private int mergeBatchRowCount;

  private SortConfig config;

  private long spillPoint;

  private long minMergeMemory;

  private int estimatedInputSize;

  public SortMemoryManager(BufferAllocator allocator, SortConfig config) {
    this.allocator = allocator;
    this.config = config;

    setMemoryLimit();
    setSpillBatchSize();
    setMergeBatchSize();
    logConfig();
  }

  private void setMemoryLimit() {

    // The maximum memory this operator can use as set by the
    // operator definition (propagated to the allocator.)

    memoryLimit = allocator.getLimit();

    if (config.maxMemory() > 0) {
      memoryLimit = Math.min(memoryLimit, config.maxMemory());
    }
  }

  private void setSpillBatchSize() {

    // The spill batch size. This is a critical setting for performance.
    // Set too large and the ratio between memory and input data sizes becomes
    // small. Set too small and disk seek times dominate performance.

    preferredSpillBatchSize = config.spillFileSize();

    // In low memory, use no more than 1/4 of memory for each spill batch. Ensures we
    // can merge.

    preferredSpillBatchSize = Math.min(preferredSpillBatchSize, memoryLimit / 4);

    // But, the spill batch should be above some minimum size to prevent complete
    // thrashing.

    preferredSpillBatchSize = Math.max(preferredSpillBatchSize, SortConfig.MIN_SPILL_BATCH_SIZE);
  }

  private void setMergeBatchSize() {

    // Set the target output batch size. Use the maximum size, but only if
    // this represents less than 10% of available memory. Otherwise, use 10%
    // of memory, but no smaller than the minimum size. In any event, an
    // output batch can contain no fewer than a single record.

    preferredMergeBatchSize = config.mergeBatchSize();
    long maxAllowance = (long) (memoryLimit - 2 * preferredSpillBatchSize);
    preferredMergeBatchSize = Math.min(maxAllowance, preferredMergeBatchSize);
    preferredMergeBatchSize = Math.max(preferredMergeBatchSize, SortConfig.MIN_MERGED_BATCH_SIZE);
  }

  private void logConfig() {

    ExternalSortBatch.logger.debug("Config: memory limit = {}, " +
                 "spill file size = {}, spill batch size = {}, merge limit = {}, merge batch size = {}",
                  memoryLimit, config.spillFileSize(), preferredSpillBatchSize,
                  config.mergeLimit(), preferredMergeBatchSize);
  }

  /**
   * Update the data-driven memory use numbers including:
   * <ul>
   * <li>The average size of incoming records.</li>
   * <li>The estimated spill and output batch size.</li>
   * <li>The estimated number of average-size records per
   * spill and output batch.</li>
   * <li>The amount of memory set aside to hold the incoming
   * batches before spilling starts.</li>
   * </ul>
   *
   * @param actualBatchSize the overall size of the current batch received from
   * upstream
   * @param actualRecordCount the number of actual (not filtered) records in
   * that upstream batch
   */

  void updateMemoryEstimates(long memoryDelta, RecordBatchSizer sizer) {

    // The record count should never be zero, but better safe than sorry...

    if (sizer.rowCount() == 0) {
      return; }

    validateBatchSize(sizer.actualSize(), memoryDelta);

    // Update input batch estimates.
    // Go no further if nothing changed.

    if (! updateInputEstimates(sizer)) {
      return;
    }

    updateSpillSettings();
    updateMergeSettings();
    adjustForLowMemory();
    logSettings(sizer.rowCount());
  }

  private void validateBatchSize(long actualBatchSize, long memoryDelta) {
    if (actualBatchSize != memoryDelta) {
      ExternalSortBatch.logger.debug("Memory delta: {}, actual batch size: {}, Diff: {}",
                   memoryDelta, actualBatchSize, memoryDelta - actualBatchSize);
    }
  }

  private boolean updateInputEstimates(RecordBatchSizer sizer) {

    // We know the batch size and number of records. Use that to estimate
    // the average record size. Since a typical batch has many records,
    // the average size is a fairly good estimator. Note that the batch
    // size includes not just the actual vector data, but any unused space
    // resulting from power-of-two allocation. This means that we don't
    // have to do size adjustments for input batches as we will do below
    // when estimating the size of other objects.

    int batchRowWidth = sizer.netRowWidth();

    // Record sizes may vary across batches. To be conservative, use
    // the largest size observed from incoming batches.

    int origRowEstimate = estimatedRowWidth;
    estimatedRowWidth = Math.max(estimatedRowWidth, batchRowWidth);

    // Maintain an estimate of the incoming batch size: the largest
    // batch yet seen. Used to reserve memory for the next incoming
    // batch. Because we are using the actual observed batch size,
    // the size already includes overhead due to power-of-two rounding.

    long origInputBatchSize = estimatedInputBatchSize;
    estimatedInputBatchSize = Math.max(estimatedInputBatchSize, sizer.actualSize());

    // The row width may end up as zero if all fields are nulls or some
    // other unusual situation. In this case, assume a width of 10 just
    // to avoid lots of special case code.

    if (estimatedRowWidth == 0) {
      estimatedRowWidth = 10;
    }

    // Estimate the total size of each incoming batch plus sv2. Note that, due
    // to power-of-two rounding, the allocated sv2 size might be twice the data size.

    estimatedInputSize = estimatedInputBatchSize + 4 * sizer.rowCount();

    // Return whether anything changed.

    return estimatedRowWidth != origRowEstimate || estimatedInputBatchSize != origInputBatchSize;
  }

  private void updateSpillSettings() {

    // Determine the number of records to spill per spill batch. The goal is to
    // spill batches of either 64K records, or as many records as fit into the
    // amount of memory dedicated to each spill batch, whichever is less.

    spillBatchRowCount = (int) Math.max(1, preferredSpillBatchSize / estimatedRowWidth / 2);
    spillBatchRowCount = Math.min(spillBatchRowCount, Character.MAX_VALUE);

    // Compute the actual spill batch size which may be larger or smaller
    // than the preferred size depending on the row width. Double the estimated
    // memory needs to allow for power-of-two rounding.

    targetSpillBatchSize = spillBatchRowCount * estimatedRowWidth * 2;

    // Determine the minimum memory needed for spilling. Spilling is done just
    // before accepting a batch, so we must spill if we don't have room for a
    // (worst case) input batch. To spill, we need room for the output batch created
    // by merging the batches already in memory. Double this to allow for power-of-two
    // memory allocations.

    spillPoint = estimatedInputBatchSize + 2 * targetSpillBatchSize;

    // Determine how much memory can be used to hold in-memory batches.

    bufferMemoryPool = memoryLimit - spillPoint;
  }

  private void updateMergeSettings() {

    // Determine the number of records per batch per merge step. The goal is to
    // merge batches of either 64K records, or as many records as fit into the
    // amount of memory dedicated to each merge batch, whichever is less.

    mergeBatchRowCount = (int) Math.max(1, preferredMergeBatchSize / estimatedRowWidth / 2);
    mergeBatchRowCount = Math.min(mergeBatchRowCount, Character.MAX_VALUE);
    mergeBatchRowCount = Math.max(1,  mergeBatchRowCount);
    targetMergeBatchSize = mergeBatchRowCount * estimatedRowWidth * 2;

    // The merge memory pool assumes we can spill all input batches. To make
    // progress, we must have at least two merge batches (same size as an output
    // batch) and one output batch. Again, double to allow for power-of-two
    // allocation and add one for a margin of error.

    minMergeMemory = 2 * targetSpillBatchSize + targetMergeBatchSize;

    // Determine how much memory can be used to hold spilled
    // runs when reading from disk.

    mergeMemoryPool = Math.max(memoryLimit - minMergeMemory,
                               (long) ((memoryLimit - 3 * targetMergeBatchSize) * 0.95));
  }

  private void adjustForLowMemory() {
    // If we are in a low-memory condition, then we might not have room for the
    // default output batch size. In that case, pick a smaller size.

    if (minMergeMemory > memoryLimit) {

      // Figure out the minimum output batch size based on memory,
      // must hold at least one complete row.

      long mergeAllowance = memoryLimit - 2 * targetSpillBatchSize;
      targetMergeBatchSize = Math.max(estimatedRowWidth, mergeAllowance / 2);
      mergeBatchRowCount = (int) (targetMergeBatchSize / estimatedRowWidth / 2);
      minMergeMemory = 2 * targetSpillBatchSize + targetMergeBatchSize;
    }

    // Determine the minimum total memory we would need to receive two input
    // batches (the minimum needed to make progress) and the allowance for the
    // output batch.

    long minLoadMemory = spillPoint + estimatedInputSize;

    // Sanity check: if we've been given too little memory to make progress,
    // issue a warning but proceed anyway. Should only occur if something is
    // configured terribly wrong.

    long minMemoryNeeds = Math.max(minLoadMemory, minMergeMemory);
    if (minMemoryNeeds > memoryLimit) {
      ExternalSortBatch.logger.warn("Potential memory overflow! " +
                   "Minumum needed = {} bytes, actual available = {} bytes",
                   minMemoryNeeds, memoryLimit);
    }
  }

  private void logSettings(int actualRecordCount) {

    // Log the calculated values. Turn this on if things seem amiss.
    // Message will appear only when the values change.

    ExternalSortBatch.logger.debug("Input Batch Estimates: record size = {} bytes; input batch = {} bytes, {} records",
                 estimatedRowWidth, estimatedInputBatchSize, actualRecordCount);
    ExternalSortBatch.logger.debug("Merge batch size = {} bytes, {} records; spill file size: {} bytes",
                 targetSpillBatchSize, spillBatchRowCount, config.spillFileSize());
    ExternalSortBatch.logger.debug("Output batch size = {} bytes, {} records",
                 targetMergeBatchSize, mergeBatchRowCount);
    ExternalSortBatch.logger.debug("Available memory: {}, buffer memory = {}, merge memory = {}",
                 memoryLimit, bufferMemoryPool, mergeMemoryPool);
  }

  // Must spill if we are below the spill point (the amount of memory
  // needed to do the minimal spill.)

  public boolean isSpillNeeded(int incomingSize) {
    return allocator.getAllocatedMemory() + incomingSize >= bufferMemoryPool;
  }

  public boolean hasMemoryMergeCapacity(long neededForInMemorySort) {
    long allocMem = allocator.getAllocatedMemory();
    long availableMem = memoryLimit - allocMem;
    return (availableMem >= neededForInMemorySort);
  }

  public int getMaxMergeWidth() {
    int maxMergeWidth = (int) (mergeMemoryPool / targetSpillBatchSize);
    return Math.min(config.mergeLimit(), maxMergeWidth);
  }

  public boolean hasMergeCapacity(long mergeRunsCount) {
    long allocated = allocator.getAllocatedMemory();
    long mergeSize = mergeRunsCount * targetSpillBatchSize;
    long totalNeeds = mergeSize + allocated;
    return (totalNeeds < mergeMemoryPool);
  }

  public long getMergeMemoryPool( ) {
    return mergeMemoryPool;
  }

  public int getSpillBatchRowCount( ) {
    return spillBatchRowCount;
  }

  public int getMergeBatchRowCount() {
    return mergeBatchRowCount;
  }
}