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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.physical.impl.xsort.MSortTemplate;
import org.apache.drill.exec.physical.impl.xsort.SingleBatchSorter;
import org.apache.drill.exec.physical.impl.xsort.managed.BatchGroup.InputBatch;
import org.apache.drill.exec.physical.impl.xsort.managed.BatchGroup.SpilledRun;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;

import com.google.common.collect.Lists;

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
  protected static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ExternalSortBatch.class);

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

  /**
   * Incoming batches buffered in memory prior to spilling
   * or an in-memory merge.
   */

  private LinkedList<BatchGroup.InputBatch> bufferedBatches = Lists.newLinkedList();
  private LinkedList<BatchGroup.SpilledRun> spilledRuns = Lists.newLinkedList();

  /**
   * Manages the set of spill directories and files.
   */

  private final SpillSet spillSet;

  /**
   * Manages the copier used to merge a collection of batches into
   * a new set of batches.
   */

  private final CopierHolder copierHolder;

  private final OperatorCodeGenerator opCodeGen;

  private final BufferAllocator allocator;

  private SortConfig config;

  private SortMetrics metrics;

  private SortMemoryManager memManager;
  private BatchSchema schema;
  private OperatorContext oContext;
  private FragmentContext context;
  private VectorAccessible outputBatch;

  public SortImpl(ExternalSort popConfig, FragmentContext context, OperatorContext oContext, BufferAllocator allocator, OperatorStats stats, VectorAccessible batch) {
    this.allocator = allocator;
    this.oContext = oContext;
    this.context = context;
    outputBatch = batch;
    opCodeGen = new OperatorCodeGenerator(context, popConfig);
    spillSet = new SpillSet(context, popConfig, "sort", "run");
    copierHolder = new CopierHolder(context, allocator, opCodeGen);
    config = new SortConfig(context.getConfig());
    memManager = new SortMemoryManager(config, allocator.getLimit());
    metrics = new SortMetrics(stats);

    // Reset the allocator to allow a 10% safety margin. This is done because
    // the memory manager will enforce the original limit. Changing the hard
    // limit will reduce the probability that random chance causes the allocator
    // to kill the query because of a small, spurious over-allocation.

    allocator.setLimit((long)(allocator.getLimit() * 1.10));
  }

  public void setSchema(BatchSchema schema) {
    this.schema = schema;

    // New schema: must generate a new sorter and copier.

    opCodeGen.setSchema(schema);

    // Coerce all existing batches to the new schema.

    for (BatchGroup b : bufferedBatches) {
      b.setSchema(schema);
    }
    for (BatchGroup b : spilledRuns) {
      b.setSchema(schema);
    }
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

  @SuppressWarnings("resource")
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

    // Convert the incoming batch to the agreed-upon schema.
    // No converted batch means we got an empty input batch.
    // Converting the batch transfers memory ownership to our
    // allocator. This gives a round-about way to learn the batch
    // size: check the before and after memory levels, then use
    // the difference as the batch size, in bytes.

    VectorContainer convertedBatch = convertBatch(incoming);
    if (convertedBatch == null) {
      return;
    }

    SelectionVector2 sv2;
    try {
      sv2 = makeSelectionVector(incoming);
    } catch (Exception e) {
      convertedBatch.clear();
      throw e;
    }

    // Compute batch size, including allocation of an sv2.

    long endMem = allocator.getAllocatedMemory();
    long batchSize = endMem - startMem;

    // Update the minimum buffer space metric.

    metrics.updateInputMetrics(sizer.rowCount(), sizer.actualSize());
    metrics.updateMemory(memManager.freeMemory(endMem));

    // Update the size based on the actual record count, not
    // the effective count as given by the selection vector
    // (which may exclude some records due to filtering.)

    validateBatchSize(sizer.actualSize(), batchSize);
    memManager.updateEstimates((int) batchSize, sizer.netRowWidth(), sizer.rowCount());

    // Sort the incoming batch using either the original selection vector,
    // or a new one created here.

    sortIncomingBatch(convertedBatch, sv2);
    bufferBatch(convertedBatch, sv2, sizer.netSize());
  }

  /**
   * Scan the vectors in the incoming batch to determine batch size and if
   * any oversize columns exist. (Oversize columns cause memory fragmentation.)
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
   * Convert an incoming batch into the agree-upon format.
   * @param incoming
   *
   * @return the converted batch, or null if the incoming batch is empty
   */

  @SuppressWarnings("resource")
  private VectorContainer convertBatch(RecordBatch incoming) {

    // Must accept the batch even if no records. Then clear
    // the vectors to release memory since we won't do any
    // further processing with the empty batch.

    VectorContainer convertedBatch = SchemaUtil.coerceContainer(incoming, schema, oContext);
    if (incoming.getRecordCount() == 0) {
      for (VectorWrapper<?> w : convertedBatch) {
        w.clear();
      }
      SelectionVector2 sv2 = incoming.getSelectionVector2();
      if (sv2 != null) {
        sv2.clear();
      }
      return null;
    }
    return convertedBatch;
  }

  private SelectionVector2 makeSelectionVector(RecordBatch incoming) {
    if (incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      return incoming.getSelectionVector2().clone();
    } else {
      return newSV2(incoming);
    }
  }

  /**
   * Allocate and initialize the selection vector used as the sort index.
   * Assumes that memory is available for the vector since memory management
   * ensured space is available.
   *
   * @return a new, populated selection vector 2
   */

  private SelectionVector2 newSV2(RecordBatch incoming) {
    SelectionVector2 sv2 = new SelectionVector2(allocator);
    if (!sv2.allocateNewSafe(incoming.getRecordCount())) {
      throw UserException.resourceError(new OutOfMemoryException("Unable to allocate sv2 buffer"))
            .build(logger);
    }
    for (int i = 0; i < incoming.getRecordCount(); i++) {
      sv2.setIndex(i, (char) i);
    }
    sv2.setRecordCount(incoming.getRecordCount());
    return sv2;
  }

  private void validateBatchSize(long actualBatchSize, long memoryDelta) {
    if (actualBatchSize != memoryDelta) {
      ExternalSortBatch.logger.debug("Memory delta: {}, actual batch size: {}, Diff: {}",
                   memoryDelta, actualBatchSize, memoryDelta - actualBatchSize);
    }
  }

  private void sortIncomingBatch(VectorContainer convertedBatch, SelectionVector2 sv2) {

    SingleBatchSorter sorter;
    sorter = opCodeGen.getSorter(convertedBatch);
    try {
      sorter.setup(context, sv2, convertedBatch);
    } catch (SchemaChangeException e) {
      convertedBatch.clear();
      throw UserException.unsupportedError(e)
            .message("Unexpected schema change.")
            .build(logger);
    }
    try {
      sorter.sort(sv2);
    } catch (SchemaChangeException e) {
      convertedBatch.clear();
      throw UserException.unsupportedError(e)
                .message("Unexpected schema change.")
                .build(logger);
    }
  }

  @SuppressWarnings("resource")
  private void bufferBatch(VectorContainer convertedBatch, SelectionVector2 sv2, int netSize) {
    RecordBatchData rbd = new RecordBatchData(convertedBatch, allocator);
    try {
      rbd.setSv2(sv2);
      bufferedBatches.add(new BatchGroup.InputBatch(rbd.getContainer(), rbd.getSv2(), oContext, netSize));
      metrics.updatePeakBatches(bufferedBatches.size());

    } catch (Throwable t) {
      rbd.clear();
      throw t;
    }
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

    // Determine the number of batches to spill to create a spill file
    // of the desired size. The actual file size might be a bit larger
    // or smaller than the target, which is expected.

    int spillCount = 0;
    long spillSize = 0;
    for (InputBatch batch : bufferedBatches) {
      long batchSize = batch.getDataSize();
      spillSize += batchSize;
      spillCount++;
      if (spillSize + batchSize / 2 > config.spillFileSize()) {
        break; }
    }

    // Must always spill at least 2, even if this creates an over-size
    // spill file. But, if this is a final consolidation, we may have only
    // a single batch.

    spillCount = Math.max(spillCount, 2);
    spillCount = Math.min(spillCount, bufferedBatches.size());

    // Do the actual spill.

    mergeAndSpill(bufferedBatches, spillCount);
  }

  private void mergeAndSpill(LinkedList<? extends BatchGroup> source, int count) {
    spilledRuns.add(doMergeAndSpill(source, count));
    logger.trace("Completed spill: memory = {}",
                 allocator.getAllocatedMemory());
  }

  private BatchGroup.SpilledRun doMergeAndSpill(LinkedList<? extends BatchGroup> batchGroups, int spillCount) {
    List<BatchGroup> batchesToSpill = Lists.newArrayList();
    spillCount = Math.min(batchGroups.size(), spillCount);
    assert spillCount > 0 : "Spill count to mergeAndSpill must not be zero";
    for (int i = 0; i < spillCount; i++) {
      batchesToSpill.add(batchGroups.pollFirst());
    }

    // Merge the selected set of matches and write them to the
    // spill file. After each write, we release the memory associated
    // with the just-written batch.

    String outputFile = spillSet.getNextSpillFile();
    metrics.incrSpillCount();
    BatchGroup.SpilledRun newGroup = null;
    int spillBatchRowCount = memManager.getSpillBatchRowCount();
    try (AutoCloseable ignored = AutoCloseables.all(batchesToSpill);
         CopierHolder.BatchMerger merger = copierHolder.startMerge(schema, batchesToSpill, spillBatchRowCount)) {
      logger.trace("Spilling {} of {} batches, spill batch size = {} rows, memory = {}, write to {}",
                   batchesToSpill.size(), bufferedBatches.size() + batchesToSpill.size(),
                   spillBatchRowCount,
                   allocator.getAllocatedMemory(), outputFile);
      newGroup = new BatchGroup.SpilledRun(spillSet, outputFile, oContext);

      // The copier will merge records from the buffered batches into
      // the outputContainer up to targetRecordCount number of rows.
      // The actual count may be less if fewer records are available.

      while (merger.next()) {

        // Add a new batch of records (given by merger.getOutput()) to the spill
        // file.
        //
        // note that addBatch also clears the merger's output container

        newGroup.addBatch(merger.getOutput());
      }
      injector.injectChecked(context.getExecutionControls(), ExternalSortBatch.INTERRUPTION_WHILE_SPILLING, IOException.class);
      newGroup.closeOutputStream();
      logger.trace("Spilled {} batches, {} records; memory = {} to {}",
                   merger.getBatchCount(), merger.getRecordCount(),
                   allocator.getAllocatedMemory(), outputFile);
      newGroup.setBatchSize(merger.getEstBatchSize());
      return newGroup;
    } catch (Throwable e) {
      // we only need to clean up newGroup if spill failed
      try {
        if (newGroup != null) {
          AutoCloseables.close(e, newGroup);
        }
      } catch (Throwable t) { /* close() may hit the same IO issue; just ignore */ }

      // Here the merger is holding onto a partially-completed batch.
      // It will release the memory in the close() call.

      try {
        // Rethrow so we can decide how to handle the error.

        throw e;
      }

      // If error is a User Exception, just use as is.

      catch (UserException ue) { throw ue; }
      catch (Throwable ex) {
        throw UserException.resourceError(ex)
              .message("External Sort encountered an error while spilling to disk")
              .build(logger);
      }
    }
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
    if (spillSet.hasSpilled()) {
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

    MergeSort memoryMerge = new MergeSort(context, allocator, opCodeGen);
    try {
      memoryMerge.merge(bufferedBatches, outputBatch, container);
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
      metrics.incrMergeCount();
    }

    // Merge in-memory batches and spilled runs for the final merge.

    List<BatchGroup> allBatches = new LinkedList<>();
    allBatches.addAll(bufferedBatches);
    bufferedBatches.clear();
    allBatches.addAll(spilledRuns);
    spilledRuns.clear();

    logger.debug("Starting merge phase. Runs = {}, Alloc. memory = {}",
                 allBatches.size(), allocator.getAllocatedMemory());

    // Do the final merge as a results iterator.

    CopierHolder.BatchMerger merger = copierHolder.startFinalMerge(schema, allBatches, container, memManager.getMergeBatchRowCount());
    merger.next();
    return merger;
  }

  private boolean consolidateBatches() {

    // Determine additional memory needed to hold one batch from each
    // spilled run.

    int inMemCount = bufferedBatches.size();
    int spilledRunsCount = spilledRuns.size();

    // Can't merge more than will fit into memory at one time.

    int maxMergeWidth = memManager.getMaxMergeWidth();

    // But, must merge at least two batches.

    maxMergeWidth = Math.max(maxMergeWidth, 2);

    // If we can't fit all batches in memory, must spill any in-memory
    // batches to make room for multiple spill-merge-spill cycles.

    if (inMemCount > 0) {
      if (spilledRunsCount > maxMergeWidth) {
        spillFromMemory();
        return true;
      }

      // If we just plain have too many batches to merge, spill some
      // in-memory batches to reduce the burden.

      if (inMemCount + spilledRunsCount > config.mergeLimit()) {
        spillFromMemory();
        return true;
      }

      // If the on-disk batches and in-memory batches need more memory than
      // is available, spill some in-memory batches.

       if (! memManager.hasMergeCapacity(allocator.getAllocatedMemory(), spilledRunsCount)) {
        spillFromMemory();
        return true;
      }
    }

    // Merge on-disk batches if we have too many.

    int mergeCount = spilledRunsCount - maxMergeWidth;
    if (mergeCount <= 0) {
      return false;
    }

    // Must merge at least 2 batches to make progress.

    mergeCount = Math.max(2, mergeCount);

    // We will merge. This will create yet another spilled
    // run. Account for that.

    mergeCount += 1;

    mergeCount = Math.min(mergeCount, maxMergeWidth);

    // If we are going to merge, and we have batches in memory,
    // spill them and try again. We need to do this to ensure we
    // have adequate memory to hold the merge batches. We are into
    // a second-generation sort/merge so there is no point in holding
    // onto batches in memory.

    if (inMemCount > 0) {
      spillFromMemory();
      return true;
    }

    // Do the merge, then loop to try again in case not
    // all the target batches spilled in one go.

    mergeRuns(mergeCount);
    return true;
  }

  private void mergeRuns(int targetCount) {

    logger.trace("Merging {} on-disk runs, Alloc. memory = {}",
        targetCount, allocator.getAllocatedMemory());

    // Determine the number of runs to merge. The count should be the
    // target count. However, to prevent possible memory overrun, we
    // double-check with actual spill batch size and only spill as much
    // as fits in the merge memory pool.

    int mergeCount = 0;
    long mergeSize = 0;
    long mergeMemoryPool = memManager.getMergeMemoryLimit();
    for (SpilledRun run : spilledRuns) {
      long batchSize = run.getBatchSize();
      if (mergeSize + batchSize > mergeMemoryPool) {
        break;
      }
      mergeSize += batchSize;
      mergeCount++;
      if (mergeCount == targetCount) {
        break;
      }
    }

    // Must always spill at least 2, even if this creates an over-size
    // spill file. But, if this is a final consolidation, we may have only
    // a single batch.

    mergeCount = Math.max(mergeCount, 2);
    mergeCount = Math.min(mergeCount, spilledRuns.size());

    // Do the actual spill.

    mergeAndSpill(spilledRuns, mergeCount);
  }

  public void close() {
    if (spillSet.getWriteBytes() > 0) {
      logger.debug("End of sort. Total write bytes: {}, Total read bytes: {}",
                   spillSet.getWriteBytes(), spillSet.getWriteBytes());
    }
    metrics.updateWriteBytes(spillSet.getWriteBytes());
    RuntimeException ex = null;
    try {
      if (bufferedBatches != null) {
        closeBatchGroups(bufferedBatches);
        bufferedBatches = null;
      }
    } catch (RuntimeException e) {
      ex = e;
    }
    try {
      if (spilledRuns != null) {
        closeBatchGroups(spilledRuns);
        spilledRuns = null;
      }
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    try {
      copierHolder.close();
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    try {
      spillSet.close();
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    try {
      opCodeGen.close();
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    if (ex != null) {
      throw ex;
    }
  }

  private void closeBatchGroups(Collection<? extends BatchGroup> groups) {
    Exception ex = null;
    for (BatchGroup group: groups) {
      try {
        group.close();
      } catch (Exception e) {
        ex = (ex == null) ? e : ex;
      }
    }
    if (ex != null) {
      throw new RuntimeException("Batch close failed", ex);
    }
  }
}
