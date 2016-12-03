/**
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
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.xsort.MSortTemplate;
import org.apache.drill.exec.physical.impl.xsort.SingleBatchSorter;
import org.apache.drill.exec.physical.impl.xsort.managed.BatchGroup.InputBatch;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;

import com.google.common.collect.Lists;

/**
 * External sort batch: a sort batch which can spill to disk in
 * order to operate within a defined memory footprint.
 * <p>
 * <h4>Basic Operation</h4>
 * The operator has three key phases:
 * <p>
 * <ul>
 * <li>The load phase in which batches are read from upstream.</li>
 * <li>The merge phase in which spilled batches are combined to
 * reduce the number of files below the configured limit. (Best
 * practice is to configure the system to avoid this phase.)
 * <li>The delivery phase in which batches are combined to produce
 * the final output.</li>
 * </ul>
 * During the load phase:
 * <p>
 * <ul>
 * <li>The incoming (upstream) operator provides a series of batches.</li>
 * <li>This operator sorts each batch, and accumulates them in an in-memory
 * buffer.</li>
 * <li>If the in-memory buffer becomes too large, this operator selects
 * a subset of the buffered batches to spill.</li>
 * <li>Each spill set is merged to create a new, sorted collection of
 * batches, and each is spilled to disk.</li>
 * <li>To allow the use of multiple disk storage, each spill group is written
 * round-robin to a set of spill directories.</li>
 * </ul>
 * <p>
 * During the sort/merge phase:
 * <p>
 * <ul>
 * <li>When the input operator is complete, this operator merges the accumulated
 * batches (which may be all in memory or partially on disk), and returns
 * them to the output (downstream) operator in chunks of no more than
 * 32K records.</li>
 * <li>The final merge must combine a collection of in-memory and spilled
 * batches. Several limits apply to the maximum "width" of this merge. For
 * example, we each open spill run consumes a file handle, and we may wish
 * to limit the number of file handles. A consolidation phase combines
 * in-memory and spilled batches prior to the final merge to control final
 * merge width.</li>
 * <li>A special case occurs if no batches were spilled. In this case, the input
 * batches are sorted in memory without merging.</li>
 * </ul>
 * <p>
 * Many complex details are involved in doing the above; the details are explained
 * in the methods of this class.
 * <p>
 * <h4>Configuration Options</h4>
 * <dl>
 * <dt>drill.exec.sort.external.spill.fs</dt>
 * <dd>The file system (file://, hdfs://, etc.) of the spill directory.</dd>
 * <dt>drill.exec.sort.external.spill.directories</dt>
 * <dd>The (comma? space?) separated list of directories, on the above file
 * system, to which to spill files in round-robin fashion. The query will
 * fail if any one of the directories becomes full.</dt>
 * <dt>drill.exec.sort.external.spill.file_size</dt>
 * <dd>Target size for first-generation spill files Set this to large
 * enough to get nice long writes, but not so large that spill directories
 * are overwhelmed.</dd>
 * <dt>drill.exec.sort.external.mem_limit</dt>
 * <dd>Maximum memory to use for the in-memory buffer. (Primarily for testing.)</dd>
 * <dt>drill.exec.sort.external.batch_limit</dt>
 * <dd>Maximum number of batches to hold in memory. (Primarily for testing.)</dd>
 * <dt>drill.exec.sort.external.spill.max_count</dt>
 * <dd>Maximum number of batches to add to “first generation” files.
 * Defaults to 0 (no limit). (Primarily for testing.)</dd>
 * <dt>drill.exec.sort.external.spill.min_count</dt>
 * <dd>Minimum number of batches to add to “first generation” files.
 * Defaults to 0 (no limit). (Primarily for testing.)</dd>
 * <dt>drill.exec.sort.external.merge_limit</dt>
 * <dd>Sets the maximum number of runs to be merged in a single pass (limits
 * the number of open files.)</dd>
 * </dl>
 * <p>
 * The memory limit observed by this operator is the lesser of:
 * <ul>
 * <li>The maximum allocation allowed the the allocator assigned to this batch, or</li>
 * <li>The maximum limit set for this operator by the Foreman.</li>
 * <li>The maximum limit configured in the mem_limit parameter above. (Primarily for
 * testing.</li>
 * </ul>
 * <h4>Logging</h4>
 * Logging in this operator serves two purposes:
 * <li>
 * <ul>
 * <li>Normal diagnostic information.</li>
 * <li>Capturing the essence of the operator functionality for analysis in unit
 * tests.</li>
 * </ul>
 * Test logging is designed to capture key events and timings. Take care
 * when changing or removing log messages as you may need to adjust unit tests
 * accordingly.
 */

public class ExternalSortBatch extends AbstractRecordBatch<ExternalSort> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSortBatch.class);
  protected static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ExternalSortBatch.class);

  private final RecordBatch incoming;

  /**
   * Memory allocator for this operator itself. Incoming batches are
   * transferred into this allocator. Intermediate batches used during
   * merge also reside here.
   */

  private final BufferAllocator oAllocator;

  /**
   * Schema of batches that this operator produces.
   */

  private BatchSchema schema;

  private LinkedList<BatchGroup.InputBatch> bufferedBatches = Lists.newLinkedList();
  private LinkedList<BatchGroup.SpilledRun> spilledRuns = Lists.newLinkedList();
  private SelectionVector4 sv4;

  /**
   * The number of records to add to each output batch sent to the
   * downstream operator or spilled to disk.
   */

  private int outputBatchRecordCount;
  private int peakNumBatches = -1;

  /**
   * The copier uses the COPIER_BATCH_MEM_LIMIT to estimate the target
   * number of records to return in each batch.
   */
  private static final int MAX_MERGED_BATCH_SIZE = 256 * 1024;

  public static final String INTERRUPTION_AFTER_SORT = "after-sort";
  public static final String INTERRUPTION_AFTER_SETUP = "after-setup";
  public static final String INTERRUPTION_WHILE_SPILLING = "spilling";

  private long memoryLimit;

  /**
   * Iterates over the final, sorted results.
   */

  private SortResults resultsIterator;

  /**
   * Manages the set of spill directories and files.
   */

  private final SpillSet spillSet;

  /**
   * Manages the copier used to merge a collection of batches into
   * a new set of batches.
   */

  private final CopierHolder copierHolder;

  private enum SortState { LOAD, DELIVER, DONE }
  private SortState sortState = SortState.LOAD;
  private int totalRecordCount = 0;
  private int totalBatches = 0; // total number of batches received so far
  private final OperatorCodeGenerator opCodeGen;

  /**
   * Estimated size of the records for this query, updated on each
   * new batch received from upstream.
   */

  private int estimatedRecordSize;

  /**
   * Estimated size of the spill and output batches that this
   * operator produces, estimated from the estimated record
   * size.
   */

  private long estimatedOutputBatchSize;

  /**
   * Amount of the memory given to this operator that can buffer
   * batches from upstream in the in-memory generation, or the
   * current batches of on-disk runs during the final merge
   * phase.
   */

  private long inputMemoryPool;
  private long estimatedInputBatchSize;
  private long mergeMemoryPool;

  /**
   * Maximum number of batches to hold in memory.
   * (Primarily for testing.)
   */

  private int bufferedBatchLimit;
  private int mergeLimit;
  private int minSpillLimit;
  private int maxSpillLimit;
  private long spillFileSize;


  public enum Metric implements MetricDef {
    SPILL_COUNT,            // number of times operator spilled to disk
    PEAK_SIZE_IN_MEMORY,    // peak value for totalSizeInMemory
    PEAK_BATCHES_IN_MEMORY; // maximum number of batches kept in memory

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  /**
   * Iterates over the final sorted results. Implemented differently
   * depending on whether the results are in-memory or spilled to
   * disk.
   */

  public interface SortResults extends AutoCloseable {
    boolean next();
    @Override
    void close();
    int getBatchCount();
    int getRecordCount();
  }

  public ExternalSortBatch(ExternalSort popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context, true);
    this.incoming = incoming;
    oAllocator = oContext.getAllocator();
    opCodeGen = new OperatorCodeGenerator( context, popConfig );

    spillSet = new SpillSet( context, popConfig );
    copierHolder = new CopierHolder( context, oAllocator, opCodeGen );
    configure( context.getConfig() );
  }

  private void configure( DrillConfig config ) {

    // The maximum memory this operator can use. It is either the
    // limit set on the allocator or on the operator, whichever is
    // less.

    memoryLimit = Math.min( popConfig.getMaxAllocation(), oAllocator.getLimit() );

    // Optional configured memory limit, typically used only for testing.

    long configLimit = config.getBytes( ExecConstants.EXTERNAL_SORT_MAX_MEMORY );
    if ( configLimit > 0 ) {
      memoryLimit = Math.min( memoryLimit, configLimit );
    }

    // Optional limit on the number of buffered in-memory batches.
    // 0 means no limit. Used primarily for testing. Must allow at least two
    // batches or no merging can occur.

    bufferedBatchLimit = getConfigLimit( config, ExecConstants.EXTERNAL_SORT_BATCH_LIMIT, Integer.MAX_VALUE, 2 );

    // Optional limit on the number of spilled runs to merge in a single
    // pass. Limits the number of open file handles. Must allow at least
    // two batches to merge to make progress.

    mergeLimit = getConfigLimit( config, ExecConstants.EXTERNAL_SORT_MERGE_LIMIT, Integer.MAX_VALUE, 2 );

    // Limits on the minimum and maximum buffered batches to spill per
    // spill event.

    minSpillLimit = getConfigLimit( config, ExecConstants.EXTERNAL_SORT_MIN_SPILL, Integer.MAX_VALUE, 2 );
    maxSpillLimit = getConfigLimit( config, ExecConstants.EXTERNAL_SORT_MAX_SPILL, Integer.MAX_VALUE, 2 );
    if ( minSpillLimit > maxSpillLimit ) {
      minSpillLimit = Math.min( minSpillLimit, maxSpillLimit );
      maxSpillLimit = minSpillLimit;
    }

    // Limits the size of first-generation spill files.

    spillFileSize = config.getBytes( ExecConstants.EXTERNAL_SORT_SPILL_FILE_SIZE );

    logger.trace( "Config: memory limit = {}, batch limit = {}, min, max spill limit: {}, {}, merge limit = {}",
                  memoryLimit, bufferedBatchLimit, minSpillLimit, maxSpillLimit, mergeLimit );
  }

  private int getConfigLimit( DrillConfig config, String paramName, int valueIfZero, int minValue ) {
    int limit = config.getInt( paramName );
    if ( limit > 0 ) {
      limit = Math.max( limit, minValue );
    } else {
      limit = valueIfZero;
    }
    return limit;
  }

  @Override
  public int getRecordCount() {
    if (sv4 != null) {
      return sv4.getCount();
    }
    return container.getRecordCount();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return sv4;
  }

  private void closeBatchGroups(Collection<? extends BatchGroup> groups) {
    for (BatchGroup group: groups) {
      try {
        group.close();
      } catch (Exception e) {
        // collect all failure and make sure to cleanup all remaining batches
        // Originally we would have thrown a RuntimeException that would propagate to FragmentExecutor.closeOutResources()
        // where it would have been passed to context.fail()
        // passing the exception directly to context.fail(e) will let the cleanup process continue instead of stopping
        // right away, this will also make sure we collect any additional exception we may get while cleaning up
        context.fail(e);
      }
    }
  }

  @Override
  public void close() {
    try {
      if (bufferedBatches != null) {
        closeBatchGroups(bufferedBatches);
        bufferedBatches = null;
      }
      if (spilledRuns != null) {
        closeBatchGroups(spilledRuns);
        spilledRuns = null;
      }
    } finally {
      if (sv4 != null) {
        sv4.clear();
      }
      if ( resultsIterator != null ) {
        resultsIterator.close( );
      }
      copierHolder.close( );
      spillSet.close( );
      opCodeGen.close( );
      super.close();
    }
  }

  @Override
  public void buildSchema() {
    IterOutcome outcome = next(incoming);
    switch (outcome) {
      case OK:
      case OK_NEW_SCHEMA:
        for (VectorWrapper<?> w : incoming) {
          ValueVector v = container.addOrGet(w.getField());
          if (v instanceof AbstractContainerVector) {
            w.getValueVector().makeTransferPair(v); // Can we remove this hack?
            v.clear();
          }
          v.allocateNew(); // Can we remove this? - SVR fails with NPE (TODO)
        }
        container.buildSchema(SelectionVectorMode.NONE);
        container.setRecordCount(0);
        break;
      case STOP:
        state = BatchState.STOP;
        break;
      case OUT_OF_MEMORY:
        state = BatchState.OUT_OF_MEMORY;
        break;
      case NONE:
        state = BatchState.DONE;
        break;
      default:
        break;
    }
  }

  /**
   * Process each request for a batch. The first request retrieves
   * the all incoming batches and sorts them, optionally spilling to
   * disk as needed. Subsequent calls retrieve the sorted results in
   * fixed-size batches.
   */

  @Override
  public IterOutcome innerNext() {
    switch ( sortState ) {
    case DONE:
      return IterOutcome.NONE;
    case LOAD:
      return load( );
    case DELIVER:
      return nextOutputBatch( );
    default:
      throw new IllegalStateException( "Unexpected sort state: " + sortState );
    }
  }

  private IterOutcome nextOutputBatch( ) {
    if ( resultsIterator.next() ) {
      return IterOutcome.OK;
    } else {
      logger.trace( "Deliver phase complete: Returned {} batches, {} records",
                    resultsIterator.getBatchCount( ), resultsIterator.getRecordCount( ) );
      sortState = SortState.DONE;
      return IterOutcome.NONE;
    }
  }

  /**
   * Load and process a single batch, handling schema changes. In general, the
   * external sort accepts only one schema. It can handle compatible schemas
   * (which seems to mean the same columns in possibly different orders.)
   *
   * @return
   */

  private IterOutcome loadBatch() {
    IterOutcome upstream = next(incoming);
    switch (upstream) {
    case NONE:
      return upstream;
    case NOT_YET:
      throw new UnsupportedOperationException();
    case STOP:
      return upstream;
    case OK_NEW_SCHEMA:
    case OK:
      // Unfortunately, the first batch is sometimes (always?) OK
      // instead of OK_NEW_SCHEMA.
      setupSchema( upstream );

      // Add the batch to the in-memory generation, spilling if
      // needed.

      processBatch( );
      break;
    case OUT_OF_MEMORY:

      // Note: it is highly doubtful that this code actually works. It
      // requires that the upstream batches got to a safe place to run
      // out of memory and that no work as in-flight and thus abandoned.
      // Consider removing this case once resource management is in place.

      logger.debug("received OUT_OF_MEMORY, trying to spill");
      if (bufferedBatches.size( ) > 2) {
        final BatchGroup.SpilledRun merged = mergeAndSpill(bufferedBatches);
        if (merged != null) {
          spilledRuns.add(merged);
        }
      } else {
        logger.debug("not enough batches to spill, sending OUT_OF_MEMORY downstream");
        return IterOutcome.OUT_OF_MEMORY;
      }
      break;
    default:
      throw new UnsupportedOperationException();
    }
    return IterOutcome.OK;
  }

  /**
   * Load the results and sort them. May bail out early if an exceptional
   * condition is passed up from the input batch.
   *
   * @return
   */

  private IterOutcome load() {
    container.clear();

    // Loop over all input batches

    for ( ; ; ) {
      IterOutcome result = loadBatch( );

      // None means all batches have been read.

      if ( result == IterOutcome.NONE ) {
        break; }

      // Any outcome other than OK means something went wrong.

      if ( result != IterOutcome.OK ) {
        return result; }
    }

    // Anything to actually sort?

    if (totalRecordCount == 0) {
      sortState = SortState.DONE;
      return IterOutcome.NONE;
    }
    logger.trace( "Completed load phase: read {} batches", totalBatches );

    // Do the merge of the loaded batches. The merge can be done entirely in memory if
    // the results fit; else we have to do a disk-based merge of
    // pre-sorted spilled batches.

    if (canUseMemoryMerge( )) {
      return sortInMemory( );
    } else {
      return mergeSpilledRuns( );
    }
  }

  /**
   * All data has been read from the upstream batch. Determine if we
   * can use a fast in-memory sort, or must use a merge (which typically,
   * but not always, involves spilled batches.)
   *
   * @return
   */

  private boolean canUseMemoryMerge( ) {
    if ( spillSet.hasSpilled( ) ) { return false; }

    // Do we have enough memory for MSorter (the in-memory sorter)?

    long allocMem = oAllocator.getAllocatedMemory();
    long availableMem = mergeMemoryPool - allocMem;
    long neededForInMemorySort = SortRecordBatchBuilder.memoryNeeded(totalRecordCount) +
                                 MSortTemplate.memoryNeeded(totalRecordCount);
    if (availableMem < neededForInMemorySort) { return false; }

    // Make sure we don't exceed the maximum number of batches SV4 can address.

    if (bufferedBatches.size( ) > Character.MAX_VALUE) { return false; }

    // We can do an in-memory merge.

    return true;
  }

  /**
   * Handle a new schema from upstream. The ESB is quite limited in its ability
   * to handle schema changes.
   *
   * @param upstream
   */

  private void setupSchema( IterOutcome upstream )  {
    // First batch: we won't have a schema.

    if (schema == null) {
      schema = incoming.getSchema();
      logger.trace( "Start of load phase" );

    // Subsequent batches, nothing to do if same schema.

    } else if ( upstream == IterOutcome.OK ) {
      return;

    // Only change in the case that the schema truly changes. Artificial schema changes are ignored.

    } else if (incoming.getSchema().equals(schema)) {
      return;
    } else if (unionTypeEnabled) {
        schema = SchemaUtil.mergeSchemas(schema, incoming.getSchema());

        // New schema: must generate a new sorter and copier.

        copierHolder.close();
        opCodeGen.setSchema( schema );
    } else {
      throw UserException.unsupportedError( )
            .message("Schema changes not supported in External Sort. Please enable Union type.")
            .build(logger);
    }

    // Coerce all existing batches to the new schema.

    for (BatchGroup b : bufferedBatches) {
      b.setSchema(schema);
    }
    for (BatchGroup b : spilledRuns) {
      b.setSchema(schema);
    }
  }

  /**
   * Convert an incoming batch into the agree-upon format. (Also seems to
   * make a persistent shallow copy of the batch saved until we are ready
   * to sort or spill.)
   *
   * @return the converted batch, or null if the incoming batch is empty
   */

  private VectorContainer convertBatch( ) {
    if ( incoming.getRecordCount() == 0 ) {
      return null; }
    VectorContainer convertedBatch = SchemaUtil.coerceContainer(incoming, schema, oContext);
    return convertedBatch;
  }

  private SelectionVector2 makeSelectionVector() {
    if (incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      return incoming.getSelectionVector2().clone();
    } else {
      return newSV2();
    }
  }

  /**
   * Process the converted incoming batch by adding it to the in-memory store
   * of data, or spilling data to disk when necessary.
   *
   * @param convertedBatch
   * @return
   */

  private void processBatch( ) {

    // Convert the incoming batch to the agreed-upon schema.
    // No converted batch means we got an empty input batch.
    // Converting the batch transfers memory ownership to our
    // allocator. This gives a round-about way to learn the batch
    // size: check the before and after memory levels, then use
    // the difference as the batch size, in bytes.

    long startMem = oAllocator.getAllocatedMemory();
    VectorContainer convertedBatch = convertBatch( );
    long endMem = oAllocator.getAllocatedMemory();
    if ( convertedBatch == null ) {
      return;
    }

    // Compute batch size, including allowance for an sv2.

    long batchSize = endMem - startMem;
    if (incoming.getSchema().getSelectionVectorMode() != BatchSchema.SelectionVectorMode.TWO_BYTE) {
      batchSize += 2 * convertedBatch.getRecordCount();
    }

    SelectionVector2 sv2 = makeSelectionVector( );
    int count = sv2.getCount();
    totalRecordCount += count;
    totalBatches++;

    // Update the size based on the actual record count, not
    // the effective count as given by the selection vector
    // (which may exclude some records due to filtering.)

    updateMemoryEstimates( batchSize, convertedBatch.getRecordCount() );

    // Sort the incoming batch using either the original selection vector,
    // or a new one created here.

    SingleBatchSorter sorter;
    sorter = opCodeGen.getSorter(convertedBatch);
    try {
      sorter.setup(context, sv2, convertedBatch);
    } catch (SchemaChangeException e) {
      throw UserException.unsupportedError(e)
            .message("Unexpected schema change.")
            .build(logger);
    }
    sorter.sort(sv2);
    RecordBatchData rbd = new RecordBatchData(convertedBatch, oAllocator);
    boolean success = false;
    try {
      rbd.setSv2(sv2);
      bufferedBatches.add(new BatchGroup.InputBatch(rbd.getContainer(), rbd.getSv2(), oContext, batchSize));
      if (peakNumBatches < bufferedBatches.size()) {
        peakNumBatches = bufferedBatches.size();
        stats.setLongStat(Metric.PEAK_BATCHES_IN_MEMORY, peakNumBatches);
      }

      // The heart of the external sort operator: spill to disk when
      // the in-memory generation exceeds the allowed memory limit.

      if ( isSpillNeeded( ) ) {
        spillFromMemory( );
      }
      success = true;
    } finally {
      if (!success) {
        rbd.clear();
      }
    }
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
   * @param batchSize the overall size of the current batch received from
   * upstream
   * @param recordCount the number of actual (not filtered) records in
   * that upstream batch
   */

  private void updateMemoryEstimates(long batchSize, int recordCount) {

    // The record count should never be zero, but better safe than sorry...

    if ( recordCount == 0 ) {
      return; }

    // We know the batch size and number of records. Use that to estimate
    // the average record size. Since a typical batch has many records,
    // the average size is a fairly good estimator.

    int recordSize = (int) (batchSize / recordCount);

    // Record sizes may vary across batches. To be conservative, use
    // the largest size observed from incoming batches.

    int origEstimate = estimatedRecordSize;
    estimatedRecordSize = Math.max( estimatedRecordSize, recordSize );

    // Go no further if nothing changed.

    if ( estimatedRecordSize == origEstimate ) {
      return; }

    // Maintain an estimate of the incoming batch size: the largest
    // batch yet seen. Used to reserve memory for the next incoming
    // batch.

    estimatedInputBatchSize = Math.max( estimatedInputBatchSize, batchSize );

    // Determine the number of records to spill per merge step. The goal is to
    // spill batches of either 32K records, or as many records as fit into the
    // amount of memory dedicated to each batch, whichever is less.

    outputBatchRecordCount = Math.max(1, MAX_MERGED_BATCH_SIZE / estimatedRecordSize);
    outputBatchRecordCount = Math.min( outputBatchRecordCount, Short.MAX_VALUE );

    // Compute the estimated size of batches that this operator creates.
    // Note that this estimate DOES NOT apply to incoming batches as we have
    // no control over those.

    estimatedOutputBatchSize = outputBatchRecordCount * estimatedRecordSize;

    // Memory available for in-memory batches. These are batches arriving from
    // upstream and buffered in the "in-memory generation", or the "head"
    // batches from spilled runs. The amount of memory available is the
    // total memory for this operator less one output batch (created during
    // merge) and one input batch (the last one received before spilling.)
    // Then reduce this by 95% to allow for various overhead.

    inputMemoryPool = (long) ((memoryLimit - estimatedOutputBatchSize - estimatedInputBatchSize) * 0.95);

    // Must allow room for at least three input batches. (Really just two, but
    // the size is not precise, so allow some wiggle room.) This estimate does
    // not include the output batch produced from merging.

    inputMemoryPool = Math.max( inputMemoryPool, 3 * estimatedInputBatchSize );

    // The merge memory pool is similar, but excludes space for another input
    // batch because we're done reading the input by the time we get to merge.

    mergeMemoryPool = (long) ((memoryLimit - estimatedOutputBatchSize) * 0.95);

    // Must allow room for at least three output batches in order to do even
    // the most minimal merge.

    mergeMemoryPool = Math.max( mergeMemoryPool, 3 * estimatedOutputBatchSize );

    // Sanity check: if we've been given too little memory to make progress,
    // issue a warning but proceed anyway. Should only occur if something is
    // configured terribly wrong.

    long actualMemoryUse = Math.max( inputMemoryPool, mergeMemoryPool ) + estimatedOutputBatchSize;
    if ( actualMemoryUse > memoryLimit ) {
      logger.warn( "updateMemoryEstimates: potential memory overflow. " +
                   "Memory too low: allocated: {}, forced to: {}",
                   memoryLimit, actualMemoryUse );
    }

    // Log the calculated values. Turn this on if things seem amiss.

    logger.debug( "updateMemoryEstimates: record: size={}, target records={}; " +
                  "batch: input size={}, output size={}; " +
                  "memory: input={}, merge={}",
                estimatedRecordSize, outputBatchRecordCount, estimatedInputBatchSize,
                estimatedOutputBatchSize, inputMemoryPool, mergeMemoryPool );
  }

  /**
   * Determine if spill is needed after receiving the new record batch.
   * Spilling is driven purely by memory availability (and an optional
   * batch limit for testing.)
   *
   * @return true if spilling is needed, false otherwise
   */

  private boolean isSpillNeeded() {

    // The amount of memory this allocator currently uses.

    long allocMem = oAllocator.getAllocatedMemory();

    if (allocMem >= inputMemoryPool) { return true; }

    // Number of incoming batches (BatchGroups) exceed the limit and number of incoming
    // batches accumulated since the last spill exceed the defined limit

    if (bufferedBatches.size() > bufferedBatchLimit) { return true; }

    return false;
  }

  /**
   * Perform an in-memory sort of the buffered batches. Obviously can
   * be used only for the non-spilling case.
   *
   * @return
   */

  private IterOutcome sortInMemory( ) {
    logger.info("Starting in-memory sort. Batches = {}, Records = {}, Memory = {}",
                bufferedBatches.size( ), totalRecordCount, oAllocator.getAllocatedMemory() );
    InMemorySorter memoryMerge = new InMemorySorter( context, oAllocator, opCodeGen, this.outputBatchRecordCount );
    sv4 = memoryMerge.sort( bufferedBatches, this, container );
    if ( sv4 == null ) {
      sortState = SortState.DONE;
      return IterOutcome.STOP;
    }
    logger.info("Completed in-memory sort. Memory = {}",
                oAllocator.getAllocatedMemory() );
    resultsIterator = memoryMerge;
    sortState = SortState.DELIVER;
    return IterOutcome.OK_NEW_SCHEMA;
  }

  /**
   * Perform merging of (typically spilled) batches. First consolidates batches
   * as needed, then performs a final merge that is read one batch at a time
   * to deliver batches to the downstream operator.
   *
   * @return
   */

  private IterOutcome mergeSpilledRuns( ) {
    logger.info("Starting consolidate phase. Batches = {}, Records = {}, Memory = {}, In-memory batches {}, spilled runs {}",
                totalBatches, totalRecordCount, oAllocator.getAllocatedMemory(),
                bufferedBatches.size( ), spilledRuns.size( ) );

    // Consolidate batches to a number that can be merged in
    // a single last pass.

    while ( consolidateBatches( ) ) {
      ;
    }

    // Merge in-memory batches and spilled runs for the final merge.

    List<BatchGroup> allBatches = new LinkedList<>( );
    allBatches.addAll(bufferedBatches);
    bufferedBatches.clear();
    allBatches.addAll(spilledRuns);
    spilledRuns.clear();

    logger.info("Starting merge phase. Runs = {}, Memory = {}", allBatches.size(), oAllocator.getAllocatedMemory());

    // Do the final merge as a results iterator.

    CopierHolder.BatchMerger merger = copierHolder.startFinalMerge(schema, allBatches, container, outputBatchRecordCount);
    merger.next();
    resultsIterator = merger;
    sortState = SortState.DELIVER;
    return IterOutcome.OK_NEW_SCHEMA;
  }

  private boolean consolidateBatches() {

    // Determine additional memory needed to hold one batch from each
    // spilled run.

    int inMemCount = bufferedBatches.size( );
    int spilledRunsCount = spilledRuns.size( );
    long allocMem = oAllocator.getAllocatedMemory();
    long readMemory = spilledRunsCount * estimatedOutputBatchSize;
    long deficit = allocMem + readMemory - mergeMemoryPool;

    // Spill how many in-memory batches to make room?

    int memSpillCount = Math.max( 0, (int) (deficit / estimatedInputBatchSize) + 2 );

    // Can't merge any more runs than an SV4 can address.

    int runCount = inMemCount + spilledRunsCount;
    int sv4SpillCount = Math.max( 0, runCount - Short.MAX_VALUE );

    // Limit number of open files.

    int fileSpillCount = 0;
    if ( spilledRunsCount > mergeLimit ) {

      // Add one for each newly created file.

      fileSpillCount = spilledRunsCount - mergeLimit + 1;
      if ( inMemCount > 0 ) {

        // Spill all in-memory batches also.

        fileSpillCount += inMemCount + 1;
      }
    }

    // Choose the larger of the three numbers, but only up to the
    // maximum spill limit.

    int toSpill = Math.max( memSpillCount, sv4SpillCount );
    toSpill = Math.max( toSpill, fileSpillCount );
    toSpill = Math.min( toSpill, maxSpillLimit );

    // Anything to spill?

    if ( toSpill == 0 ) {
      return false; }

    // Prefer to spill from memory to consolidate.

    int memSpill = Math.min( toSpill, inMemCount );
    if ( memSpill > 0 ) {
      logger.trace( "Merging {} in-memory batches, Memory = {}",
          toSpill, oAllocator.getAllocatedMemory() );
      mergeAndSpill( bufferedBatches, memSpill );
      return true;
    }

    // Do the spill, then loop to try again in case not
    // all the target batches spilled in one go.

    logger.trace( "Merging {} on-disk runs, Memory = {}",
                  toSpill, oAllocator.getAllocatedMemory() );
    mergeAndSpill( spilledRuns, toSpill );
    return true;
  }

  /**
   * This operator has accumulated a set of sorted incoming record batches.
   * We wish to spill some of them to disk. To do this, a "{@link #copier}"
   * merges the target batches to produce a stream of new (merged) batches
   * which are then written to disk.
   * <p>
   * This method spills only half the accumulated batches
   * minimizing unnecessary disk writes. The exact count must lie between
   * the minimum and maximum spill counts.
   *
   * @param batchGroups the accumulated set of sorted incoming batches
   * @return a new batch group representing the combined set of spilled
   * batches
   */

  private void spillFromMemory( ) {
    int spillCount;
    if ( spillFileSize == 0 ) {
      // If no spill limit given, guess half the batches.

      spillCount = bufferedBatches.size() / 2;
    } else {
      // Spill file size given. Figure out how many
      // batches.

      long estSize = 0;
      spillCount = 0;
      for ( InputBatch batch : bufferedBatches ) {
        estSize += batch.getDataSize();
        if ( estSize > spillFileSize )
          break;
        spillCount++;
      }
    }

    // Upper spill bound (optional)

    spillCount = Math.min( spillCount, maxSpillLimit );

    // Lower spill bound (at least 2, perhaps more)

    spillCount = Math.max( spillCount, minSpillLimit );

    // Should not happen, but just to be sure...

    if ( spillCount == 0 ) {
      return; }

    // Do the actual spill.

    logger.debug("Starting spill from memory. Memory = {}, Batch count = {}, Spill count = {}",
                 oAllocator.getAllocatedMemory(), bufferedBatches.size( ), spillCount );
    mergeAndSpill( bufferedBatches, spillCount );
  }

  private void mergeAndSpill(LinkedList<? extends BatchGroup> source, int count) {
    if ( count == 0 ) {
      return; }
    spilledRuns.add( doMergeAndSpill( source, count ) );
  }

  private BatchGroup.SpilledRun mergeAndSpill(LinkedList<? extends BatchGroup> batchGroups) {
    return doMergeAndSpill( batchGroups, batchGroups.size( ) );
  }

  private BatchGroup.SpilledRun doMergeAndSpill(LinkedList<? extends BatchGroup> batchGroups, int spillCount) {
    List<BatchGroup> batchGroupList = Lists.newArrayList();
    spillCount = Math.min( batchGroups.size( ), spillCount );
    assert spillCount > 0 : "Spill count to mergeAndSpill must not be zero";
    long spillSize = 0;
    for (int i = 0; i < spillCount; i++) {
      BatchGroup batch = batchGroups.pollFirst();
      assert batch != null : "Encountered a null batch during merge and spill operation";
      batchGroupList.add(batch);
      spillSize += batch.getDataSize();
    }

    // Merge the selected set of matches and write them to the
    // spill file. After each write, we release the memory associated
    // with the just-written batch.

    String outputFile = spillSet.getNextSpillFile();
    stats.setLongStat(Metric.SPILL_COUNT, spillSet.getFileCount());
    BatchGroup.SpilledRun newGroup = null;
    try (AutoCloseable a = AutoCloseables.all(batchGroupList);
        CopierHolder.BatchMerger merger = copierHolder.startMerge(schema, batchGroupList, outputBatchRecordCount)) {
      logger.info("Merging and spilling to {}", outputFile);
      newGroup = new BatchGroup.SpilledRun(spillSet, outputFile, oContext, spillSize);

      // The copier will merge records from the buffered batches into
      // the outputContainer up to targetRecordCount number of rows.
      // The actual count may be less if fewer records are available.

      while (merger.next()) {

        // Add a new batch of records (given by merger.getOutput()) to the spill
        // file, opening the file if not yet open, and creating the target
        // directory if it does not yet exist.
        //
        // note that addBatch also clears the merger's output container
        newGroup.addBatch(merger.getOutput());
      }
      injector.injectChecked(context.getExecutionControls(), INTERRUPTION_WHILE_SPILLING, IOException.class);
      newGroup.closeOutputStream();
      logger.debug("mergeAndSpill: completed, memory = {}, spilled {} records to {}",
                   oAllocator.getAllocatedMemory(), merger.getRecordCount( ), outputFile);
      return newGroup;
    } catch (Throwable e) {
      // we only need to cleanup newGroup if spill failed
      try {
        if ( newGroup != null ) {
          AutoCloseables.close(e, newGroup);
        }
      } catch (Throwable t) { /* close() may hit the same IO issue; just ignore */ }

      // Here the merger is holding onto a partially-completed batch.
      // It will release the memory in the close() call.

      try {
        // Rethrow so we can organize how to handle the error.

        throw e;
      }

      // If error is a User Exception, just use as is.

      catch ( UserException ue ) { throw ue; }
      catch ( Throwable ex ) {
        throw UserException.resourceError(ex)
              .message("External Sort encountered an error while spilling to disk")
              .build(logger);
      }
    }
  }

  /**
   * Allocate and initialize the selection vector used as the sort index.
   * Assumes that memory is available for the vector since memory management
   * ensured space is available.
   *
   * @return
   */

  private SelectionVector2 newSV2() {
    SelectionVector2 sv2 = new SelectionVector2(oAllocator);
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

  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("A sort batch is not writable.");
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }
}
