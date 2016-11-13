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

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.xsort.MSortTemplate;
import org.apache.drill.exec.physical.impl.xsort.SingleBatchSorter;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.VectorAccessible;
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
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

/**
 * External sort batch: a sort batch which can spill to disk in
 * order to operate within a defined memory footprint.
 * <p>
 * Basic operation:
 * <ul>
 * <li>The incoming (upstream) operator provides a series of batches.</ul>
 * <li>This operator sorts each batch, and accumulates them in an in-memory
 * buffer.</li>
 * <li>If the in-memory buffer becomes too large, this operator selects
 * a subset of the buffered batches to spill.</li>
 * <li>Each spill set is merged to create a new, sorted collection of
 * batches, and each is spilled to disk.</li>
 * <li>To allow the use of multiple disk storage, each spill group is written
 * round-robin to a set of spill directories.</li>
 * <li>When the input operator is complete, this operator merges the accumulated
 * batches (which may be all in memory or partially on disk), and returns
 * them to the output (downstream) operator in chunks of no more than
 * 32K records.</li>
 * </ul>
 * <p>
 * Many complex details are involved in doing the above; the details are explained
 * in the methods of this class.
 * <p>
 * Configuration Options:
 * <dl>
 * <dt>drill.exec.sort.external.spill.fs</dt>
 * <dd>The file system (file://, hdfs://, etc.) of the spill directory.</dd>
 * <dt>drill.exec.sort.external.spill.directories</dt>
 * <dd>The (comma? space?) separated list of directories, on the above file
 * system, to which to spill files in round-robin fashion. The query will
 * fail if any one of the directories becomes full.</dt>
 * <dt>drill.exec.sort.external.spill.group.size</dt>
 * <dd>The number of batches to spill per spill event.
 * (Represented as <code>SPILL_BATCH_GROUP_SIZE</code>.)</dd>
 * <dt>drill.exec.sort.external.spill.threshold</dt>
 * <dd>The number of batches to accumulate in memory before starting
 * a spill event. (May be overridden if insufficient memory is available.)
 * (Represented as <code>SPILL_THRESHOLD</code>.)</dd>
 * </dl>
 * <p>
 * The memory limit observed by this operator is the lesser of:
 * <ul>
 * <li>The maximum allocation allowed the the allocator assigned to this batch, or</li>
 * <li>The maximum limit set for this operator by the Foreman.</li>
 * </ul>
 */

public class ExternalSortBatch extends AbstractRecordBatch<ExternalSort> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSortBatch.class);
  protected static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ExternalSortBatch.class);

  protected static final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  protected static final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  protected static final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);

  private final int SPILL_BATCH_GROUP_SIZE;
  private final int SPILL_THRESHOLD;
  private final RecordBatch incoming;

  /**
   * Memory allocator for this operator itself. Primarily used to create
   * intermediate vectors used for merging the incoming batches.
   */

  private final BufferAllocator oAllocator;

  /**
   * Schema of batches that this operator produces.
   */

  private BatchSchema schema;

  /**
   * Generated sort operation used to sort each incoming batch according to
   * the sort criteria specified in the {@link ExternalSort} definition of
   * this operator.
   */

  private SingleBatchSorter sorter;

  private LinkedList<BatchGroup.InputBatchGroup> batchGroups = Lists.newLinkedList();
  private LinkedList<BatchGroup.SpilledBatchGroup> spilledBatchGroups = Lists.newLinkedList();
  private SelectionVector4 sv4;

  private int batchesSinceLastSpill = 0;
  private int targetRecordCount;
  private int firstSpillBatchCount = 0;
  private int peakNumBatches = -1;

  /**
   * The copier uses the COPIER_BATCH_MEM_LIMIT to estimate the target
   * number of records to return in each batch.
   */
  private static final int COPIER_BATCH_MEM_LIMIT = 256 * 1024;

  public static final String INTERRUPTION_AFTER_SORT = "after-sort";
  public static final String INTERRUPTION_AFTER_SETUP = "after-setup";
  public static final String INTERRUPTION_WHILE_SPILLING = "spilling";

  private long memoryLimit;
  private final ExternalSort popConfig;
  private SortResults resultsIterator;
  private SpillSet spillSet;
  private CopierHolder copierHolder;

  private enum SortState { LOAD, DELIVER, DONE }
  private SortState sortState = SortState.LOAD;
  private int totalRecordCount = 0;
  private int totalBatches = 0; // total number of batches received so far


  public enum Metric implements MetricDef {
    SPILL_COUNT,            // number of times operator spilled to disk
    PEAK_SIZE_IN_MEMORY,    // peak value for totalSizeInMemory
    PEAK_BATCHES_IN_MEMORY; // maximum number of batches kept in memory

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public interface SortResults extends AutoCloseable {
    boolean next();
    @Override
    void close();
  }

  public ExternalSortBatch(ExternalSort popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, true);
    this.popConfig = popConfig;
    this.incoming = incoming;
    DrillConfig config = context.getConfig();
    SPILL_BATCH_GROUP_SIZE = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_GROUP_SIZE);
    SPILL_THRESHOLD = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_THRESHOLD);
    oAllocator = oContext.getAllocator();
    spillSet = new SpillSet( context, popConfig );
    copierHolder = new CopierHolder( this, context, oAllocator );

    // The maximum memory this operator can use. It is either the
    // limit set on the allocator or on the operator, whichever is
    // less.

    memoryLimit = (long) (Math.min( popConfig.getMaxAllocation(), oAllocator.getLimit() ) * 0.95);

    memoryLimit = 40_000_000;
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
      if (batchGroups != null) {
        closeBatchGroups(batchGroups);
        batchGroups = null;
      }
      if (spilledBatchGroups != null) {
        closeBatchGroups(spilledBatchGroups);
        spilledBatchGroups = null;
      }
    } finally {
      if (sv4 != null) {
        sv4.clear();
      }

      copierHolder.close( );
      super.close();

      if ( resultsIterator != null ) {
        resultsIterator.close( );
      }
      spillSet.close( );
    }
  }

  @Override
  public void buildSchema() throws SchemaChangeException {
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
   * @throws SchemaChangeException
   * @throws ClassTransformationException
   * @throws IOException
   * @throws InterruptedException
   */

  private IterOutcome loadBatch() throws SchemaChangeException, ClassTransformationException, IOException, InterruptedException {
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
        break;
      }

      // Add the batch to the in-memory generation, spilling if
      // needed.

      processBatch( convertedBatch, endMem - startMem );
      break;
    case OUT_OF_MEMORY:

      // Note: it is highly doubtful that this code actually works. It
      // requires that the upstream batches got to a safe place to run
      // out of memory and that no work as in-flight and thus abandoned.
      // Consider removing this case once resource management is in place.

      logger.debug("received OUT_OF_MEMORY, trying to spill");
      if (batchesSinceLastSpill > 2) {
        final BatchGroup.SpilledBatchGroup merged = mergeAndSpill(batchGroups);
        if (merged != null) {
          spilledBatchGroups.add(merged);
          batchesSinceLastSpill = 0;
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
   * @throws SchemaChangeException
   * @throws ClassTransformationException
   * @throws IOException
   * @throws InterruptedException
   */

  private IterOutcome loadIncoming() throws SchemaChangeException, ClassTransformationException, IOException, InterruptedException {
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

    // Do the merge of the loaded batches. The merge can be done entirely in memory if
    // the results fit; else we have to do a disk-based merge of
    // pre-sorted spilled batches.

    if (spillSet.hasSpilled( )) {
      return mergeSpilledRuns( );
    } else {
      return mergeInMemory( );
    }
  }

  public IterOutcome load( ) {
    try {
      return loadIncoming( );
    } catch(InterruptedException e) {
      return IterOutcome.STOP;
    } catch (SchemaChangeException ex) {
      kill(false);
      context.fail(UserException.unsupportedError(ex)
        .message("Sort doesn't currently support sorts with changing schemas").build(logger));
      return IterOutcome.STOP;
    } catch(ClassTransformationException | IOException ex) {
      kill(false);
      context.fail(ex);
      return IterOutcome.STOP;
    } catch (UnsupportedOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private void setupSchema( IterOutcome upstream ) throws SchemaChangeException {
    // First batch: we won't have a schema.

    if (schema == null) {
      schema = incoming.getSchema();

    // Subsequent batches, nothing to do if same schema.

    } else if ( upstream == IterOutcome.OK ) {
      return;

    // Only change in the case that the schema truly changes. Artificial schema changes are ignored.

    } else if (incoming.getSchema().equals(schema)) {
      return;
    } else if (unionTypeEnabled) {
        schema = SchemaUtil.mergeSchemas(schema, incoming.getSchema());

        // New schema: must generate a new sorter and copier.

        sorter = null;
        copierHolder.close();
    } else {
      throw new SchemaChangeException("Schema changes not supported in External Sort. Please enable Union type.");
    }

    // Coerce all existing batches to the new schema.

    for (BatchGroup b : batchGroups) {
      b.setSchema(schema);
    }
    for (BatchGroup b : spilledBatchGroups) {
      b.setSchema(schema);
    }
  }

  /**
   * Convert an incoming batch into the agree-upon format. (Also seems to
   * make a persistent shallow copy of the batch saved until we are ready
   * to sort or spill.)
   *
   * @return the converted batch, or null if the incoming batch is empty
   * @throws ClassTransformationException
   * @throws SchemaChangeException
   * @throws IOException
   */

  private VectorContainer convertBatch( ) throws ClassTransformationException, SchemaChangeException, IOException {
    if ( incoming.getRecordCount() == 0 ) {
      return null; }
    VectorContainer convertedBatch = SchemaUtil.coerceContainer(incoming, schema, oContext);
    if ( sorter == null ) {
      sorter = createNewSorter(context, convertedBatch);
    }
    return convertedBatch;
  }

  private SelectionVector2 makeSelectionVector() throws InterruptedException {
    if (incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      return incoming.getSelectionVector2().clone();
    } else {
      try {
        return newSV2();
      } catch (OutOfMemoryException e) {
        throw new OutOfMemoryException(e);
      }
    }
  }

  /**
   * Process the converted incoming batch by adding it to the in-memory store
   * of data, or spilling data to disk when necessary.
   *
   * @param convertedBatch
   * @return
   * @throws SchemaChangeException
   * @throws ClassTransformationException
   * @throws IOException
   * @throws InterruptedException
   */

  private void processBatch(VectorContainer convertedBatch, long batchSize) throws SchemaChangeException, ClassTransformationException, IOException, InterruptedException {
    SelectionVector2 sv2 = makeSelectionVector( );
    int count = sv2.getCount();
    totalRecordCount += count;
    totalBatches++;
    sorter.setup(context, sv2, convertedBatch);
    sorter.sort(sv2);
    RecordBatchData rbd = new RecordBatchData(convertedBatch, oAllocator);
    boolean success = false;
    try {
      rbd.setSv2(sv2);
      batchGroups.add(new BatchGroup.InputBatchGroup(rbd.getContainer(), rbd.getSv2(), oContext, batchSize));
      if (peakNumBatches < batchGroups.size()) {
        peakNumBatches = batchGroups.size();
        stats.setLongStat(Metric.PEAK_BATCHES_IN_MEMORY, peakNumBatches);
      }

      batchesSinceLastSpill++;
      if ( spillNeeded( ) ) {
        doSpill( );
      }
      success = true;
    } finally {
      if (!success) {
        rbd.clear();
      }
    }
  }

  /**
   * Spill batches to disk. The first spill batch establishes a baseline spilled
   * batch count. On subsequent spills, if the number of accumulated spilled
   * batches exceeds half the baseline, read, merge, and respill the existing
   * batches. (Thus, we can end up reading and writing the same data multiple
   * times.) Then, spill the batch groups accumulated since the last spill
   *
   * @throws SchemaChangeException which should never actually happen as we
   * caught schema changes while receiving the batches earlier
   */

  private void doSpill() throws SchemaChangeException {
    if (firstSpillBatchCount == 0) {
      firstSpillBatchCount = batchGroups.size();
    }

    if (spilledBatchGroups.size() > firstSpillBatchCount / 2) {
      logger.info("Merging spills");
      final BatchGroup.SpilledBatchGroup merged = mergeAndSpill(spilledBatchGroups);
      if (merged != null) {
        spilledBatchGroups.addFirst(merged);
      }
    }
    final BatchGroup.SpilledBatchGroup merged = mergeAndSpill(batchGroups);
    if (merged != null) { // make sure we don't add null to spilledBatchGroups
      spilledBatchGroups.add(merged);
      batchesSinceLastSpill = 0;
    }
  }

  /**
   * Determine if spill is needed after receiving the new record batch.
   * A number of conditions trigger spilling.
   *
   * @return true if spilling is needed, false otherwise
   */

  private boolean spillNeeded() {

    // The amount of memory this allocator currently uses.

    long allocMem = oAllocator.getAllocatedMemory();

    // The maximum memory this operator can use. It is either the
    // limit set on the allocator or on the operator, whichever is
    // less.

    long limitMem = Math.min( popConfig.getMaxAllocation(), oAllocator.getLimit() );

    long availableMem = limitMem - allocMem;

    // If we haven't spilled so far...

    if (! spillSet.hasSpilled()) {

      // Do we have enough memory for MSorter
      // if this turns out to be the last incoming batch?

      long neededForInMemorySort = SortRecordBatchBuilder.memoryNeeded(totalRecordCount) +
                                   MSortTemplate.memoryNeeded(totalRecordCount);
      if (availableMem < neededForInMemorySort) { return true; }

      // Make sure we don't exceed the maximum
      // number of batches SV4 can address

      if (totalBatches > Character.MAX_VALUE) { return true; }
    }

    // TODO(DRILL-4438) - consider setting this threshold more intelligently,
    // lowering caused a failing low memory condition (test in BasicPhysicalOpUnitTest)
    // to complete successfully (although it caused perf decrease as there was more spilling)

    // current memory used is more than 95% of memory usage limit of this operator

    if (allocMem > .95 * limitMem) { return true; }

    // Number of incoming batches (BatchGroups) exceed the limit and number of incoming
    // batches accumulated since the last spill exceed the defined limit

    if (batchGroups.size() > SPILL_THRESHOLD &&
        batchesSinceLastSpill >= SPILL_BATCH_GROUP_SIZE) { return true; }

    return false;
  }

  public IterOutcome mergeInMemory( ) throws SchemaChangeException, ClassTransformationException, IOException {
    InMemoryMerge memoryMerge = new InMemoryMerge( context, oAllocator, popConfig );
    sv4 = memoryMerge.merge( batchGroups, this, container );
    if ( sv4 == null ) {
      sortState = SortState.DONE;
      return IterOutcome.STOP;
    }
    resultsIterator = memoryMerge;
    sortState = SortState.DELIVER;
    return IterOutcome.OK_NEW_SCHEMA;
  }

  private IterOutcome mergeSpilledRuns( ) throws SchemaChangeException {
    final BatchGroup.SpilledBatchGroup merged = mergeAndSpill(batchGroups);
    if (merged != null) {
      spilledBatchGroups.add(merged);
    }
    List<BatchGroup> allBatches = new LinkedList<>( );
    allBatches.addAll(batchGroups);
    allBatches.addAll(spilledBatchGroups);
    batchGroups = null;
    spilledBatchGroups = null; // no need to cleanup spilledBatchGroups, all it's batches are in allBatches now

    logger.info("Starting to merge. {} batch groups. Current allocated memory: {}", allBatches.size(), oAllocator.getAllocatedMemory());

    int estimatedRecordSize = 0;
    for (VectorWrapper<?> w : allBatches.get(0)) {
      try {
        estimatedRecordSize += TypeHelper.getSize(w.getField().getType());
      } catch (UnsupportedOperationException e) {
        estimatedRecordSize += 50;
      }
    }
    targetRecordCount = Math.min(MAX_BATCH_SIZE, Math.max(1, COPIER_BATCH_MEM_LIMIT / estimatedRecordSize));

    CopierHolder.BatchMerger merger = copierHolder.startFinalMerge(schema, allBatches, container, targetRecordCount);
    merger.next();
    resultsIterator = merger;
    sortState = SortState.DELIVER;
    return IterOutcome.OK_NEW_SCHEMA;
  }

   /**
   * This operator has accumulated a set of sorted incoming record batches.
   * We wish to spill some of them to disk. To do this, a "{@link #copier}"
   * merges the target batches to produce a stream of new (merged) batches
   * which are then written to disk.
   * <p>
   * This method spills only half the accumulated batches (presumably
   * minimizing unnecessary disk writes.) Note that the number spilled here
   * is not correlated with the {@link #SPILL_BATCH_GROUP_SIZE} used to
   * detect spilling is needed.
   *
   * @param batchGroups the accumulated set of sorted incoming batches
   * @return a new batch group representing the combined set of spilled
   * batches
   * @throws SchemaChangeException should never occur as schema change
   * detection is done as each incoming batch arrives
   */

  public BatchGroup.SpilledBatchGroup mergeAndSpill(LinkedList<? extends BatchGroup> batchGroups) throws SchemaChangeException {
    logger.debug("mergeAndSpill: starting total size in memory = {}", oAllocator.getAllocatedMemory());
    List<BatchGroup> batchGroupList = Lists.newArrayList();
    int batchCount = batchGroups.size();
    for (int i = 0; i < batchCount / 2; i++) {
      if (batchGroups.size() == 0) {
        break;
      }
      BatchGroup batch = batchGroups.pollLast();
      assert batch != null : "Encountered a null batch during merge and spill operation";
      batchGroupList.add(batch);
    }

    if (batchGroupList.size() == 0) {
      return null;
    }
    int estimatedRecordSize = 0;
    for (VectorWrapper<?> w : batchGroupList.get(0)) {
      try {
        estimatedRecordSize += TypeHelper.getSize(w.getField().getType());
      } catch (UnsupportedOperationException e) {
        estimatedRecordSize += 50;
      }
    }
    int targetRecordCount = Math.max(1, COPIER_BATCH_MEM_LIMIT / estimatedRecordSize);

    logger.debug("mergeAndSpill: estimated record size = {}, target record count = {}", estimatedRecordSize, targetRecordCount);

    String outputFile = spillSet.getNextSpillFile();

    // Merge the selected set of matches and write them to the
    // spill file. After each write, we release the memory associated
    // with the just-written batch.

    stats.setLongStat(Metric.SPILL_COUNT, spillSet.getFileCount());
    BatchGroup.SpilledBatchGroup newGroup = null;
    try (AutoCloseable a = AutoCloseables.all(batchGroupList);
        CopierHolder.BatchMerger merger = copierHolder.startMerge(schema, batchGroupList, targetRecordCount)) {
      newGroup = new BatchGroup.SpilledBatchGroup(spillSet, outputFile, oContext, 0); // TODO Real size

      // The copier will merge records from the buffered batches into
      // the outputContainer up to targetRecordCount number of rows.
      // The actual count may be less if fewer records are available.

      logger.info("Merging and spilling to {}", outputFile);
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
    } catch (Throwable e) {
      // we only need to cleanup newGroup if spill failed
      try {
        if ( newGroup != null ) {
          AutoCloseables.close(e, newGroup);
        }
      } catch (Throwable t) { /* close() may hit the same IO issue; just ignore */ }
      throw UserException.resourceError(e)
        .message("External Sort encountered an error while spilling to disk")
              .addContext(e.getMessage() /* more detail */)
        .build(logger);
    } finally {
    }
    logger.debug("mergeAndSpill: final total size in memory = {}", oAllocator.getAllocatedMemory());
    logger.info("Completed spilling to {}", outputFile);
    return newGroup;
  }

  private SelectionVector2 newSV2() throws OutOfMemoryException, InterruptedException {
    SelectionVector2 sv2 = new SelectionVector2(oAllocator);
    if (!sv2.allocateNewSafe(incoming.getRecordCount())) {
      throw new OutOfMemoryException("Unable to allocate sv2 buffer after repeated attempts");
    }
    for (int i = 0; i < incoming.getRecordCount(); i++) {
      sv2.setIndex(i, (char) i);
    }
    sv2.setRecordCount(incoming.getRecordCount());
    return sv2;
  }

  public SingleBatchSorter createNewSorter(FragmentContext context, VectorAccessible batch)
          throws ClassTransformationException, IOException, SchemaChangeException{
    CodeGenerator<SingleBatchSorter> cg = CodeGenerator.get(SingleBatchSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    ClassGenerator<SingleBatchSorter> g = cg.getRoot();

    generateComparisons(g, batch);

    return context.getImplementationClass(cg);
  }

  protected void generateComparisons(ClassGenerator<?> g, VectorAccessible batch) throws SchemaChangeException {
    g.setMappingSet(MAIN_MAPPING);

    for (Ordering od : popConfig.getOrderings()) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      g.setMappingSet(LEFT_MAPPING);
      HoldingContainer left = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(RIGHT_MAPPING);
      HoldingContainer right = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(MAIN_MAPPING);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, ClassGenerator.BlkCreateMode.FALSE);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));
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
