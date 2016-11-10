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
package org.apache.drill.exec.physical.impl.xsort;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
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
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.xsort.BatchGroup.InputBatchGroup;
import org.apache.drill.exec.physical.impl.xsort.BatchGroup.SpilledBatchGroup;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
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
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ExternalSortBatch.class);

  static final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private static final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private static final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);

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

  BatchSchema schema;

  private LinkedList<BatchGroup.SpilledBatchGroup> spilledBatchGroups = Lists.newLinkedList();
  private SelectionVector4 sv4;

  private int peakNumBatches = -1;

  private CopierHolder copierHolder;

  /**
   * Target size, in bytes, of each batch written to disk when spilling,
   * and each batch returned to the downstream operator. Since the copier
   * works in terms of records, the code estimates row width, then
   * divides the memory limit by row width to get the target batch
   * sizes (in rows.) The target size is the maximum memory allocated
   * to the copier, though for smaller rows the memory used is often
   * much less.
   */

  protected enum SortState {
    START,
    RESULTS,
    END,
    ERROR
  }

  protected SortState sortState = SortState.START;

  private static final int COPIER_BATCH_MEM_LIMIT = (int) PriorityQueueCopier.MAX_ALLOCATION;

  public static final String INTERRUPTION_AFTER_SORT = "after-sort";
  public static final String INTERRUPTION_AFTER_SETUP = "after-setup";
  public static final String INTERRUPTION_WHILE_SPILLING = "spilling";

  private boolean enableDebug = true; // Temporary, do not check in

  public enum Metric implements MetricDef {
    SPILL_COUNT,            // number of times operator spilled to disk
    PEAK_SIZE_IN_MEMORY,    // peak value for totalSizeInMemory
    PEAK_BATCHES_IN_MEMORY; // maximum number of batches kept in memory

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public ExternalSortBatch(ExternalSort popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, true);
    this.incoming = incoming;
    DrillConfig config = context.getConfig();
    SPILL_BATCH_GROUP_SIZE = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_GROUP_SIZE);
    SPILL_THRESHOLD = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_THRESHOLD);
    oAllocator = oContext.getAllocator();
    spillSet = new SpillSet( context );
    copierHolder = new CopierHolder( this, context );
  }

  public class SortBuilder {

    /**
     * Generated sort operation used to sort each incoming batch according to
     * the sort criteria specified in the {@link ExternalSort} definition of
     * this operator.
     */

    private SingleBatchSorter sorter;
    private LinkedList<BatchGroup.InputBatchGroup> batchGroups = Lists.newLinkedList();
    private SortResults resultsIterator;
    private int batchesSinceLastSpill = 0;
    private int firstSpillBatchCount = 0;
    private int totalRecordCount = 0;
    private int totalBatches = 0; // total number of batches received so far
    private ExternalSortBatch esb;
    FragmentContext context;
    private final RecordBatch incoming;

    /**
     * Estimate of the average record width (including overhead) of all
     * records currently stored in memory.
     */

    private int recordWidthEstimate;

    /**
     * The cumulative count of the records spilled to disk thus
     * far. Used to calculate the in-memory record count when
     * estimating the average row width.
     */

    private int spilledRecordCount;


    public SortBuilder( ExternalSortBatch esb, FragmentContext context ) {
      this.esb = esb;
      incoming = esb.incoming;
      this.context = context;
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

    public IterOutcome build() throws SchemaChangeException, ClassTransformationException, IOException, InterruptedException {
      IterOutcome upstreamOutcome = gatherBatches( );
      if ( upstreamOutcome != IterOutcome.NONE ) {
        return upstreamOutcome;
      }

      return finalMerge( );
    }

    /**
     * Loop to read all batches from the upstream operator, buffer them in memory,
     * and optionally spill to disk as needed.
     *
     * @return
     * @throws SchemaChangeException
     * @throws ClassTransformationException
     * @throws IOException
     * @throws InterruptedException
     */
    private IterOutcome gatherBatches() throws SchemaChangeException, ClassTransformationException, IOException, InterruptedException {

      // Loop over all input batches

      IterOutcome result;
      for ( ; ; ) {
        result = loadBatch( );

        // None means all batches have been read.

        if ( result == IterOutcome.NONE ) {
          break; }

        // Any outcome other than OK means something went wrong.

        if ( result != IterOutcome.OK ) {
          break; }
      }
      return result;
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
      IterOutcome upstream = esb.next(incoming);
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

        VectorContainer convertedBatch = convertBatch( );
        if ( convertedBatch == null ) {
          break;
        }

        // Add the batch to our buffered set of batches.

        processBatch( convertedBatch );
        break;
      case OUT_OF_MEMORY:

        // Note: it is highly doubtful that this code actually works. It
        // requires that the upstream batches got to a safe place to run
        // out of memory and that no work as in-flight and thus abandoned.
        // Consider removing this case once resource management is in place.

        logger.debug("received OUT_OF_MEMORY, trying to spill");
        if (batchesSinceLastSpill > 2) {
          final SpillEvent spill = spillSet.mergeAndSpill(batchGroups, recordWidthEstimate);
          if (spill != null && spill.recordCount > 0) {
            spilledBatchGroups.add(spill.batchGroup);
            spilledRecordCount += spill.recordCount;
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

    private void setupSchema(IterOutcome upstream) throws SchemaChangeException {
      // First batch: we won't have a schema.

      if (esb.schema == null) {
        esb.schema = incoming.getSchema();

      // Subsequent batches, nothing to do if same schema.

      } else if ( upstream == IterOutcome.OK ) {
        return;

      // only change in the case that the schema truly changes.  Artificial schema changes are ignored.
      } else if (incoming.getSchema().equals(esb.schema)) {
        return;
      } else if (esb.unionTypeEnabled) {
        esb.schema = SchemaUtil.mergeSchemas(esb.schema, incoming.getSchema());
          sorter = null;
      } else {
        throw new SchemaChangeException("Schema changes not supported in External Sort. Please enable Union type.");
      }

      // Coerce all existing batches to the new schema.

      for (BatchGroup b : batchGroups) {
        b.setSchema(esb.schema);
      }
      for (BatchGroup b : spilledBatchGroups) {
        b.setSchema(esb.schema);
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
      return SchemaUtil.coerceContainer(incoming, schema, oContext);
    }

    /**
     * Process the converted incoming batch. Sort it, add it to the in-memory store
     * of data, and spill to disk when necessary.
     *
     * @param convertedBatch
     * @return
     * @throws SchemaChangeException
     * @throws ClassTransformationException
     * @throws IOException
     * @throws InterruptedException
     */

    private void processBatch(VectorContainer convertedBatch) throws SchemaChangeException, ClassTransformationException, IOException, InterruptedException {
      SelectionVector2 sv2 = makeSelectionVector( );
      int count = sv2.getCount();
      totalRecordCount += count;
      totalBatches++;
      if ( sorter == null ) {
        sorter = createNewSorter(context, convertedBatch);
      }
      sorter.setup(context, sv2, convertedBatch);
      sorter.sort(sv2);
      RecordBatchData rbd = new RecordBatchData(convertedBatch, esb.oAllocator);
      boolean success = false;
      try {
        rbd.setSv2(sv2);
        batchGroups.add(new BatchGroup.InputBatchGroup(rbd.getContainer(), rbd.getSv2(), oContext));
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
     * Determine if spill is needed after receiving the new record batch.
     * A number of conditions trigger spilling.
     *
     * @return true if spilling is needed, false otherwise
     */

    private boolean spillNeeded() {

      // The amount of memory this allocator currently uses.

      long allocMem = esb.oAllocator.getAllocatedMemory();

      // The maximum memory this operator can use. It is either the
      // limit set on the allocator or on the operator, whichever is
      // less.

      long limitMem = Math.min( esb.popConfig.getMaxAllocation(), esb.oAllocator.getLimit() );

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

    /**
     * Spill batches to disk. The first spill batch establishes a baseline spilled
     * batch count. On subsequent spills, if the number of accumulated spilled
     * batches exceeds half the baseline, read, merge, and respill the existing
     * batches. (Thus, we can end up reading and writing the same data multiple
     * times.) Then, spill the batch groups accumulated since the last spill.
     *
     * @throws SchemaChangeException which should never actually happen as we
     * caught schema changes while receiving the batches earlier
     */

    private void doSpill() throws SchemaChangeException {

      updateRecordWidthEstimate( );
     if (! spillSet.hasSpilled()) {
        firstSpillBatchCount = batchGroups.size();
      }

      if (spilledBatchGroups.size() > firstSpillBatchCount / 2) {
        logger.info("Merging spills: threshhold=" + (firstSpillBatchCount / 2) +
                    ", spilled count: " + spilledBatchGroups.size());
        final SpillEvent spill = spillSet.mergeAndSpill(spilledBatchGroups, recordWidthEstimate);
        assert spill != null  &&  spill.recordCount > 0;
        spilledBatchGroups.addFirst(spill.batchGroup);
      }
      final SpillEvent spill = spillSet.mergeAndSpill(batchGroups, recordWidthEstimate);
      if (spill != null && spill.recordCount > 0) { // make sure we don't add null to spilledBatchGroups
        spilledBatchGroups.add(spill.batchGroup);
        batchesSinceLastSpill = 0;
      }
    }

    /**
     * Estimate the row width based on the total memory consumed
     * by the in-memory batches, divided by the number or rows
     * in those batches. This estimate "amortizes" overhead memory
     * across rows so we get an accurate memory use estimate later.
     */

    private void updateRecordWidthEstimate() {

      long memUsed = esb.getAllocator( ).getAllocatedMemory();
      int inMemRecordCount = totalRecordCount - spilledRecordCount;
      recordWidthEstimate = Math.max( recordWidthEstimate,
                                             (int) (memUsed / inMemRecordCount) );
      if ( esb.enableDebug ) {
        System.out.println( String.format("Record width estimate: %d = %d bytes / %d records",
                                          recordWidthEstimate, memUsed, inMemRecordCount ) );
      }
    }

    /**
     * Each batch is sorted individually before being spilled and/or merged.
     * Sorting is done via a 2-byte selection vector, where the selection
     * vector provides an indirection into the underlying records. The sort
     * batch can reuse a selection vector provided by the incoming batch,
     * or will create a new one if the incoming batch does not provide one.
     *
     * @return
     * @throws InterruptedException
     */

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

    private SelectionVector2 newSV2() throws OutOfMemoryException, InterruptedException {
      @SuppressWarnings("resource")
      SelectionVector2 sv2 = new SelectionVector2(esb.oAllocator);
      if (!sv2.allocateNewSafe(incoming.getRecordCount())) {
        try {
          final SpillEvent spill = spillSet.mergeAndSpill(batchGroups, recordWidthEstimate);
          if (spill != null && spill.recordCount > 0) {
            spilledBatchGroups.add(spill.batchGroup);
          } else {
            throw UserException.memoryError("Unable to allocate sv2 for %d records, and not enough batchGroups to spill.",
                incoming.getRecordCount())
              .addContext("batchGroups.size", batchGroups.size())
              .addContext("spilledBatchGroups.size", spilledBatchGroups.size())
              .addContext("allocated memory", esb.oAllocator.getAllocatedMemory())
              .addContext("allocator limit", esb.oAllocator.getLimit())
              .build(logger);
          }
        } catch (SchemaChangeException e) {
          throw new RuntimeException(e);
        }
        int waitTime = 1;
        while (true) {
          try {
            Thread.sleep(waitTime * 1000);
          } catch(final InterruptedException e) {
            if (!context.shouldContinue()) {
              throw e;
            }
          }
          waitTime *= 2;
          if (sv2.allocateNewSafe(incoming.getRecordCount())) {
            break;
          }
          if (waitTime >= 32) {
            throw new OutOfMemoryException("Unable to allocate sv2 buffer after repeated attempts");
          }
        }
      }
      for (int i = 0; i < incoming.getRecordCount(); i++) {
        sv2.setIndex(i, (char) i);
      }
      sv2.setRecordCount(incoming.getRecordCount());
      return sv2;
    }

    public SingleBatchSorter createNewSorter(FragmentContext context,
        VectorAccessible batch) throws ClassTransformationException,
        IOException, SchemaChangeException {
      CodeGenerator<SingleBatchSorter> cg = CodeGenerator.get(
          SingleBatchSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry(),
          context.getOptions());
      ClassGenerator<SingleBatchSorter> g = cg.getRoot();

      generateComparisons(g, batch);

      return context.getImplementationClass(cg);
    }

    private IterOutcome finalMerge( ) throws SchemaChangeException, ClassTransformationException, IOException {

      // Anything to actually sort?

      if (totalRecordCount == 0) {
        return IterOutcome.NONE; }

      // Do the actual sort. The sort can be done entirely in memory if
      // the results fit; else we have to do a disk-based merge of
      // pre-sorted spilled batches.

      if (spillSet.hasSpilled( )) {
        // some batches were spilled
        resultsIterator = new SpilledMerge( batchGroups, recordWidthEstimate );
      } else {
        resultsIterator = new InMemoryMerge( esb, batchGroups );
      }
      IterOutcome mergeOutcome = resultsIterator.merge( );
      return mergeOutcome;
    }

    public SortResults getResults( ) {
      return resultsIterator;
    }

   /**
     * Remove any left-over in-memory batches if the build phase fails.
     * Note: if the phase succeeds, the list of in-memory batches is
     * empty, so no close is required.
     */

    public void close() {
      closeBatchGroups(batchGroups);
    }
  }

  public SpillSet spillSet;
  private SortResults resultsIterator;

  public class SpillSet {

    /**
     * The HDFS file system (for local directories, HDFS storage, etc.) used to
     * create the temporary spill files. Allows spill files to be either on local
     * disk, or in a DFS. (The admin can choose to put spill files in DFS when
     * nodes provide insufficient local disk space)
     */

    private FileSystem fs;

    private final Iterator<String> dirs;

    /**
     * Set of directories to which this operator should write spill files in a round-robin
     * fashion. The operator requires at least one spill directory, but can
     * support any number. The admin must ensure that sufficient space exists
     * on all directories as this operator does not check space availability
     * before writing to the directories.
     */

    private Set<Path> currSpillDirs = Sets.newTreeSet();

    /**
     * The base part of the file name for spill files. Each file has this
     * name plus an appended spill serial number.
     */

    private final String fileName;

    private int spillCount = 0;

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

    public SpillSet( FragmentContext context ) {
      DrillConfig config = context.getConfig();
      dirs = Iterators.cycle(config.getStringList(ExecConstants.EXTERNAL_SORT_SPILL_DIRS));
      Configuration conf = new Configuration();
      conf.set("fs.default.name", config.getString(ExecConstants.EXTERNAL_SORT_SPILL_FILESYSTEM));
      try {
        this.fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      FragmentHandle handle = context.getHandle();
      fileName = String.format("%s_majorfragment%s_minorfragment%s_operator%s", QueryIdHelper.getQueryId(handle.getQueryId()),
          handle.getMajorFragmentId(), handle.getMinorFragmentId(), popConfig.getOperatorId());
    }

    public boolean hasSpilled() {
      return spillCount > 0;
    }

    public SpillEvent mergeAndSpill(LinkedList<? extends BatchGroup> batchGroups, int estimatedRecordSize) throws SchemaChangeException {
//      logger.debug("Copier allocator current allocation {}", copierAllocator.getAllocatedMemory());
      logger.debug("mergeAndSpill: starting total size in memory = {}", oAllocator.getAllocatedMemory());
      if ( enableDebug ) {
        System.out.println( // Debugging only, do not check in
            "Before spilling, buffered batch count: " + batchGroups.size() );
      }

      // Determine the number of batches to spill. This is set by the
      // SPILL_BATCH_GROUP_SIZE parameter, but adjusted to not be fewer than
      // half of the batches or more than all of them.

      int batchCount = batchGroups.size();
      int spillBatchCount = Math.min( Math.max( SPILL_BATCH_GROUP_SIZE, batchCount / 2 ), batchCount );
      SpillEvent spill = mergeAndSpill( batchGroups, spillBatchCount, estimatedRecordSize );
      if ( spill == null )
        return null;

      logger.debug("mergeAndSpill: final total size in memory = {}", oAllocator.getAllocatedMemory());
      logger.info("Completed spilling to {}", spill.fileName);
      if ( enableDebug ) {
        System.out.println( // Debugging only, do not check in
            "After spilling, buffered batch count: " + batchGroups.size() );
      }
      return spill;
    }

    public SpillEvent mergeAndSpill(LinkedList<? extends BatchGroup> batchGroups, int spillBatchCount, int estimatedRecordSize) throws SchemaChangeException {


      // Create a list of batch groups to spill. If batches are:
      //
      // [ 1 2 3 4 5 6 ]
      //
      // We create the spill list as:
      //
      // [ 6 5 4 ]
      //
      // Leaving the accumulated batches as:
      //
      // [ 1 2 3 ]
      //
      // This means that the earliest-arriving batches may never actually
      // spilled, preferring to spill the latest-arriving batches instead.

      List<BatchGroup> batchGroupList = Lists.newArrayList();
      for (int i = 0; i < spillBatchCount; i++) {
        if (batchGroups.isEmpty()) {
          break;
        }
        BatchGroup batch = batchGroups.pollLast();
        assert batch != null : "Encountered a null batch during merge and spill operation";
        batchGroupList.add(batch);
      }

      if (batchGroupList.size() == 0) {
        return null;
      }

      // Determine the number of records to spill per merge step. The goal is to
      // spill batches of either 32K records, or as may records as fit into the
      // amount of memory dedicated to the copier, whichever is less.

      int targetRecordCount = getTargetRecordCount(estimatedRecordSize);
//      if ( enableDebug ) { // Do not check in
//        System.out.println( "Spill - record width estimate: " + estimatedRecordSize ); // Do not check in
//        System.out.println( "Spill - target record count: " + targetRecordCount ); // Do not check in
//        System.out.println( "Spill - actual record count: " + (totalRecordCount - spilledRecordCount) ); // Do not check in
//      }

      CopierHolder.BatchMerger merger = copierHolder.startMerge(batchGroupList, targetRecordCount);

      // Identify the next directory from the round-robin list to
      // the file created from this round of spilling. The directory must already
      // exist and must have sufficient space for the output file.

      String spillDir = dirs.next();
      Path currSpillPath = new Path(Joiner.on("/").join(spillDir, fileName));
      currSpillDirs.add(currSpillPath);
      String outputFile = Joiner.on("/").join(currSpillPath, spillCount++);
      try {
          fs.deleteOnExit(currSpillPath);
      } catch (IOException e) {
          // since this is meant to be used in a batches's spilling, we don't propagate the exception
          logger.warn("Unable to mark spill directory " + currSpillPath + " for deleting on exit", e);
      }

      // Merge the selected set of matches and write them to the
      // spill file. After each write, we release the memory associated
      // with the just-written batch.

      stats.setLongStat(Metric.SPILL_COUNT, spillCount);
      BatchGroup.SpilledBatchGroup newGroup = new BatchGroup.SpilledBatchGroup(fs, outputFile, oContext);
      try (AutoCloseable a = AutoCloseables.all(batchGroupList)) {

        // The copier will merge records from the buffered batches into
        // the outputContainer up to targetRecordCount number of rows.
        // The actual count may be less if fewer records are available.

        logger.info("Merging and spilling to {}", outputFile);
//        if ( enableDebug ) {
//          System.out.println( "Copier allocator at start: " + copierAllocator.getAllocatedMemory() ); // Do not check in
//        }
        int spilledCount = 0;
        int count;
        while ((count = merger.next()) > 0) {
          if ( enableDebug ) {
            System.out.println( String.format("Copied records: %d", count ) ); // Do not check in
          }

          // Add a new batch of records (given by outputContainer) to the spill
          // file, opening the file if not yet open, and creating the target
          // directory if it does not yet exist.
          //
          // note that addBatch also clears the outputContainer
          newGroup.addBatch(merger.getOutput());
          spilledCount += count;
        }
        injector.injectChecked(context.getExecutionControls(), INTERRUPTION_WHILE_SPILLING, IOException.class);
        newGroup.closeOutputStream();
        return new SpillEvent( newGroup, outputFile, spilledCount );
      } catch (Throwable e) {
        // we only need to cleanup newGroup if spill failed
        try {
          AutoCloseables.close(e, newGroup);
        } catch (Throwable t) { /* close() may hit the same IO issue; just ignore */ }
        throw UserException.resourceError(e)
          .message("External Sort encountered an error while spilling to disk")
                .addContext(e.getMessage() /* more detail */)
          .build(logger);
      } finally {
        merger.clear( );
      }
    }

    /**
     * Compute the number of records to include in each merge batch, used either
     * for spilling or for returning to the downstream operator. The number is
     * the lesser of 32K or the number (based on the estimated row size) that
     * will fit in the memory allocated to the copier. In any event, a batch
     * must have at least 1 record, even if that record is huge.
     *
     * @return the number of records to include in each merge record batch
     */

    private int getTargetRecordCount(int estimatedRecordSize) {
      int targetRecordCount = Math.max(1, COPIER_BATCH_MEM_LIMIT / estimatedRecordSize);
      targetRecordCount = Math.min( targetRecordCount, Short.MAX_VALUE );
      logger.debug("getTargetRecordCount: estimated record size = {}, target record count = {}", estimatedRecordSize, targetRecordCount);
      // Debug only, do not check in
      if ( enableDebug ) {
        System.out.println(String.format("getTargetRecordCount: estimated record size = %s, target record count = %s", estimatedRecordSize, targetRecordCount));
      }
      return targetRecordCount;
    }

    public void close() {
      for ( Path path : currSpillDirs ) {
        try {
            if (fs != null && path != null && fs.exists(path)) {
                if (fs.delete(path, true)) {
                    fs.cancelDeleteOnExit(path);
                }
            }
        } catch (IOException e) {
            // since this is meant to be used in a batches's cleanup, we don't propagate the exception
            logger.warn("Unable to delete spill directory " + path,  e);
        }
      }
    }

  }

  public class SpillEvent {
    public int recordCount;
    public BatchGroup.SpilledBatchGroup batchGroup;
    public String fileName;

    public SpillEvent(SpilledBatchGroup batchGroup, String fileName,
        int recordCount) {
      this.batchGroup = batchGroup;
      this.fileName = fileName;
      this.recordCount = recordCount;
    }
  }

  public interface SortResults {

    IterOutcome merge() throws SchemaChangeException, ClassTransformationException, IOException;
    IterOutcome next();
    void close();
  }

  public static class InMemoryMerge implements SortResults {

    private final ExternalSortBatch esb;
    private SortRecordBatchBuilder builder;
    private MSorter mSorter;
    private LinkedList<BatchGroup.InputBatchGroup> batchGroups;
    protected final FragmentContext context;
    private SelectionVector4 sv4;

    public InMemoryMerge( ExternalSortBatch esb, LinkedList<BatchGroup.InputBatchGroup> batchGroups ) {
      this.esb = esb;
      this.batchGroups = batchGroups;
      context = esb.context;
    }

    @Override
    public IterOutcome merge() throws SchemaChangeException, ClassTransformationException, IOException {
      if (builder != null) {
        builder.clear();
        builder.close();
      }
      builder = new SortRecordBatchBuilder(esb.oAllocator);

      while ( ! batchGroups.isEmpty() ) {
        BatchGroup.InputBatchGroup group = batchGroups.pollLast();
        RecordBatchData rbd = new RecordBatchData(group.getContainer(), esb.oAllocator);
        rbd.setSv2(group.getSv2());
        builder.add(rbd);
      }

      builder.build(context, esb.container);
      sv4 = builder.getSv4();
      mSorter = createNewMSorter();
      mSorter.setup(context, esb.oAllocator, sv4, esb.container);

      // For testing memory-leak purpose, inject exception after mSorter finishes setup
      injector.injectUnchecked(context.getExecutionControls(), INTERRUPTION_AFTER_SETUP);
      mSorter.sort(esb.container);

      // sort may have prematurely exited due to should continue returning false.
      if (!context.shouldContinue()) {
        return IterOutcome.STOP;
      }

      // For testing memory-leak purpose, inject exception after mSorter finishes sorting
      injector.injectUnchecked(context.getExecutionControls(), INTERRUPTION_AFTER_SORT);
      esb.sv4 = mSorter.getSV4();

      esb.container.buildSchema(SelectionVectorMode.FOUR_BYTE);
      return IterOutcome.OK_NEW_SCHEMA;
    }

    @Override
    public IterOutcome next() {
      return (sv4.next()) ? IterOutcome.OK : IterOutcome.NONE;
    }

    private MSorter createNewMSorter() throws ClassTransformationException, IOException, SchemaChangeException {
      return createNewMSorter(context, esb.popConfig.getOrderings(), esb, MAIN_MAPPING, LEFT_MAPPING, RIGHT_MAPPING);
    }

    private MSorter createNewMSorter(FragmentContext context, List<Ordering> orderings, VectorAccessible batch, MappingSet mainMapping, MappingSet leftMapping, MappingSet rightMapping)
            throws ClassTransformationException, IOException, SchemaChangeException{
      CodeGenerator<MSorter> cg = CodeGenerator.get(MSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
      ClassGenerator<MSorter> g = cg.getRoot();
      g.setMappingSet(mainMapping);

      for (Ordering od : orderings) {
        // first, we rewrite the evaluation stack for each side of the comparison.
        ErrorCollector collector = new ErrorCollectorImpl();
        final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
        if (collector.hasErrors()) {
          throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
        }
        g.setMappingSet(leftMapping);
        HoldingContainer left = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
        g.setMappingSet(rightMapping);
        HoldingContainer right = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
        g.setMappingSet(mainMapping);

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

      return context.getImplementationClass(cg);
    }

    @Override
    public void close() {
      if (builder != null) {
        builder.clear();
        builder.close();
      }
      if (mSorter != null) {
        mSorter.clear();
      }
    }
  }

  public class SpilledMerge implements SortResults {

    private CopierHolder.BatchMerger merger;
    private LinkedList<BatchGroup.InputBatchGroup> batchGroups;
    private int estimatedRecordSize;

    public SpilledMerge( LinkedList<BatchGroup.InputBatchGroup> batchGroups, int estimatedRecordSize ) {
      this.batchGroups = batchGroups;
      this.estimatedRecordSize = estimatedRecordSize;
    }

    @Override
    public IterOutcome merge() throws SchemaChangeException {
      final SpillEvent spill = spillSet.mergeAndSpill(batchGroups, estimatedRecordSize);
      if (spill != null && spill.recordCount > 0) {
        spilledBatchGroups.add(spill.batchGroup);
      }
      List<BatchGroup> allBatches = new LinkedList<>( );
      allBatches.addAll(batchGroups);
      batchGroups.clear();
      allBatches.addAll(spilledBatchGroups);
      batchGroups = null;
      spilledBatchGroups = null; // no need to cleanup spilledBatchGroups, all it's batches are in batchGroups now

      // The number of records to return per batch to the downstream
      // operator.

      int targetRecordCount = spillSet.getTargetRecordCount(estimatedRecordSize);
      logger.info("Starting to merge. {} batch groups. Current allocated memory: {}", allBatches.size(), oAllocator.getAllocatedMemory());
      merger = copierHolder.startFinalMerge(allBatches, container, targetRecordCount);

      merger.next();
      return IterOutcome.OK_NEW_SCHEMA;
    }

    @Override
    public IterOutcome next() {
      return nextBatch() == 0 ? IterOutcome.NONE : IterOutcome.OK;
    }

    private int nextBatch() {
      Stopwatch w = Stopwatch.createStarted();
      int count = merger.next( );
      if (count > 0) {
        long t = w.elapsed(TimeUnit.MICROSECONDS);
        logger.debug("Took {} us to merge {} records", t, count);
        container.setRecordCount(count);
      } else {
        logger.debug("copier returned 0 records");
      }
      return count;
    }

    @Override
    public void close() { }
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
      if (spilledBatchGroups != null) {
        closeBatchGroups(spilledBatchGroups);
        spilledBatchGroups = null;
      }
    } finally {
      resultsIterator.close( );
      if (sv4 != null) {
        sv4.clear();
      }

      try {
        copierHolder.close( );
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
//        copierAllocator.close();
        super.close();
        spillSet.close( );
      }
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
    case START:
      return buildResults( );
    case RESULTS:
      return nextResults( );
    case END:
      return IterOutcome.NONE;
    default:
      throw new IllegalStateException( "Unexpected sort state: " + sortState );
    }
  }

  /**
   * Build and sort the result set.
   *
   * @return outcome of the build-and-sort phase
   */

  /**
   * Build and sort the result set. Errors are handled here, ensuring
   * proper cleanup.
   *
   * @return outcome of the build-and-sort phase
   */

  private IterOutcome buildResults( ) {

    container.clear();
    SortBuilder sortBuilder = new SortBuilder( this, context );
    try {
      try {
        IterOutcome outcome = sortBuilder.build( );
        if ( outcome == IterOutcome.NONE ) {
          sortState = SortState.END;
        } else {
          sortState = SortState.RESULTS;
          resultsIterator = sortBuilder.getResults();
        }
        return outcome;
      }
      catch ( Exception e ) {
        sortState = SortState.ERROR;
        sortBuilder.close( );
        throw e;
      }
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

  /**
   * Return the next batch to the downstream operator.
   *
   * @return
   */

  private IterOutcome nextResults( ) {
    IterOutcome outcome = resultsIterator.next( );
    if ( outcome != IterOutcome.OK ) {
      sortState = SortState.END;
    }
    return outcome;
  }

  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("A sort batch is not writable.");
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  void generateComparisons(ClassGenerator<?> g, VectorAccessible batch) throws SchemaChangeException {
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

  public BufferAllocator getAllocator() {
    return oAllocator;
  }
}
