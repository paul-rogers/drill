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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
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
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
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
import org.apache.drill.exec.vector.CopyUtil;
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

  private static final GeneratorMapping COPIER_MAPPING = new GeneratorMapping("doSetup", "doCopy", null, null);
  private static final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private static final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private static final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private static final MappingSet COPIER_MAPPING_SET = new MappingSet(COPIER_MAPPING, COPIER_MAPPING);

  private final int SPILL_BATCH_GROUP_SIZE;
  private final int SPILL_THRESHOLD;
  private final Iterator<String> dirs;
  private final RecordBatch incoming;

  /**
   * Memory allocator for this operator itself. Primarily used to create
   * intermediate vectors used for merging the incoming batches.
   */

  private final BufferAllocator oAllocator;

  /**
   * Allocator used for the "copier" (actually the merge) operation that
   * takes a set of merged batches and creates new, sorted and merged
   * output batches for spilling to disk.
   */

  private final BufferAllocator copierAllocator;

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
  private SortRecordBatchBuilder builder;
  private MSorter mSorter;
  /**
   * A single PriorityQueueCopier instance is used for 2 purposes:
   * 1. Merge sorted batches before spilling
   * 2. Merge sorted batches when all incoming data fits in memory
   */
  private PriorityQueueCopier copier;
  private LinkedList<BatchGroup.InputBatchGroup> batchGroups = Lists.newLinkedList();
  private LinkedList<BatchGroup.SpilledBatchGroup> spilledBatchGroups = Lists.newLinkedList();
  private SelectionVector4 sv4;

  /**
   * The HDFS file system (for local directories, HDFS storage, etc.) used to
   * create the temporary spill files. Allows spill files to be either on local
   * disk, or in a DFS. (The admin can choose to put spill files in DFS when
   * nodes provide insufficient local disk space)
   */

  private FileSystem fs;

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
  private int batchesSinceLastSpill = 0;

  /**
   * The number of records to return per batch to the downstream
   * operator.
   */

  private int targetRecordCount;
  private int firstSpillBatchCount = 0;
  private int peakNumBatches = -1;
  private int totalRecordCount = 0;
  private int totalBatches = 0; // total number of batches received so far

  /**
   * Estimate of the average record width (including overhead) of all
   * records currently stored in memory.
   */

  private int revisedRecordWidthEstimate;

  /**
   * The cumulative count of the records spilled to disk thus
   * far. Used to calculate the in-memory record count when
   * estimating the average row width.
   */

  private int spilledRecordCount;

  /**
   * Target size, in bytes, of each batch written to disk when spilling,
   * and each batch returned to the downstream operator. Since the copier
   * works in terms of records, the code estimates row width, then
   * divides the memory limit by row width to get the target batch
   * sizes (in rows.) The target size is the maximum memory allocated
   * to the copier, though for smaller rows the memory used is often
   * much less.
   */

  private static final int COPIER_BATCH_MEM_LIMIT = (int) PriorityQueueCopier.MAX_ALLOCATION;

  public static final String INTERRUPTION_AFTER_SORT = "after-sort";
  public static final String INTERRUPTION_AFTER_SETUP = "after-setup";
  public static final String INTERRUPTION_WHILE_SPILLING = "spilling";

  private boolean enableDebug = false; // Temporary, do not check in

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
    Configuration conf = new Configuration();
    conf.set("fs.default.name", config.getString(ExecConstants.EXTERNAL_SORT_SPILL_FILESYSTEM));
    try {
      this.fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    SPILL_BATCH_GROUP_SIZE = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_GROUP_SIZE);
    SPILL_THRESHOLD = config.getInt(ExecConstants.EXTERNAL_SORT_SPILL_THRESHOLD);
    dirs = Iterators.cycle(config.getStringList(ExecConstants.EXTERNAL_SORT_SPILL_DIRS));
    oAllocator = oContext.getAllocator();
    copierAllocator = oAllocator.newChildAllocator(oAllocator.getName() + ":copier",
        PriorityQueueCopier.INITIAL_ALLOCATION, PriorityQueueCopier.MAX_ALLOCATION);
    FragmentHandle handle = context.getHandle();
    fileName = String.format("%s_majorfragment%s_minorfragment%s_operator%s", QueryIdHelper.getQueryId(handle.getQueryId()),
        handle.getMajorFragmentId(), handle.getMinorFragmentId(), popConfig.getOperatorId());
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
      if (builder != null) {
        builder.clear();
        builder.close();
      }
      if (sv4 != null) {
        sv4.clear();
      }

      try {
        if (copier != null) {
          copier.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        copierAllocator.close();
        super.close();

        if (mSorter != null) {
          mSorter.clear();
        }
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

    // Existence of schema tells us if this the first request.

    if (schema != null) {
      return nextResults( );
    } else {
      return buildResults( );
    }
  }

  private IterOutcome nextResults( ) {
    if (spillCount == 0) {
      return (getSelectionVector4().next()) ? IterOutcome.OK : IterOutcome.NONE;
    } else {
      return nextSpilledResults( );
    }
  }

  private IterOutcome nextSpilledResults() {
    Stopwatch w = Stopwatch.createStarted();
    int count = copier.next(targetRecordCount);
    if (count > 0) {
      long t = w.elapsed(TimeUnit.MICROSECONDS);
      logger.debug("Took {} us to merge {} records", t, count);
      container.setRecordCount(count);
      return IterOutcome.OK;
    } else {
      logger.debug("copier returned 0 records");
      return IterOutcome.NONE;
    }
  }

  /**
   * Build and sort the result set. Errors are handled here, ensuring
   * proper cleanup.
   *
   * @return outcome of the build-and-sort phase
   */

  private IterOutcome buildResults( ) {

    try{
      return loadResults( );
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
   * Load the results and sort them. May bail out early if an exceptional
   * condition is passed up from the input batch.
   *
   * @return
   * @throws SchemaChangeException
   * @throws ClassTransformationException
   * @throws IOException
   * @throws InterruptedException
   */

  private IterOutcome loadResults() throws SchemaChangeException, ClassTransformationException, IOException, InterruptedException {
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
      return IterOutcome.NONE; }

    // Do the actual sort. The sort can be done entirely in memory if
    // the results fit; else we have to do a disk-based merge of
    // pre-sorted spilled batches.

    if (spillCount == 0) {
      return mergeInMemory( );
    } else { // some batches were spilled
      return mergeSpilled( );
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

  private void setupSchema(IterOutcome upstream) throws SchemaChangeException {
    // First batch: we won't have a schema.

    if (schema == null) {
      schema = incoming.getSchema();

    // Subsequent batches, nothing to do if same schema.

    } else if ( upstream == IterOutcome.OK ) {
      return;

    // only change in the case that the schema truly changes.  Artificial schema changes are ignored.
    } else if (incoming.getSchema().equals(schema)) {
      return;
    } else if (unionTypeEnabled) {
        schema = SchemaUtil.mergeSchemas(schema, incoming.getSchema());
        sorter = null;
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
    sorter.setup(context, sv2, convertedBatch);
    sorter.sort(sv2);
    RecordBatchData rbd = new RecordBatchData(convertedBatch, oAllocator);
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

    long allocMem = oAllocator.getAllocatedMemory();

    // The maximum memory this operator can use. It is either the
    // limit set on the allocator or on the operator, whichever is
    // less.

    long limitMem = Math.min( popConfig.getMaxAllocation(), oAllocator.getLimit() );

    long availableMem = limitMem - allocMem;

    // If we haven't spilled so far...

    if (spillCount == 0) {

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
   * times.) Then, spill the batch groups accumulated since the last spill
   *
   * @throws SchemaChangeException which should never actually happen as we
   * caught schema changes while receiving the batches earlier
   */

  private void doSpill() throws SchemaChangeException {

    // Estimate the row width based on the total memory consumed
    // by the in-memory batches, divided by the number or rows
    // in those batches. This estimate "amortizes" overhead memory
    // across rows so we get an accurate memory use estimate later.

    long memUsed = oAllocator.getAllocatedMemory();
    int inMemRecordCount = totalRecordCount - spilledRecordCount;
    revisedRecordWidthEstimate = (int) (memUsed / inMemRecordCount);
    if ( enableDebug ) {
      System.out.println( "Revised record width estimate: " + revisedRecordWidthEstimate );
    }

    if (firstSpillBatchCount == 0) {
      firstSpillBatchCount = batchGroups.size();
    }

    if (spilledBatchGroups.size() > firstSpillBatchCount / 2) {
      logger.info("Merging spills: threshhold=" + (firstSpillBatchCount / 2) +
                  ", spilled count: " + spilledBatchGroups.size());
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

  private IterOutcome mergeInMemory() throws SchemaChangeException, ClassTransformationException, IOException {

    if (builder != null) {
      builder.clear();
      builder.close();
    }
    builder = new SortRecordBatchBuilder(oAllocator);

    for (BatchGroup.InputBatchGroup group : batchGroups) {
      RecordBatchData rbd = new RecordBatchData(group.getContainer(), oAllocator);
      rbd.setSv2(group.getSv2());
      builder.add(rbd);
    }

    builder.build(context, container);
    sv4 = builder.getSv4();
    mSorter = createNewMSorter();
    mSorter.setup(context, oAllocator, getSelectionVector4(), this.container);

    // For testing memory-leak purpose, inject exception after mSorter finishes setup
    injector.injectUnchecked(context.getExecutionControls(), INTERRUPTION_AFTER_SETUP);
    mSorter.sort(this.container);

    // sort may have prematurely exited due to should continue returning false.
    if (!context.shouldContinue()) {
      return IterOutcome.STOP;
    }

    // For testing memory-leak purpose, inject exception after mSorter finishes sorting
    injector.injectUnchecked(context.getExecutionControls(), INTERRUPTION_AFTER_SORT);
    sv4 = mSorter.getSV4();

    container.buildSchema(SelectionVectorMode.FOUR_BYTE);
    return IterOutcome.OK_NEW_SCHEMA;
  }

  private IterOutcome mergeSpilled() throws SchemaChangeException {
    final BatchGroup.SpilledBatchGroup merged = mergeAndSpill(batchGroups);
    if (merged != null) {
      spilledBatchGroups.add(merged);
    }
    List<BatchGroup> allBatches = new LinkedList<>( );
    allBatches.addAll(batchGroups);
    allBatches.addAll(spilledBatchGroups);
    batchGroups = null;
    spilledBatchGroups = null; // no need to cleanup spilledBatchGroups, all it's batches are in batchGroups now

    logger.info("Starting to merge. {} batch groups. Current allocated memory: {}", allBatches.size(), oAllocator.getAllocatedMemory());
    VectorContainer hyperBatch = constructHyperBatch(allBatches);
    createCopier(hyperBatch, allBatches, container, oAllocator);

    targetRecordCount = getTargetRecordCount();
    int count = copier.next(targetRecordCount);
    container.buildSchema(SelectionVectorMode.NONE);
    container.setRecordCount(count);
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
    logger.debug("Copier allocator current allocation {}", copierAllocator.getAllocatedMemory());
    logger.debug("mergeAndSpill: starting total size in memory = {}", oAllocator.getAllocatedMemory());
    if ( enableDebug ) {
      System.out.println( // Debugging only, do not check in
          "Before spilling, buffered batch count: " + batchGroups.size( ) );
    }
    VectorContainer outputContainer = new VectorContainer();

    // Determine the number of batches to spill. This is set by the
    // SPILL_BATCH_GROUP_SIZE parameter, but adjusted to not be fewer than
    // half of the batches or more than all of them.

    int batchCount = batchGroups.size();
    int spillBatchCount = Math.min( Math.max( SPILL_BATCH_GROUP_SIZE, batchCount / 2 ), batchCount );

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

    // Determine the number of records to spill per merge step. The goal is to
    // spill batches of either 32K records, or as may records as fit into the
    // amount of memory dedicated to the copier, whichever is less.

    int targetRecordCount = getTargetRecordCount( );
//    if ( enableDebug ) { // Do not check in
//      System.out.println( "Spill - record width estimate: " + estimatedRecordSize ); // Do not check in
//      System.out.println( "Spill - target record count: " + targetRecordCount ); // Do not check in
//      System.out.println( "Spill - actual record count: " + (totalRecordCount - spilledRecordCount) ); // Do not check in
//    }

    // We've gathered a set of batches, each of which has been sorted. The batches
    // may have passed through a filter and thus may have "holes" where rows have
    // been filtered out. We will spill records in blocks of targetRecordCount.
    // To prepare, copy that many records into an outputContainer as a set of
    // contiguous values in new vectors. The result is a single batch with
    // vectors that combine a collection of input batches up to the
    // given threshold.
    //
    // Input (selection vector, data vector):
    // [3 7 4 8 0 6 1] [5 3 6 8 2 0]
    // [eh_ad_ibf]     [r_qm_kn_p]
    //
    // Output (assuming blocks of 5 records, data vectors only):
    // [abcde] [fhikm] [npqr]
    //
    // The copying operation does a merge as well: copying
    // values from the sources in ordered fashion.
    //
    // Input:  [aceg] [bdfh]
    // Output: [abcdefgh]
    //
    // Here we bind the copier to the batchGroupList of sorted, buffered batches
    // to be merged. We bind the copier output to outputContainer: the copier will write its
    // merged "batches" of records to that container.

    VectorContainer hyperBatch = constructHyperBatch(batchGroupList);
    createCopier(hyperBatch, batchGroupList, outputContainer, copierAllocator);

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
      if ( enableDebug ) {
        System.out.println( "Copier allocator at start: " + copierAllocator.getAllocatedMemory() ); // Do not check in
      }
      int count;
      while ((count = copier.next(targetRecordCount)) > 0) {
        if ( enableDebug ) {
          System.out.println( String.format("Copier allocator after copy: %d, Records: %d",
              copierAllocator.getAllocatedMemory(), count ) ); // Do not check in
        }

        // Identify the schema to be used in the output container. (Since
        // all merged batches have the same schema, the schema we identify
        // here should be the same as that which we already had.

        outputContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);

        // The copier does not set the record count in the output
        // container, so do that here.

        outputContainer.setRecordCount(count);

        // Add a new batch of records (given by outputContainer) to the spill
        // file, opening the file if not yet open, and creating the target
        // directory if it does not yet exist.
        //
        // note that addBatch also clears the outputContainer
        newGroup.addBatch(outputContainer);
        spilledRecordCount += count;
      }
      injector.injectChecked(context.getExecutionControls(), INTERRUPTION_WHILE_SPILLING, IOException.class);
      newGroup.closeOutputStream();
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
      hyperBatch.clear();
    }
    logger.debug("mergeAndSpill: final total size in memory = {}", oAllocator.getAllocatedMemory());
    logger.info("Completed spilling to {}", outputFile);
    if ( enableDebug ) {
      System.out.println( // Debugging only, do not check in
          "After spilling, buffered batch count: " + batchGroups.size( ) );
    }
    return newGroup;
  }

  private int getTargetRecordCount() {
    int estimatedRecordSize = estimateRecordSize( );
    int targetRecordCount = Math.max(1, COPIER_BATCH_MEM_LIMIT / estimatedRecordSize);
    targetRecordCount = Math.min( targetRecordCount, Short.MAX_VALUE );
    logger.debug("mergeAndSpill: estimated record size = {}, target record count = {}", estimatedRecordSize, targetRecordCount);
    // Debug only, do not check in
    if ( enableDebug ) {
      System.out.println(String.format("mergeAndSpill: estimated record size = %s, target record count = %s", estimatedRecordSize, targetRecordCount));
    }
    return targetRecordCount;
  }

  /**
   * Get an estimate of the record size for the current set of batches.
   * Used to calculate memory needs. Required because generated code
   * works with records, not bytes, when setting limits such as when
   * creating a new merged batch.
   *
   * @return the estimated record size, in bytes
   */

  private int estimateRecordSize( ) {

    // Whether to use the "old school" (prior to Drill 1.10) estimate
    // or the "revised" estimate.

    boolean oldSchool = false;
    if ( oldSchool ) {

      // Estimate the size of each record in all the batches. Start with assumptions
      // about column width based on type (not on actual data), then sum up the
      // assumed column widths. This number is then used to limit the number of
      // records written to stay under the copier memory limit. (Needless to say,
      // this estimate causes the copier to use more or less memory than the limit
      // depending on how far actual record widths are from the estimated width.)

      int estimatedRecordSize = 0;
      for (VectorWrapper<?> w : batchGroups.get(0)) {
        try {
          estimatedRecordSize += TypeHelper.getSize(w.getField().getType());
        } catch (UnsupportedOperationException e) {
          estimatedRecordSize += 50;
        }
      }
      return estimatedRecordSize;
    } else {

      // Return the revised estimate based on actual data consumed.
      // See revisedRecordWidthEstimate for details.

      return revisedRecordWidthEstimate;
    }
  }

  /**
   * Prepare a copier which will write a collection of vectors to disk. The copier
   * uses generated code to to the actual writes. If the copier has not yet been
   * created, generated code and create it. If it has been created, close it and
   * prepare it for a new collection of batches.
   *
   * @param batch the (hyper) batch of vectors to be copied
   * @param batchGroupList same batches as above, but represented as a list
   * of individual batches
   * @param outputContainer the container into which to copy the batches
   * @param allocator allocator to use to allocate memory in the operation
   * @throws SchemaChangeException thrown, but should never occur because
   * schema changes were caught earlier as the batches were received
   */

  private void createCopier(VectorAccessible batch, List<BatchGroup> batchGroupList, VectorContainer outputContainer, BufferAllocator allocator) throws SchemaChangeException {
    if (copier != null) {
      try {
        copier.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {

      // Generate the copier code and obtain the resulting class

      CodeGenerator<PriorityQueueCopier> cg = CodeGenerator.get(PriorityQueueCopier.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
      ClassGenerator<PriorityQueueCopier> g = cg.getRoot();

      generateComparisons(g, batch);

      g.setMappingSet(COPIER_MAPPING_SET);
      CopyUtil.generateCopies(g, batch, true);
      g.setMappingSet(MAIN_MAPPING);
      try {
        copier = context.getImplementationClass(cg);
      } catch (ClassTransformationException | IOException e) {
        throw new RuntimeException(e);
      }
    }

    // Initialize the value vectors for the output container using the
    // allocator provided

    for (VectorWrapper<?> i : batch) {
      ValueVector v = TypeHelper.getNewVector(i.getField(), allocator);
      outputContainer.add(v);
    }
    copier.setup(context, allocator, batch, batchGroupList, outputContainer);
  }

  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("A sort batch is not writable.");
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  private SelectionVector2 newSV2() throws OutOfMemoryException, InterruptedException {
    @SuppressWarnings("resource")
    SelectionVector2 sv2 = new SelectionVector2(oAllocator);
    if (!sv2.allocateNewSafe(incoming.getRecordCount())) {
      try {
        final BatchGroup.SpilledBatchGroup merged = mergeAndSpill(batchGroups);
        if (merged != null) {
          spilledBatchGroups.add(merged);
        } else {
          throw UserException.memoryError("Unable to allocate sv2 for %d records, and not enough batchGroups to spill.",
              incoming.getRecordCount())
            .addContext("batchGroups.size", batchGroups.size())
            .addContext("spilledBatchGroups.size", spilledBatchGroups.size())
            .addContext("allocated memory", oAllocator.getAllocatedMemory())
            .addContext("allocator limit", oAllocator.getLimit())
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

  /**
   * Construct a vector container that holds a list of batches, each represented as an
   * array of vectors. The entire collection of vectors has a common schema.
   * <p>
   * To build the collection, we go through the current schema (which has been
   * devised to be common for all batches.) For each field in the schema, we create
   * an array of vectors. To create the elements, we iterate over all the incoming
   * batches and search for the vector that matches the current column.
   * <p>
   * Finally, we build a new schema for the combined container. That new schema must,
   * because of the way the container was created, match the current schema.
   *
   * @param batchGroupList list of batches to combine
   * @return a container where each column is represented as an array of vectors
   * (hence the "hyper" in the method name)
   */

  private VectorContainer constructHyperBatch(List<BatchGroup> batchGroupList) {
    VectorContainer cont = new VectorContainer();
    for (MaterializedField field : schema) {
      ValueVector[] vectors = new ValueVector[batchGroupList.size()];
      int i = 0;
      for (BatchGroup group : batchGroupList) {
        vectors[i++] = group.getValueAccessorById(
            field.getValueClass(),
            group.getValueVectorId(SchemaPath.getSimplePath(field.getPath())).getFieldIds())
            .getValueVector();
      }
      cont.add(vectors);
    }
    cont.buildSchema(BatchSchema.SelectionVectorMode.FOUR_BYTE);
    return cont;
  }

  private MSorter createNewMSorter() throws ClassTransformationException, IOException, SchemaChangeException {
    return createNewMSorter(this.context, this.popConfig.getOrderings(), this, MAIN_MAPPING, LEFT_MAPPING, RIGHT_MAPPING);
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

  public SingleBatchSorter createNewSorter(FragmentContext context, VectorAccessible batch)
          throws ClassTransformationException, IOException, SchemaChangeException{
    CodeGenerator<SingleBatchSorter> cg = CodeGenerator.get(SingleBatchSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    ClassGenerator<SingleBatchSorter> g = cg.getRoot();

    generateComparisons(g, batch);

    return context.getImplementationClass(cg);
  }

  private void generateComparisons(ClassGenerator<?> g, VectorAccessible batch) throws SchemaChangeException {
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
}
