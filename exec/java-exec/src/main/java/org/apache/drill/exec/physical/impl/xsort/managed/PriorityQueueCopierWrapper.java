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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperExecContext;
import org.apache.drill.exec.physical.impl.xsort.managed.SortImpl.SortResults;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.CopyUtil;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Stopwatch;

/**
 * Manages a {@link PriorityQueueCopier} instance produced from code generation.
 * Provides a wrapper around a copier "session" to simplify reading batches
 * from the copier.
 */

public class PriorityQueueCopierWrapper extends BaseSortWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PriorityQueueCopierWrapper.class);

  private static final GeneratorMapping COPIER_MAPPING = new GeneratorMapping("doSetup", "doCopy", null, null);
  private static final MappingSet COPIER_MAPPING_SET = new MappingSet(COPIER_MAPPING, COPIER_MAPPING);

  /**
   * A single PriorityQueueCopier instance is used for 2 purposes:
   * 1. Merge sorted batches before spilling
   * 2. Merge sorted batches when all incoming data fits in memory
   */

  private PriorityQueueCopierDriver copier;
  private final boolean useGenericCopier;

  public PriorityQueueCopierWrapper(OperExecContext opContext, boolean useGenericCopier) {
    super(opContext);
    this.useGenericCopier = useGenericCopier;
  }

  public PriorityQueueCopierDriver getCopier(VectorAccessible batch) {
    if (copier == null) {
      copier = newCopier(batch);
    }
    return copier;
  }

  /**
   * Generate the priority queue copier. Code is split into a "driver"
   * that has the full algorithm, and the template, which holds only
   * the generated code. (This structure avoids unnecessary copies of
   * the constant algorithm code when using the byte code fixup method
   * of code generation.)
   * <p>
   * Two implementations are available. The "traditional" one that
   * generates code to copy each row, and a "generic copier" version
   * that generates the compare method, but uses non-generated code
   * to do the row copy. The non-generated ("generic") copier is
   * faster for wide rows and appears no slower for narrow rows.
   * <p>
   * If we gain confidence in the generic version, then we can
   * drop support for the generated version.
   *
   * @param batch batch that provides the schema and vectors
   * @return the priority queue copier driver instance
   */

  private PriorityQueueCopierDriver newCopier(VectorAccessible batch) {

    // Generate the copier code and obtain the resulting class
    TemplateClassDefinition<PriorityQueueCopier> template =
        useGenericCopier
            ? PriorityQueueCopier.GENERIC_TEMPLATE_DEFINITION
            : PriorityQueueCopier.TEMPLATE_DEFINITION;
    CodeGenerator<PriorityQueueCopier> cg = CodeGenerator.get(template, context.getFunctionRegistry(), context.getOptionSet());
    ClassGenerator<PriorityQueueCopier> g = cg.getRoot();
    cg.plainJavaCapable(true);
    // Uncomment out this line to debug the generated code.
    cg.saveCodeForDebugging(true);

    generateComparisons(g, batch, logger);

    if (! useGenericCopier) {
      g.setMappingSet(COPIER_MAPPING_SET);
      CopyUtil.generateCopies(g, batch, true);
    }
    g.setMappingSet(MAIN_MAPPING);
    return new PriorityQueueCopierDriver( getInstance(cg, logger) );
  }

  /**
   * Start a merge operation using the specified vector container. Used for
   * the final merge operation.
   *
   * @param schema
   * @param batchGroupList
   * @param outputContainer
   * @param targetRecordCount
   * @return
   */
  public BatchMerger startMerge(BatchSchema schema, List<? extends BatchGroup> batchGroupList, VectorContainer outputContainer, int targetRecordCount) {
    return new BatchMerger(this, schema, batchGroupList, outputContainer, targetRecordCount);
  }

  /**
   * Prepare a copier which will write a collection of vectors to disk. The copier
   * uses generated code to do the actual writes. If the copier has not yet been
   * created, generate code and create it. If it has been created, close it and
   * prepare it for a new collection of batches.
   *
   * @param batch the (hyper) batch of vectors to be copied
   * @param batchGroupList same batches as above, but represented as a list
   * of individual batches
   * @param outputContainer the container into which to copy the batches
   */

  @SuppressWarnings("unchecked")
  private void createCopier(VectorAccessible batch, List<? extends BatchGroup> batchGroupList, VectorContainer outputContainer) {
    copier = getCopier(batch);

    // Initialize the value vectors for the output container

    for (VectorWrapper<?> i : batch) {
      @SuppressWarnings("resource")
      ValueVector v = TypeHelper.getNewVector(i.getField(), context.getAllocator());
      outputContainer.add(v);
    }
    try {
      copier.setup(context.getAllocator(), batch, (List<BatchGroup>) batchGroupList, outputContainer);
    } catch (SchemaChangeException e) {
      throw UserException.unsupportedError(e)
            .message("Unexpected schema change - likely code error.")
            .build(logger);
    }
  }

  public BufferAllocator getAllocator() { return context.getAllocator(); }

  public void close() {
    if (copier == null) {
      return; }
    try {
      copier.close();
      copier = null;
    } catch (IOException e) {
      throw UserException.dataWriteError(e)
            .message("Failure while flushing spilled data")
            .build(logger);
    }
  }

  /**
   * We've gathered a set of batches, each of which has been sorted. The batches
   * may have passed through a filter and thus may have "holes" where rows have
   * been filtered out. We will spill records in blocks of targetRecordCount.
   * To prepare, copy that many records into an outputContainer as a set of
   * contiguous values in new vectors. The result is a single batch with
   * vectors that combine a collection of input batches up to the
   * given threshold.
   * <p>
   * Input. Here the top line is a selection vector of indexes.
   * The second line is a set of batch groups (separated by underscores)
   * with letters indicating individual records:<pre>
   * [3 7 4 8 0 6 1] [5 3 6 8 2 0]
   * [eh_ad_ibf]     [r_qm_kn_p]</pre>
   * <p>
   * Output, assuming blocks of 5 records. The brackets represent
   * batches, the line represents the set of batches copied to the
   * spill file.<pre>
   * [abcde] [fhikm] [npqr]</pre>
   * <p>
   * The copying operation does a merge as well: copying
   * values from the sources in ordered fashion. Consider a different example,
   * we want to merge two input batches to produce a single output batch:
   * <pre>
   * Input:  [aceg] [bdfh]
   * Output: [abcdefgh]</pre>
   * <p>
   * In the above, the input consists of two sorted batches. (In reality,
   * the input batches have an associated selection vector, but that is omitted
   * here and just the sorted values shown.) The output is a single batch
   * with the merged records (indicated by letters) from the two input batches.
   * <p>
   * Here we bind the copier to the batchGroupList of sorted, buffered batches
   * to be merged. We bind the copier output to outputContainer: the copier will write its
   * merged "batches" of records to that container.
   * <p>
   * Calls to the {@link #next()} method sequentially return merged batches
   * of the desired row count.
    */

  public static class BatchMerger implements SortResults, AutoCloseable {

    private PriorityQueueCopierWrapper holder;
    private VectorContainer hyperBatch;
    private VectorContainer outputContainer;
    private int targetRecordCount;
    private int copyCount;
    private int batchCount;
    private long estBatchSize;

    /**
     * Creates a merger with an temporary output container.
     *
     * @param holder the copier that does the work
     * @param schema schema for the input and output batches
     * @param batchGroupList the input batches
     * @param targetRecordCount number of records for each output batch
     */
    private BatchMerger(PriorityQueueCopierWrapper holder, BatchSchema schema, List<? extends BatchGroup> batchGroupList,
                        int targetRecordCount) {
      this(holder, schema, batchGroupList, new VectorContainer(), targetRecordCount);
    }

    /**
     * Creates a merger with the specified output container
     *
     * @param holder the copier that does the work
     * @param schema schema for the input and output batches
     * @param batchGroupList the input batches
     * @param outputContainer merges output batch into the given output container
     * @param targetRecordCount number of records for each output batch
     */
    private BatchMerger(PriorityQueueCopierWrapper holder, BatchSchema schema, List<? extends BatchGroup> batchGroupList,
                        VectorContainer outputContainer, int targetRecordCount) {
      this.holder = holder;
      hyperBatch = constructHyperBatch(schema, batchGroupList);
      copyCount = 0;
      this.targetRecordCount = targetRecordCount;
      this.outputContainer = outputContainer;
      holder.createCopier(hyperBatch, batchGroupList, outputContainer);
    }

    /**
     * Read the next merged batch. The batch holds the specified row count, but
     * may be less if this is the last batch.
     *
     * @return the number of rows in the batch, or 0 if no more batches
     * are available
     */

    @Override
    public boolean next() {
      Stopwatch w = Stopwatch.createStarted();
      long start = holder.getAllocator().getAllocatedMemory();
      int count = holder.copier.next(targetRecordCount);
      copyCount += count;
      if (count > 0) {
        long t = w.elapsed(TimeUnit.MICROSECONDS);
        batchCount++;
        logger.trace("Took {} us to merge {} records", t, count);
        long size = holder.getAllocator().getAllocatedMemory() - start;
        estBatchSize = Math.max(estBatchSize, size);
      } else {
        logger.trace("copier returned 0 records");
      }

      // Identify the schema to be used in the output container. (Since
      // all merged batches have the same schema, the schema we identify
      // here should be the same as that which we already had.

      outputContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);

      // The copier does not set the record count in the output
      // container, so do that here.

      outputContainer.setRecordCount(count);

      return count > 0;
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
     * @param schema schema for the hyper batch
     * @param batchGroupList list of batches to combine
     * @return a container where each column is represented as an array of vectors
     * (hence the "hyper" in the method name)
     */

    private VectorContainer constructHyperBatch(BatchSchema schema, List<? extends BatchGroup> batchGroupList) {
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

    @Override
    public void close() {
      hyperBatch.clear();
      holder.close();
    }

    @Override
    public int getRecordCount() { return copyCount; }

    @Override
    public int getBatchCount() { return batchCount; }

    /**
     * Gets the estimated batch size, in bytes. Use for estimating the memory
     * needed to process the batches that this operator created.
     * @return the size of the largest batch created by this operation,
     * in bytes
     */

    public long getEstBatchSize() { return estBatchSize; }

    @Override
    public SelectionVector4 getSv4() { return null; }

    @Override
    public SelectionVector2 getSv2() { return null; }

    @Override
    public VectorContainer getContainer() { return outputContainer; }
  }
}