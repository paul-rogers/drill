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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.SortResults;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Stopwatch;

/**
 * Manages a {@link PriorityQueueCopier} instance produced from code generation.
 * Provides a wrapper around a copier "session" to simplify reading batches
 * from the copier.
 */

public class CopierHolder {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopierHolder.class);

  private PriorityQueueCopier copier;

  private final FragmentContext context;
  private final BufferAllocator allocator;
  private OperatorCodeGenerator opCodeGen;

  public CopierHolder(FragmentContext context, BufferAllocator allocator, OperatorCodeGenerator opCodeGen) {
    this.context = context;
    this.allocator = allocator;
    this.opCodeGen = opCodeGen;
  }

  /**
   * Start a merge operation using a temporary vector container. Used for
   * intermediate merges.
   *
   * @param schema
   * @param batchGroupList
   * @param targetRecordCount
   * @return
   */

  public CopierHolder.BatchMerger startMerge(BatchSchema schema, List<? extends BatchGroup> batchGroupList, int targetRecordCount) {
    return new BatchMerger(this, schema, batchGroupList, targetRecordCount);
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
  public CopierHolder.BatchMerger startFinalMerge(BatchSchema schema, List<? extends BatchGroup> batchGroupList, VectorContainer outputContainer, int targetRecordCount) {
    return new BatchMerger(this, schema, batchGroupList, outputContainer, targetRecordCount);
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
   */

  @SuppressWarnings("unchecked")
  private void createCopier(VectorAccessible batch, List<? extends BatchGroup> batchGroupList, VectorContainer outputContainer) {
    if (copier != null) {
      opCodeGen.closeCopier();
    } else {
      copier = opCodeGen.getCopier(batch);
    }

    // Initialize the value vectors for the output container using the
    // allocator provided

    for (VectorWrapper<?> i : batch) {
      ValueVector v = TypeHelper.getNewVector(i.getField(), allocator);
      outputContainer.add(v);
    }
    try {
      copier.setup(context, allocator, batch, (List<BatchGroup>) batchGroupList, outputContainer);
    } catch (SchemaChangeException e) {
      throw UserException.unsupportedError(e)
            .message("Unexpected schema change - likely code error.")
            .build(logger);
    }
  }

  public void close() {
    opCodeGen.closeCopier();
    copier = null;
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
   * Input (selection vector, data vector):<pre>
   * [3 7 4 8 0 6 1] [5 3 6 8 2 0]
   * [eh_ad_ibf]     [r_qm_kn_p]</pre>
   * <p>
   * Output (assuming blocks of 5 records, data vectors only):<pre>
   * [abcde] [fhikm] [npqr]</pre>
   * <p>
   * The copying operation does a merge as well: copying
   * values from the sources in ordered fashion.
   * <pre>
   * Input:  [aceg] [bdfh]
   * Output: [abcdefgh]</pre>
   * <p>
   * Here we bind the copier to the batchGroupList of sorted, buffered batches
   * to be merged. We bind the copier output to outputContainer: the copier will write its
   * merged "batches" of records to that container.
   * <p>
   * Calls to the {@link #next()} method sequentially return merged batches
   * of the desired row count.
    */

  public static class BatchMerger implements SortResults {

    private CopierHolder holder;
    private VectorContainer hyperBatch;
    private VectorContainer outputContainer;
    private int targetRecordCount;
    private int copyCount;
    private int batchCount;

    /**
     * Creates a merger with an temporary output container.
     *
     * @param holder
     * @param batchGroupList
     * @param targetRecordCount
     */
    private BatchMerger(CopierHolder holder, BatchSchema schema, List<? extends BatchGroup> batchGroupList, int targetRecordCount) {
      this(holder, schema, batchGroupList, new VectorContainer(), targetRecordCount);
    }

    /**
     * Creates a merger with the specified output container
     *
     * @param holder
     * @param batchGroupList
     * @param outputContainer
     * @param targetRecordCount
     */
    private BatchMerger(CopierHolder holder, BatchSchema schema, List<? extends BatchGroup> batchGroupList, VectorContainer outputContainer, int targetRecordCount) {
      this.holder = holder;
      hyperBatch = constructHyperBatch(schema, batchGroupList);
      copyCount = 0;
      this.targetRecordCount = targetRecordCount;
      this.outputContainer = outputContainer;
      holder.createCopier(hyperBatch, batchGroupList, outputContainer);
    }

    /**
     * Return the output container.
     *
     * @return
     */
    public VectorContainer getOutput() {
      return outputContainer;
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
      int count = holder.copier.next(targetRecordCount);
      copyCount += count;
      if (count > 0) {
        long t = w.elapsed(TimeUnit.MICROSECONDS);
        logger.debug("Took {} us to merge {} records", t, count);
      } else {
        logger.debug("copier returned 0 records");
      }
      batchCount++;

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
    public int getRecordCount() {
      return copyCount;
    }

    @Override
    public int getBatchCount() {
      return batchCount;
    }
  }
}
