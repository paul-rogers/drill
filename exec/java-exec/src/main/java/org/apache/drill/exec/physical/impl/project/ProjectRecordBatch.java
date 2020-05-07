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
package org.apache.drill.exec.physical.impl.project;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SimpleRecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchIOType;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.EMIT;

public class ProjectRecordBatch extends AbstractSingleRecordBatch<Project> {
  private static final Logger logger = LoggerFactory.getLogger(ProjectRecordBatch.class);

  private enum State {

    /**
     * Awaiting the first batch. More precisely, the base class will
     * read the first batch, this operator is waiting to see that
     * first batch.
     */
    FIRST,

    /**
     * The project is a simple mapping of input vectors to output
     * vectors. We only shuffle buffers from input to output,
     * no computation is involved. Allows a "fast path" through
     * the operator.
     */
    TRANSFER,

    /**
     * Operator must do a full project and is ready for the next
     * batch to process.
     */
    READY,

    /**
     * Rows were left over from the current input batch. Process
     * those. Note that remainder batches can be as small as a single
     * row, resulting in inefficiencies.
     */
    REMAINDER,

    /**
     * All rows are processed, saw {@code NONE} from upstream.
     */
    DONE
  }

  protected List<ValueVector> allocationVectors;
  protected List<ComplexWriter> complexWriters;
  protected List<FieldReference> complexFieldReferencesList;
  protected ProjectMemoryManager memoryManager;
  private Projector projector;
  private State state = State.FIRST;
  private int remainderIndex;

  public ProjectRecordBatch(Project pop, RecordBatch incoming, FragmentContext context) {
    super(pop, context, incoming);
  }

  @Override
  public int getRecordCount() {
    return container.hasRecordCount() ? container.getRecordCount() : 0;
  }

  @Override
  protected void cancelIncoming() {
    super.cancelIncoming();
    state = State.DONE;
    incoming.getContainer().zeroVectors();
  }

  @Override
  public IterOutcome innerNext() {
    switch (state) {
      case DONE:
        return IterOutcome.NONE;
      case REMAINDER:
        handleRemainder();
        // Check if we are supposed to return EMIT outcome and have consumed entire batch
        return getFinalOutcome(state == State.REMAINDER);
      default:
        return super.innerNext();
    }
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return container;
  }

  @Override
  protected IterOutcome doWork() {
    if (state == State.DONE) {
      return IterOutcome.NONE;
    }

    if (state == State.TRANSFER) {
      return doTransfers();
    }

    int incomingRecordCount = incoming.getRecordCount();
    logger.trace("doWork(): incoming rc {}, incoming {}, Project {}", incomingRecordCount, incoming, this);
    // Calculate the output row count
    memoryManager.update();

    if (state == State.FIRST && incomingRecordCount == 0) {
      if (complexWriters != null) {
        IterOutcome next = null;
        while (incomingRecordCount == 0) {
          if (getLastKnownOutcome() == EMIT) {
            throw new UnsupportedOperationException("Currently functions producing complex types as output are not " +
                    "supported in project list for subquery between LATERAL and UNNEST. Please re-write the query using this " +
                    "function in the projection list of outermost query.");
          }

          next = next(incoming);
          setLastKnownOutcome(next);
          switch (next) {
            case NONE:
              // Since this is first batch and we already got a NONE, no need to set up the schema
              setValueCount(0);
              state = State.DONE;
              return IterOutcome.NONE;
            case OK_NEW_SCHEMA:
              setupNewSchema();
              break;
            case OK:
            case EMIT:
              break;
            default:
              return next;
          }
          incomingRecordCount = incoming.getRecordCount();
          memoryManager.update();
          logger.trace("doWork():[1] memMgr RC {}, incoming rc {}, incoming {}, Project {}",
                       memoryManager.getOutputRowCount(), incomingRecordCount, incoming, this);
        }
      }
    }

    if (complexWriters != null && getLastKnownOutcome() == EMIT) {
      throw UserException.unsupportedError()
          .message("Currently functions producing complex types as output are not " +
            "supported in project list for subquery between LATERAL and UNNEST. Please re-write the query using this " +
            "function in the projection list of outermost query.")
          .build(logger);
    }

    state = State.READY;

    int outputRecords = Math.min(incomingRecordCount, memoryManager.getOutputRowCount());
    logger.trace("doWork():[2] memMgr RC {}, incoming rc {}, incoming {}, project {}",
                 memoryManager.getOutputRowCount(), incomingRecordCount, incoming, this);

    doAlloc(outputRecords);
    long projectStartTime = System.currentTimeMillis();
    projector.projectRecords(incoming, 0, outputRecords);
    long projectEndTime = System.currentTimeMillis();
    logger.trace("doWork(): projection: records {}, time {} ms", outputRecords, (projectEndTime - projectStartTime));

    setValueCount(outputRecords);
    if (outputRecords < incomingRecordCount) {
      state = State.REMAINDER;
      remainderIndex = outputRecords;
    } else {
      assert outputRecords == incomingRecordCount;
      incoming.getContainer().zeroVectors();
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    memoryManager.updateOutgoingStats(outputRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());

    // Get the final outcome based on hasRemainder since that will determine if all the incoming records were
    // consumed in current output batch or not
    return getFinalOutcome(state == State.REMAINDER);
  }

  /**
   * For a projection that simply maps input to output columns, but does no
   * calculation, we can just transfer data from incoming to outgoing vectors.
   * No need to allocate memory: all outgoing vectors must be populated via
   * transfer. We do need to release incoming memory for any vectors that were
   * not transferred (no harm in releasing memory for all.)
   * <p>
   * This mode does not do any batch sizing: the outgoing batch will be no
   * larger than the incoming batch which, presumably, was correctly sized
   * by the upstream operator.
   */
  private IterOutcome doTransfers() {
    projector.transferOnly();
    container.setRecordCount(incoming.getRecordCount());
    incoming.getContainer().zeroVectors();
    return getFinalOutcome(false);
  }

  private void handleRemainder() {
    int remainingRecordCount = incoming.getRecordCount() - remainderIndex;
    assert memoryManager.incomingBatch() == incoming;
    int projRecords = Math.min(remainingRecordCount, memoryManager.getOutputRowCount());
    doAlloc(projRecords);

    logger.trace("handleRemainder: remaining RC {}, toProcess {}, remainder index {}, incoming {}, Project {}",
                 remainingRecordCount, projRecords, remainderIndex, incoming, this);

    long projectStartTime = System.currentTimeMillis();
    projector.projectRecords(incoming, remainderIndex, projRecords);
    long projectEndTime = System.currentTimeMillis();

    logger.trace("handleRemainder: projection: records {}, time {} ms", projRecords,(projectEndTime - projectStartTime));

    setValueCount(projRecords);
    if (projRecords < remainingRecordCount) {
      remainderIndex += projRecords;
    } else {
      state = State.READY;
      remainderIndex = 0;
      incoming.getContainer().zeroVectors();
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    memoryManager.updateOutgoingStats(projRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());
  }

  // Called from generated code.
  public void addComplexWriter(ComplexWriter writer) {
    complexWriters.add(writer);
  }

  private void doAlloc(int recordCount) {
    // Allocate memory to those vectors that hold computed
    // columns. Not done for transfer columns.
    for (ValueVector v : allocationVectors) {
      AllocationHelper.allocateNew(v, recordCount);
    }

    // Allocate vv for complexWriters.
    if (complexWriters != null) {
      for (ComplexWriter writer : complexWriters) {
        writer.allocate();
      }
    }
  }

  private void setValueCount(int count) {
    if (count == 0) {
      container.setEmpty();
      return;
    }
    container.setValueCount(count);

    if (complexWriters == null) {
      return;
    }

    for (ComplexWriter writer : complexWriters) {
      writer.setValueCount(count);
    }
  }

  @Override
  protected boolean setupNewSchema() {
    setupNewSchemaFromInput(incoming);
    boolean newSchema = container.isSchemaChanged() || callBack.getSchemaChangedAndReset();
    if (newSchema) {
      container.buildSchema(SelectionVectorMode.NONE);
    }
    return newSchema;
  }

  private void setupNewSchemaFromInput(RecordBatch incomingBatch) {
    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    setupNewSchema(incomingBatch, configuredBatchSize);

    ProjectBatchBuilder batchBuilder = new ProjectBatchBuilder(this,
        container, callBack, incomingBatch);
    ProjectionMaterializer em = new ProjectionMaterializer(context.getOptions(),
        incomingBatch, popConfig.getExprs(), context.getFunctionRegistry(),
        batchBuilder, unionTypeEnabled);
    boolean saveCode = false;
    // Uncomment this line to debug the generated code.
    saveCode = true;
    projector = em.generateProjector(context, saveCode);
    try {
      projector.setup(context, incomingBatch, this, batchBuilder.transfers());
    } catch (SchemaChangeException e) {
      throw UserException.schemaChangeError(e)
          .addContext("Unexpected schema change in the Project operator")
          .build(logger);
    }

    // Reset to default mode if we were in the short-cut mode.
    if (state == State.TRANSFER) {
      state = State.READY;
    }

    // When called for the initial NONE case, there is no upstream batch.
    // But, there is also no reason to use the transfer mode, since there
    // is no data to transfer.
    if (getLastKnownOutcome() != IterOutcome.NONE) {

        // Handle the simplest case: transfer-only vectors, no computation. Drill
        // inserts many of these into each query, and so this is a worthwhile
        // optimization.
       if (incoming.getContainer().getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE &&
          complexWriters == null && allocationVectors.isEmpty()) {
        Preconditions.checkState(container.getNumberOfColumns() == batchBuilder.transfers().size());
        state = State.TRANSFER;
      }
    }
  }

  /**
   * Handle Null input specially when Project operator is for query output.
   * This happens when the input returns no batches (returns a FAST {@code NONE} directly).
   *
   * <p>
   * Project operator has to return a batch with schema derived using the following 3 rules:
   * </p>
   * <ul>
   *  <li>Case 1:  * ==> expand into an empty list of columns. </li>
   *  <li>Case 2:  regular column reference ==> treat as nullable-int column </li>
   *  <li>Case 3:  expressions => Call ExpressionTreeMaterialization over an empty vector contain.
   *           Once the expression is materialized without error, use the output type of materialized
   *           expression. </li>
   * </ul>
   * <p>
   * Note that case 1 is moot for an empty input: there are no columns to expand.
   * <p>
   * The batch is constructed with the above rules, and recordCount = 0.
   * Returned with {@code OK_NEW_SCHEMA} to down-stream operator.
   * </p>
   */
  @Override
  protected IterOutcome handleNullInput() {
    if (!popConfig.isOutputProj()) {
//      BatchSchema incomingSchema = incoming.getSchema();
//      if (incomingSchema != null && incomingSchema.getFieldCount() > 0) {
//        setupNewSchemaFromInput(incoming);
//      }
      return super.handleNullInput();
    }

    // This is a NONE state, so the incoming batch has no batch
    // or schema. Make up one to allow us to compute output columns.
    VectorContainer emptyVC = new VectorContainer();
    emptyVC.buildSchema(SelectionVectorMode.NONE);
    RecordBatch emptyIncomingBatch = new SimpleRecordBatch(emptyVC, context);
    setupNewSchemaFromInput(emptyIncomingBatch);

    container.buildSchema(SelectionVectorMode.NONE);
    container.setEmpty();
    state = State.DONE;

    // Return an empty schema batch.
    return IterOutcome.OK_NEW_SCHEMA;
  }

  private void setupNewSchema(RecordBatch incomingBatch, int configuredBatchSize) {
    memoryManager = new ProjectMemoryManager(configuredBatchSize);
    memoryManager.init(incomingBatch, ProjectRecordBatch.this);
    if (allocationVectors != null) {
      for (ValueVector v : allocationVectors) {
        v.clear();
      }
    }
    allocationVectors = new ArrayList<>();

    if (complexWriters != null) {
      container.clear();
    } else {
      // Release the underlying DrillBufs and reset the ValueVectors to empty
      // Not clearing the container here is fine since Project output schema is
      // not determined solely based on incoming batch. It is defined by the
      // expressions it has to evaluate.
      //
      // If there is a case where only the type of ValueVector already present
      // in container is changed then addOrGet method takes care of it by
      // replacing the vectors.
      container.zeroVectors();
    }
  }

  @Override
  public void dump() {
    logger.error(
        "ProjectRecordBatch[projector={}, hasRemainder={}, remainderIndex={}, recordCount={}, container={}]",
        projector, state == State.REMAINDER, remainderIndex, getRecordCount(), container);
  }
}
