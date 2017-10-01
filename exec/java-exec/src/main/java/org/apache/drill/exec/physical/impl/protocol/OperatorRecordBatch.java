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
package org.apache.drill.exec.physical.impl.protocol;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentExecContext;
import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.ops.OperatorExecutionContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Modular implementation of the standard Drill record batch iterator
 * protocol. The protocol has two parts: control of the operator and
 * access to the record batch. Each is encapsulated in separate
 * implementation classes to allow easier customization for each
 * situation. The operator internals are, themselves, abstracted to
 * yet another class with the steps represented as method calls rather
 * than as internal states as in the record batch iterator protocol.
 * <p>
 * Note that downstream operators make an assumption that the
 * same vectors will appear from one batch to the next. That is,
 * not only must the schema be the same, but if column "a" appears
 * in two batches, the same value vector must back "a" in both
 * batches. The <tt>TransferPair</tt> abstraction fails if different
 * vectors appear across batches.
 */

public class OperatorRecordBatch implements CloseableRecordBatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorRecordBatch.class);

  public interface BatchAccessor {
    BatchSchema getSchema();
    int schemaVersion();
    int getRowCount();
    VectorContainer getOutgoingContainer();
    TypedFieldId getValueVectorId(SchemaPath path);
    VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids);
    WritableBatch getWritableBatch();
    SelectionVector2 getSelectionVector2();
    SelectionVector4 getSelectionVector4();
    Iterator<VectorWrapper<?>> iterator();
    void release();
  }

  /**
   * Tracks changes to schemas via "snapshots" over time. That is, given
   * a schema, tracks if a new schema is the same as the current one. For
   * example, each batch output from a series of readers might be compared,
   * as they are returned, to detect schema changes from one batch to the
   * next. This class does not track vector-by-vector changes as a schema
   * is built, but rather periodic "snapshots" at times determined by the
   * operator.
   * <p>
   * If an operator is guaranteed to emit a consistent schema, then no
   * checks need be done, and this tracker will report no schema change.
   * On the other hand, a scanner might check schema more often. At least
   * once per reader, and more often if a reader is "late-schema": if the
   * reader can change schema batch-by-batch.
   * <p>
   * Drill defines "schema change" in a very specific way. Not only must
   * the set of columns be the same, and have the same types, it must also
   * be the case that the <b>vectors</b> that hold the columns be identical.
   * Generated code contains references to specific vector objects; passing
   * along different vectors requires new code to be generated and is treated
   * as a schema change.
   * <p>
   * Drill has no concept of "same schema, different vectors." A change in
   * vector is just as serious as a change in schema. Hence, operators
   * try to use the same vectors for their entire lives. That is the change
   * tracked here.
   */

  // TODO: Does not handle SV4 situations

  public static class SchemaTracker {

    private int schemaVersion;
    private BatchSchema currentSchema;
    private List<ValueVector> currentVectors = new ArrayList<>();

    public void trackSchema(VectorContainer newBatch) {

      if (! isSameSchema(newBatch)) {
        schemaVersion++;
        captureSchema(newBatch);
      }
    }

    private boolean isSameSchema(VectorContainer newBatch) {
      if (currentVectors.size() != newBatch.getNumberOfColumns()) {
        return false;
      }

      // Compare vectors by identity: not just same type,
      // must be same instance.

      for (int i = 0; i < currentVectors.size(); i++) {
        if (currentVectors.get(i) != newBatch.getValueVector(i).getValueVector())
          return false;
      }
      return true;
    }

    private void captureSchema(VectorContainer newBatch) {
      currentVectors.clear();
      for (VectorWrapper<?> vw : newBatch) {
        currentVectors.add(vw.getValueVector());
      }
      currentSchema = newBatch.getSchema();
    }

    public int schemaVersion() { return schemaVersion; }
    public BatchSchema schema() { return currentSchema; }
  }

  public static class VectorContainerAccessor implements BatchAccessor {

    private VectorContainer container;
    private SchemaTracker schemaTracker = new SchemaTracker();

    /**
     * Set the vector container. Done initially, and any time the schema of
     * the container may have changed. May be called with the same container
     * as the previous call, or a different one. A schema change occurs
     * unless the vectors are identical across the two containers.
     *
     * @param container the container that holds vectors to be sent
     * downstream
     */

    public void setContainer(VectorContainer container) {
      this.container = container;
      if (container != null) {
        schemaTracker.trackSchema(container);
      }
    }

    @Override
    public BatchSchema getSchema() {
      return container == null ? null : container.getSchema();
    }

    @Override
    public int schemaVersion() { return schemaTracker.schemaVersion(); }

    @Override
    public int getRowCount() {
      return container == null ? 0 : container.getRecordCount();
    }

    @Override
    public VectorContainer getOutgoingContainer() { return container; }

    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      return container.getValueVectorId(path);
    }

    @Override
    public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
      return container.getValueAccessorById(clazz, ids);
    }

    @Override
    public WritableBatch getWritableBatch() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      // Throws an exception by default
      return container.getSelectionVector2();
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      // Throws an exception by default
      return container.getSelectionVector4();
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() { return container.iterator(); }

    @Override
    public void release() { container.zeroVectors(); }
  }

  public static class ContainerAndSv2Accessor extends VectorContainerAccessor {

    private SelectionVector2 sv2;

    public void setSelectionVector(SelectionVector2 sv2) {
      this.sv2 = sv2;
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      return sv2;
    }
  }

  public static class ContainerAndSv4Accessor extends VectorContainerAccessor {

    private SelectionVector4 sv4;

    @Override
    public SelectionVector4 getSelectionVector4() {
      return sv4;
    }
  }

  /**
   * Skeleton implementation of an operator execution context. The concept is
   * <ul>
   * <li>One object that provides services to an operation execution.</li>
   * <li>Wraps fragment-level services to avoid need to pass in a fragment context.</li>
   * <li>Wraps common operator-level services to avoid redundant code.</li>
   * <li>Provides access to the operator definition, to avoid need to pass that
   * along everywhere needed.</li>
   * <li>Defined as an interface to allow a test-time mock without the need
   * for a full Drillbit.</li>
   * </ul>
   * The full collection of services will be large. To start, this is just a
   * wrapper around the existing contexts. Over time, we can add methods here
   * rather than referring to the other contexts. In the end, the other operator
   * contexts can be deprecated.
   */
  public interface OperatorExecServices {

    FragmentExecContext getFragmentContext();

    /**
     * Return the physical operator definition created by the planner and passed
     * into the Drillbit executing the query.
     * @return the physical operator definition
     */

    <T extends PhysicalOperator> T operatorDefn();

    OperatorExecContext getOperatorContext();

    <T extends OperatorExec> T operatorExec();

    BufferAllocator allocator();
  }

  public interface OperatorExec {
    public void bind(OperatorExecServices context);
    BatchAccessor batchAccessor();
    boolean buildSchema();
    boolean next();
    void cancel();
    void close();
  }

  public static class OperatorExecServicesImpl implements OperatorExecServices {

    private final FragmentExecContext fragmentContext;
    private final PhysicalOperator operatorDefn;
    private final OperatorExecContext opContext;
    private final OperatorExec operatorExec;

    public OperatorExecServicesImpl(FragmentExecContext fragContext, PhysicalOperator pop, OperatorExec opExec) {
      fragmentContext = fragContext;
      operatorDefn = pop;
      opContext = fragContext.newOperatorExecContext(pop, null);
      operatorExec = opExec;
    }

    @Override
    public FragmentExecContext getFragmentContext() { return fragmentContext; }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends PhysicalOperator> T operatorDefn() {
      return (T) operatorDefn;
    }

    @Override
    public OperatorExecContext getOperatorContext() { return opContext; }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends OperatorExec> T operatorExec() {
      return (T) operatorExec;
    }

    @Override
    public BufferAllocator allocator() {
      return opContext.getAllocator();
    }

    public void close() {
      opContext.close();
    }
  }

  /**
   * State machine that drives the operator executable. Converts
   * between the iterator protocol and the operator executable protocol.
   * Implemented as a separate class in anticipation of eventually
   * changing the record batch (iterator) protocol.
   */

  public static class OperatorDriver {
    public enum State { START, SCHEMA, RUN, END, FAILED, CLOSED }


    private State state = State.START;
    private final OperatorExecServices opServices;
    private final FragmentExecContext fragmentContext;
    private final OperatorExec operatorExec;
    private final BatchAccessor batchAccessor;
    private int schemaVersion;

    public OperatorDriver(OperatorExecServices opServicees, OperatorExec opExec) {
      this.opServices = opServicees;
      this.fragmentContext = opServicees.getFragmentContext();
      this.operatorExec = opExec;
      batchAccessor = operatorExec.batchAccessor();
    }

    public FragmentExecContext getExecContext() {
      return fragmentContext;
    }

    /**
     * Get the next batch. Performs initialization on the first call.
     * @return the iteration outcome to send downstream
     */

    public IterOutcome next() {
      try {
        switch (state) {
        case START:
          return start();
        case RUN:
          return doNext();
         default:
          logger.debug("Extra call to next() in state " + state + ": " + getOperatorLabel());
          return IterOutcome.NONE;
        }
      } catch (UserException e) {
        cancelSilently();
        state = State.FAILED;
        throw e;
      } catch (Throwable t) {
        cancelSilently();
        state = State.FAILED;
        throw UserException.executionError(t)
          .addContext("Exception thrown from", getOperatorLabel())
          .build(logger);
      }
    }

    /**
     * Cancels the operator before reaching EOF.
     */

    public void cancel() {
      try {
        switch (state) {
        case START:
        case RUN:
          cancelSilently();
          break;
        default:
          break;
        }
      } finally {
        state = State.FAILED;
      }
    }

   /**
     * Start the operator executor. Bind it to the various contexts.
     * Then start the executor and fetch the first schema.
     * @return result of the first batch, which should contain
     * only a schema, or EOF
     */

    private IterOutcome start() {
      state = State.SCHEMA;
      if (operatorExec.buildSchema()) {
        schemaVersion = batchAccessor.schemaVersion();
        state = State.RUN;
        return IterOutcome.OK_NEW_SCHEMA;
      } else {
        state = State.END;
        return IterOutcome.NONE;
      }
    }

    /**
     * Fetch a record batch, detecting EOF and a new schema.
     * @return the <tt>IterOutcome</tt> for the above cases
     */

    private IterOutcome doNext() {
      if (! operatorExec.next()) {
        state = State.END;
        return IterOutcome.NONE;
      }
      int newVersion = batchAccessor.schemaVersion();
      if (newVersion != schemaVersion) {
        schemaVersion = newVersion;
        return IterOutcome.OK_NEW_SCHEMA;
      }
      return IterOutcome.OK;
    }

    /**
     * Implement a cancellation, and ignore any exception that is
     * thrown. We're already in trouble here, no need to keep track
     * of additional things that go wrong.
     */

    private void cancelSilently() {
      try {
        if (state == State.SCHEMA || state == State.RUN) {
          operatorExec.cancel();
        }
      } catch (Throwable t) {
        // Ignore; we're already in a bad state.
        logger.error("Exception thrown from cancel() for " + getOperatorLabel(), t);
      }
    }

    private String getOperatorLabel() {
      return operatorExec.getClass().getCanonicalName();
    }

    public void close() {
      if (state == State.CLOSED) {
        return;
      }
      try {
        operatorExec.close();
      } catch (UserException e) {
        throw e;
      } catch (Throwable t) {
        throw UserException.executionError(t)
          .addContext("Exception thrown from", getOperatorLabel())
          .build(logger);
      } finally {
        this.opServices.getOperatorContext().close();
        state = State.CLOSED;
      }
    }

    public BatchAccessor batchAccessor() {
      return batchAccessor;
    }
  }

  private final OperatorDriver driver;
  private final BatchAccessor batchAccessor;

  public OperatorRecordBatch(FragmentExecContext context, PhysicalOperator config, OperatorExec opExec) {
    OperatorExecServicesImpl services = new OperatorExecServicesImpl(context, config, opExec);

    // Chicken-and-egg binding: the two objects must know about each other. Pass the
    // context to the operator exec via a bind method.

    try {
      opExec.bind(services);
    } catch (UserException e) {
      services.close();
      throw e;
    } catch (Throwable t) {
      services.close();
      throw UserException.executionError(t)
        .addContext("Exception thrown from", opExec.getClass().getSimpleName() + ".bind()")
        .build(logger);
    }
    driver = new OperatorDriver(services, opExec);
    batchAccessor = opExec.batchAccessor();
  }

  @Override
  public FragmentContext getContext() {

    // Backward compatibility with the full server context. Awkward for testing

    FragmentExecContext fragmentContext = driver.getExecContext();
    if (fragmentContext instanceof FragmentContext) {
      return (FragmentContext) fragmentContext;
    } else {
      return null;
    }
  }

  /**
   * Revised method that returns the testing-compatible context.
   * @return
   */

  public FragmentExecContext getExecContext() {
    return driver.getExecContext();
  }

  @Override
  public BatchSchema getSchema() { return batchAccessor.getSchema(); }

  @Override
  public int getRecordCount() { return batchAccessor.getRowCount(); }

  @Override
  public VectorContainer getOutgoingContainer() {
    return batchAccessor.getOutgoingContainer();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return batchAccessor.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return batchAccessor.getValueAccessorById(clazz, ids);
  }

  @Override
  public WritableBatch getWritableBatch() {
    return batchAccessor.getWritableBatch();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return batchAccessor.getSelectionVector2();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return batchAccessor.getSelectionVector4();
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return batchAccessor.iterator();
  }

  @Override
  public void kill(boolean sendUpstream) {
    driver.cancel();
  }

  @Override
  public IterOutcome next() {
    return driver.next();
  }

  @Override
  public void close() {
    driver.close();
  }
}
