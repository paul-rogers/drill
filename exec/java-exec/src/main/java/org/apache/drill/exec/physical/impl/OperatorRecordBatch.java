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
package org.apache.drill.exec.physical.impl;

import java.util.Iterator;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentExecContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

/**
 * Modular implementation of the standard Drill record batch iterator
 * protocol. The protocol has two parts: control of the operator and
 * access to the record batch. Each is encapsulated in separate
 * implementation classes to allow easier customization for each
 * situation. The operator internals are, themselves, abstracted to
 * yet another class with the steps represented as method calls rather
 * than as internal states as in the record batch iterator protocol.
 */

public class OperatorRecordBatch implements CloseableRecordBatch {

  public interface BatchAccessor {
    public BatchSchema getSchema();
    public int schemaVersion();
    public int getRowCount();
    public VectorContainer getOutgoingContainer();
    public TypedFieldId getValueVectorId(SchemaPath path);
    public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids);
    public WritableBatch getWritableBatch();
    public SelectionVector2 getSelectionVector2();
    public SelectionVector4 getSelectionVector4();
    public Iterator<VectorWrapper<?>> iterator();
  }

  /**
   * Tracks changes to schemas via "snapshots" over time. That is, given
   * a schema, tracks if a new schema is the same as the current one. For
   * example, each batch output from a series of readers might be compared,
   * as they are returned, to detect schema changes from one batch to the
   * next. This class does not track vector-by-vector changes as a schema
   * is built.
   */

  public static class SchemaTracker {

    private int schemaVersion;
    private BatchSchema currentSchema;

    public void trackSchema(BatchSchema newSchema) {
      if (currentSchema == newSchema) {
        // The new schema is the same as this one. Only need to
        // check for new fields (existign ones cannot be changed or
        // removed.)

        if (currentSchema.getFieldCount() < newSchema.getFieldCount()) {
          schemaVersion++;
        }
      } else if (currentSchema == null || ! currentSchema.equals(newSchema)) {
        currentSchema = newSchema;
        schemaVersion++;
      }
    }

    public int schemaVersion() {
      return schemaVersion;
    }
  }

  public static class VectorContainerAccessor implements BatchAccessor {

    private VectorContainer container;
    private SchemaTracker schemaTracker = new SchemaTracker();

    public void setContainer(VectorContainer container) {
      this.container = container;
      schemaTracker.trackSchema(container.getSchema());
    }

    @Override
    public BatchSchema getSchema() { return container.getSchema(); }

    @Override
    public int schemaVersion() { return schemaTracker.schemaVersion(); }

    @Override
    public int getRowCount() { return container.getRecordCount(); }

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

  public interface OperatorExec {
    BatchAccessor batchAccessor();
    boolean buildSchema();
    boolean next();
    void cancel();
    void close();
  }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorRecordBatch.class);

  /**
   * State machine that drives the operator executable. Converts
   * between the iterator protocol and the operator executable protocol.
   * Implemented as a separate class in anticipation of eventually
   * changing the record batch (iterator) protocol.
   */

  public static class OperatorDriver {
    public enum State { START, SCHEMA, RUN, END, FAILED, CLOSED }


    private State state = State.START;
    private FragmentExecContext fragmentContext;
    private OperatorExec operatorExec;
    private BatchAccessor batchAccessor;
    private int schemaVersion;

    public OperatorDriver(FragmentExecContext fragmentContext, OperatorExec operatorExec) {
      this.fragmentContext = fragmentContext;
      this.operatorExec = operatorExec;
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
        state = State.CLOSED;
      }
    }
  }

  private final OperatorDriver driver;
  private final BatchAccessor batchAccessor;

  public OperatorRecordBatch(FragmentExecContext fragmentContext, OperatorExec operatorExec) {
    driver = new OperatorDriver(fragmentContext, operatorExec);
    batchAccessor = operatorExec.batchAccessor();
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
  public void close() throws Exception {
    driver.close();
  }
}
