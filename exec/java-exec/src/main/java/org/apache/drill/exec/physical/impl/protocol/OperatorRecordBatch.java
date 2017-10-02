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

import java.util.Iterator;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentExecContext;
import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.ops.services.ExecutionControlsService;
import org.apache.drill.exec.ops.services.FragmentServices;
import org.apache.drill.exec.ops.services.OperatorScopeService;
import org.apache.drill.exec.ops.services.OperatorServices;
import org.apache.drill.exec.ops.services.OperatorStatsService;
import org.apache.drill.exec.physical.base.PhysicalOperator;
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
 * <p>
 * Note that downstream operators make an assumption that the
 * same vectors will appear from one batch to the next. That is,
 * not only must the schema be the same, but if column "a" appears
 * in two batches, the same value vector must back "a" in both
 * batches. The <tt>TransferPair</tt> abstraction fails if different
 * vectors appear across batches.
 */

public class OperatorRecordBatch implements CloseableRecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorRecordBatch.class);

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

  public static class OperatorScopeServiceImpl implements OperatorScopeService {

    private final PhysicalOperator config;

    @SuppressWarnings("unchecked")
    @Override
    public <T extends PhysicalOperator> T getOperatorDefn() {
      return (T) config;
    }

    @Override
    public BufferAllocator getAllocator() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public OperatorStatsService getStats() {
      // TODO Auto-generated method stub
      return null;
    }

  }

  public static class OperatorServicesImpl implements OperatorServices {

    private final FragmentServices fragmentServices;

    public OperatorServicesImpl(FragmentServices fragmentServices) {
      this.fragmentServices = fragmentServices;
    }

    @Override
    public FragmentServices fragment() {
      return fragmentServices;
    }

    @Override
    public ExecutionControlsService controls() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public OperatorScopeService operator() {
      // TODO Auto-generated method stub
      return null;
    }

  }

  private final OperatorDriver driver;
  private final BatchAccessor batchAccessor;

  public OperatorRecordBatch(FragmentServices context, PhysicalOperator config, OperatorExec opExec) {
    OperatorServicesImpl services = new OperatorServicesImpl(context, config, opExec);

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
