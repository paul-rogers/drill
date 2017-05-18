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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.AbstractOperatorExecContext;
import org.apache.drill.exec.ops.FragmentExecContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStatReceiver;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ExecutionControls;

/**
 * Implementation of the context used by low-level operator
 * tasks.
 */

public class OperatorExecutionContextImpl extends AbstractOperatorExecContext implements OperatorExecutionContext {

  private FragmentExecContext fragmentContext;
  private ControlsInjector injector;

  public OperatorExecutionContextImpl(FragmentExecContext fragContext, OperatorContext opContext, PhysicalOperator opDefn, ControlsInjector injector) {
    this(fragContext, opContext.getAllocator(), opContext.getStats(), opDefn, injector);
  }

  public OperatorExecutionContextImpl(FragmentExecContext fragContext, BufferAllocator allocator, OperatorStatReceiver stats, PhysicalOperator opDefn, ControlsInjector injector) {
    super(allocator, opDefn, fragContext.getExecutionControls(), stats);
    this.fragmentContext = fragContext;
    this.injector = injector;
  }

  @Override
  public ExecutionControls getExecutionControls() {
    return fragmentContext.getExecutionControls();
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends PhysicalOperator> T getOperatorDefn() {
    return (T) popConfig;
  }

  @Override
  public ControlsInjector getInjector() {
    return injector;
  }

  @Override
  public void injectUnchecked(String desc) {
    ExecutionControls executionControls = fragmentContext.getExecutionControls();
    if (injector != null  &&  executionControls != null) {
      injector.injectUnchecked(executionControls, desc);
    }
  }

  @Override
  public <T extends Throwable> void injectChecked(String desc, Class<T> exceptionClass)
      throws T {
    ExecutionControls executionControls = fragmentContext.getExecutionControls();
    if (injector != null  &&  executionControls != null) {
      injector.injectChecked(executionControls, desc, exceptionClass);
    }
  }

  @Override
  public OperatorStatReceiver getStats() {
    return statsWriter;
  }

  @Override
  public FragmentExecContext getFragmentContext() {
    return fragmentContext;
  }
}
