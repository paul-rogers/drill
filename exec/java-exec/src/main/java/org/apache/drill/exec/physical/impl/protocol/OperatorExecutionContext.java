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

import org.apache.drill.exec.ops.FragmentExecContext;
import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.ops.OperatorStatReceiver;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.testing.ControlsInjector;

/**
 * Defines the set of services used by operator implementations. Combines
 * the fragment and operator execution contexts, removing global services
 * that most operators should not use. Two concrete implementations exist:
 * one for testing, another for full server integration.
 */

public interface OperatorExecutionContext extends OperatorExecContext {

  FragmentExecContext getFragmentContext();

  /**
   * Return the physical operator definition created by the planner and passed
   * into the Drillbit executing the query.
   * @return the physical operator definition
   */

  <T extends PhysicalOperator> T getOperatorDefn();

  /**
   * A write-only interface to the Drill statistics mechanism. Allows
   * operators to update statistics.
   * @return operator statistics
   */

  OperatorStatReceiver getStats();

  /**
   * Returns the fault injection mechanism used to introduce faults at runtime
   * for testing.
   * @return the fault injector
   */

  ControlsInjector getInjector();

  /**
   * Insert an unchecked fault (exception). Handles the details of checking if
   * fault injection is enabled and this particular fault is selected.
   * @param desc the description of the fault used to match a fault
   * injection parameter to determine if the fault should be injected
   * @throws RuntimeException an unchecked exception if the fault is enabled
   */

  void injectUnchecked(String desc);

  /**
   * Insert a checked fault (exception) of the given class. Handles the details
   * of checking if fault injection is enabled and this particular fault is
   * selected.
   *
   * @param desc the description of the fault used to match a fault
   * injection parameter to determine if the fault should be injected
   * @param exceptionClass the class of exception to be thrown
   * @throws T if the fault is enabled
   */

  <T extends Throwable> void injectChecked(String desc, Class<T> exceptionClass)
      throws T;
}
