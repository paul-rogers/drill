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
package org.apache.drill.exec.ops;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.testing.ExecutionControls;

public interface FragmentExecContext {
  FunctionImplementationRegistry getFunctionRegistry();
  OptionSet getOptionSet();

  <T> T getImplementationClass(final ClassGenerator<T> cg)
      throws ClassTransformationException, IOException;

  <T> T getImplementationClass(final CodeGenerator<T> cg)
      throws ClassTransformationException, IOException;

  <T> List<T> getImplementationClass(final ClassGenerator<T> cg, final int instanceCount)
      throws ClassTransformationException, IOException;

  <T> List<T> getImplementationClass(final CodeGenerator<T> cg, final int instanceCount)
      throws ClassTransformationException, IOException;
  boolean shouldContinue();
  ExecutionControls getExecutionControls();
  DrillConfig getConfig();
}
