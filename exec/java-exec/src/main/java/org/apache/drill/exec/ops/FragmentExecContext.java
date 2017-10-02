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

import org.apache.drill.exec.ops.services.CodeGenService;
import org.apache.drill.exec.ops.services.CoreExecService;

/**
 * Services passed to fragments that deal only with execution details
 * such as the function registry, options, code generation and the like.
 * Does not include top-level services such as network endpoints. Code
 * written to use this interface can be unit tested quite easily using
 * the {@link OperatorContext} class. Code that uses the wider,
 * more global {@link FragmentContext} must be tested in the context
 * of the entire Drill server, or using mocks for the global services.
 */

public interface FragmentExecContext extends CoreExecService, CodeGenService, BufferManager {
}
