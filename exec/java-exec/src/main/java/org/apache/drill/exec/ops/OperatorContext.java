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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.services.FileSystemService;
import org.apache.drill.exec.ops.services.OperatorServices;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.util.concurrent.ListenableFuture;

public interface OperatorContext extends BufferManager, FileSystemService {

  BufferAllocator getAllocator();

  ExecutionControls getExecutionControls();

  <T extends PhysicalOperator> T getPopConfig();
  OperatorStats getStats();
  OperatorServices asServices();

  ExecutorService getExecutor();

  ExecutorService getScanExecutor();

  ExecutorService getScanDecodeExecutor();

  /**
   * Run the callable as the given proxy user.
   *
   * @param proxyUgi proxy user group information
   * @param callable callable to run
   * @param <RESULT> result type
   * @return Future<RESULT> future with the result of calling the callable
   */
  <RESULT> ListenableFuture<RESULT> runCallableAs(UserGroupInformation proxyUgi,
                                                                  Callable<RESULT> callable);

  @Override
  void close();
}
