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
package org.apache.drill.exec.work.foreman.rm;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.work.foreman.Foreman;

/**
 * Drillbit-wide resource manager shared by all queries.
 */

public interface ResourceManager {

  long memoryPerNode();
  int cpusPerNode();

  /**
   * Create a resource manager to prepare or describe a query.
   * In this form, no queuing is done, but the plan is created
   * as if queuing had been done.
   * @return a resource manager for the query
   */

  QueryPlanner newQueryPlanner(QueryContext queryContext);

  /**
   * Create a resource manager to execute a query.
   * @param foreman Foreman which manages the execution
   * @return a resource manager for the query
   */

  QueryResourceManager newExecRM(final Foreman foreman);
  void close();
}
