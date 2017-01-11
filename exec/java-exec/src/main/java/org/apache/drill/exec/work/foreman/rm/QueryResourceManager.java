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

import java.util.List;

import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueryQueueException;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueueTimeoutException;

/**
 * Manages resources for an individual query in conjunction with the
 * global {@link ResourceManager}. Handles memory and CPU allocation along
 * with queueing.
 * <p>
 * This interface allows a variety of resource management strategies to
 * exist for different purposes.
 * <p>
 * The methods here assume external synchronization: a single query calls
 * the methods at known times; there are no concurrent calls.
 */

public interface QueryResourceManager {

  /**
   * Hint that this resource manager queues. Allows the Foreman
   * to short-circuit expensive logic if no queuing will actually
   * be done. This is a static attribute per Drillbit run.
   */
  boolean hasQueue();
  void setPlan(PhysicalPlan plan, QueryWorkUnit work);
  void setCost(double cost);
  /**
   * Apply memory limits to the physical plan.
   */
  void planMemory(boolean replanMemory);
  /**
   * Admit the query into the cluster. Blocks until the query
   * can run. (Later revisions may use a more thread-friendly
   * approach.)
   * @throws QueryQueueException if something goes wrong with the
   * queue mechanism
   * @throws QueueTimeoutException if the query timed out waiting to
   * be admitted.
   */
  void admit() throws QueueTimeoutException, QueryQueueException;
  /**
   * Mark the query as completing, giving up its slot in the
   * cluster.
   */
  void exit();
}
