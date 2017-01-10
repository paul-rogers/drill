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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.util.MemoryAllocationUtilities;
import org.apache.drill.exec.work.foreman.Foreman;

/**
 * Represents a default resource manager for clusters that do not
 * provide query queues. Without queues to provide a hard limit on the
 * query admission rate, the number of active queries must be estimated
 * and the resulting resource allocations will be rough estimates.
 */

public class DefaultResourceManager implements ResourceManager {

  public static class DefaultQueryResourceManager implements QueryResourceManager {

    private final DefaultResourceManager rm;
    private final Foreman foreman;

    public DefaultQueryResourceManager(final DefaultResourceManager rm, final Foreman foreman) {
      this.rm = rm;
      this.foreman = foreman;
    }

    @Override
    public void applyMemoryAllocation(PhysicalPlan plan) {
      MemoryAllocationUtilities.setupSortMemoryAllocations(plan, foreman.getQueryContext());
    }

    @Override
    public void admit(double cost) {
      // No queueing by default
    }

    @Override
    public void exit() {
      // No queueing by default
    }

    @Override
    public boolean hasQueue() {
      return false;
    }

  }

  BootStrapContext bootStrapContext;
  public long memoryPerNode;
  public int cpusPerNode;

  public DefaultResourceManager() {
    memoryPerNode = DrillConfig.getMaxDirectMemory();
    cpusPerNode = Runtime.getRuntime().availableProcessors();
  }

  @Override
  public long memoryPerNode() {
    return memoryPerNode;
  }

  @Override
  public int cpusPerNode() {
    return cpusPerNode;
  }

  @Override
  public QueryResourceManager newQueryRM(final Foreman foreman) {
    return new DefaultQueryResourceManager(this, foreman);
  }

  @Override
  public void close() {
    // Nothing to do.
  }

}
