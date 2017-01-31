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
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.foreman.Foreman;

/**
 * Represents a default resource manager for clusters that do not provide query
 * queues. Without queues to provide a hard limit on the query admission rate,
 * the number of active queries must be estimated and the resulting resource
 * allocations will be rough estimates.
 */

public class DefaultResourceManager implements ResourceManager {

//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultResourceManager.class);

  public static class DefaultQueryResourceManager implements QueryResourceManager {

    @SuppressWarnings("unused")
    private final DefaultResourceManager rm;
    private final Foreman foreman;

    public DefaultQueryResourceManager(final DefaultResourceManager rm, final Foreman foreman) {
      this.rm = rm;
      this.foreman = foreman;
    }

    @Override
    public void visitAbstractPlan(boolean replanMemory, PhysicalPlan plan) {
      if (! replanMemory || plan == null) {
        return;
      }
      MemoryAllocationUtilities.setupSortMemoryAllocations(plan, foreman.getQueryContext());
    }

    @Override
    public void setPhysicalPlan(QueryWorkUnit work) {
    }

    @Override
    public void setCost(double cost) {
      // Nothing to do. The EXECUTION option in Foreman calls this,
      // but does not do the work to plan sort memory. Is EXECUTION
      // even used?
    }

    @Override
    public void admit() {
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
