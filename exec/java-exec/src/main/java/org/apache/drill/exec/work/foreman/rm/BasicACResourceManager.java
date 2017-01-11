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

import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.local.LocalClusterCoordinator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.util.MemoryAllocationUtilities;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueryQueueException;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueueLease;
import org.apache.drill.exec.work.foreman.rm.QueryQueue.QueueTimeoutException;

/**
 * Global resource manager that provides basic admission control (AC)
 * via a configured queue: either the Zookeeper-based distributed queue
 * or the in-process embedded Drillbit queue. The queue places an upper
 * limit on the number of running queries. This limit then "slices"
 * memory and CPU between queries: each gets the same share of resources.
 * <p>
 * This is a "basic" implementation. Clearly, a more advanced implementation
 * could look at query cost to determine whether to give a given query more
 * or less than the "standard" share. That is left as a future exercise;
 * in this version we just want to get the basics working.
 */

public class BasicACResourceManager extends AbstractResourceManager {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicACResourceManager.class);

  public static class QueuedQueryResourceManager implements QueryResourceManager {

    private final BasicACResourceManager rm;
    private final Foreman foreman;
    private QueueLease lease;
    private PhysicalPlan plan;
    private QueryWorkUnit work;
    private double queryCost;

    public QueuedQueryResourceManager(final BasicACResourceManager rm, final Foreman foreman) {
      this.rm = rm;
      this.foreman = foreman;
    }

    @Override
    public void setPlan(final PhysicalPlan plan, final QueryWorkUnit work) {
      this.plan = plan;
      this.work = work;
    }

    @Override
    public void setCost(double cost) {
      this.queryCost = cost;
    }

    @Override
    public void admit( ) throws QueueTimeoutException, QueryQueueException {
      lease = rm.queue().queue(foreman.getQueryId(), queryCost());
    }

    /**
     * We are normally given a plan from which we can compute cost and on
     * which we can do memory allocations. Other times we are simply given
     * a cost, with memory pre-planed elsewhere.
     * @return
     */

    private double queryCost() {
      if (plan != null)
        return plan.totalCost();
      return queryCost;
    }

    @Override
    public void planMemory(boolean replanMemory) {
      if (! replanMemory) {
        return;
      }
      List<ExternalSort> sortList = findSorts();
      if (sortList.isEmpty()) {
        return;
      }
      // if there are any sorts, compute the maximum allocation, and set it on them
      planSortMemory(sortList);
    }

    private List<ExternalSort> findSorts() {
      // look for external sorts
      final List<ExternalSort> sortList = new LinkedList<>();
      for (final PhysicalOperator op : plan.getSortedOperators()) {
        if (op instanceof ExternalSort) {
          sortList.add((ExternalSort) op);
        }
      }
      return sortList;
    }

    private void planSortMemory(List<ExternalSort> sortList) {
      @SuppressWarnings("resource")
      QueryContext queryContext = foreman.getQueryContext();
      final OptionManager optionManager = queryContext.getOptions();
      final long maxWidthPerNode = optionManager.getOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY).num_val;
      long maxAllocPerNode = lease.queryMemoryPerNode();
      final long maxSortAlloc = maxAllocPerNode / (sortList.size() * maxWidthPerNode);
      logger.debug("Max sort alloc: {}", maxSortAlloc);

      for(final ExternalSort externalSort : sortList) {
        long alloc = Math.max(maxSortAlloc, externalSort.getInitialAllocation());
        externalSort.setMaxAllocation(alloc);
      }
    }

    @Override
    public void exit() {
      assert lease != null;
      rm.queue().release(lease);
      lease = null;
    }

    @Override
    public boolean hasQueue() {
      return true;
    }
  }

  private final QueryQueue queue;

  public BasicACResourceManager(final DrillbitContext drillbitContext, final QueryQueue queue) {
    super(drillbitContext);
    this.queue = queue;
    queue.setMemoryPerNode(memoryPerNode());
  }

  protected QueryQueue queue() { return queue; }

  @Override
  public QueryResourceManager newQueryRM(Foreman foreman) {
    return new QueuedQueryResourceManager(this, foreman);
  }

  @Override
  public void close() {
    queue.close();
  }

}
