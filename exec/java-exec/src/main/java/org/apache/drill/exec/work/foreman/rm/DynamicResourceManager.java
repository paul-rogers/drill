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

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.work.foreman.Foreman;

/**
 * Wrapper around the default and/or distributed resource managers
 * to allow dynamically enabling and disabling queueing.
 */

public class DynamicResourceManager implements ResourceManager {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamicResourceManager.class);

  private final DrillbitContext context;
  private ResourceManager defaultRm;
  private ResourceManager queueingRm;
  private ResourceManager activeRm;
  public long lastUpdateTime;
  public int recheckDelayMs = 5000;

  public DynamicResourceManager(final DrillbitContext context) {
    this.context = context;
    refreshRM();
  }

  public synchronized ResourceManager activeRM() { return activeRm; }

  @Override
  public long memoryPerNode() {
    return activeRm.memoryPerNode();
  }

  @Override
  public int cpusPerNode() {
    return activeRm.cpusPerNode();
  }

  @Override
  public synchronized QueryResourceAllocator newQueryPlanner(QueryContext queryContext) {
    refreshRM();
    return activeRm.newQueryPlanner(queryContext);
  }

  @Override
  public synchronized QueryResourceManager newExecRM(Foreman foreman) {
    refreshRM();
    return activeRm.newExecRM(foreman);
  }

  private void refreshRM() {
    long now = System.currentTimeMillis();
    if (lastUpdateTime + recheckDelayMs >= now) {
      return;
    }
    lastUpdateTime = now;
    @SuppressWarnings("resource")
    SystemOptionManager systemOptions = context.getOptionManager();
    if (systemOptions.getOption(ExecConstants.ENABLE_QUEUE)) {
      if (queueingRm == null) {
        queueingRm = new ThrottledResourceManager(context, new DistributedQueryQueue(context));
      }
      if (activeRm != queueingRm) {
        logger.debug("Enabling ZK-based query queue.");
        activeRm = queueingRm;
      }
    } else {
      if (defaultRm == null) {
        defaultRm = new DefaultResourceManager();
      }
      if (activeRm != defaultRm) {
        logger.debug("Disabling ZK-based query queue.");
        activeRm = defaultRm;
      }
    }
  }

  @Override
  public void close() {
    if (defaultRm != null) {
      defaultRm.close();
      defaultRm = null;
    }
    if (queueingRm != null) {
      queueingRm.close();
      queueingRm = null;
    }
    activeRm = null;
  }
}
