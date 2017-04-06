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

import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DistributedSemaphore.DistributedLease;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

/**
 * Distributed query queue which uses a Zookeeper distributed semaphore to
 * control queueing across the cluster. The distributed queue is actually two
 * queues: one for "small" queries, another for "large" queries. Query size is
 * determined by the Planner's estimate of query cost.
 * <p>
 * This queue is configured using system options:
 * <dl>
 * <dt><tt>exec.queue.enable</tt>
 * <dt>
 * <dd>Set to true to enable the distributed queue.</dd>
 * <dt><tt>exec.queue.large</tt>
 * <dt>
 * <dd>The maximum number of large queries to admit. Additional
 * queries wait in the queue.</dd>
 * <dt><tt>exec.queue.small</tt>
 * <dt>
 * <dd>The maximum number of small queries to admit. Additional
 * queries wait in the queue.</dd>
 * <dt><tt>exec.queue.threshold</tt>
 * <dt>
 * <dd>The cost threshold. Queries below this size are small, at
 * or above this size are large..</dd>
 * <dt><tt>exec.queue.timeout_millis</tt>
 * <dt>
 * <dd>The maximum time (in milliseconds) a query will wait in the
 * queue before failing.</dd>
 * </dl>
 * <p>
 * The above values are refreshed every five seconds. This aids performance
 * a bit in systems with very high query arrival rates.
 */

public class DistributedQueryQueue implements QueryQueue {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DistributedQueryQueue.class);

  private class DistributedQueueLease implements QueueLease {
    private final QueryId queryId;
    private DistributedLease lease;
    private final String queueName;
    private long queryMemory;

    public DistributedQueueLease(QueryId queryId, String queueName, DistributedLease lease, long queryMemory) {
      this.queryId = queryId;
      this.queueName = queueName;
      this.lease = lease;
      this.queryMemory = queryMemory;
    }

    @Override
    public String toString() {
      return String.format("Lease for %s queue to query %s",
          queueName, QueryIdHelper.getQueryId(queryId));
    }

    @Override
    public long queryMemoryPerNode() {
      return queryMemory;
    }

    @Override
    public void release() {
      DistributedQueryQueue.this.release(this);
    }
  }

  private int largeQueueSize;
  private int smallQueueSize;
  private SystemOptionManager optionManager;
  private ClusterCoordinator clusterCoordinator;
  private long queueThreshold;
  private long queueTimeout;
  private long refreshTime;
  private long memoryPerSmallQuery;
  private long memoryPerLargeQuery;

  public DistributedQueryQueue(DrillbitContext context) {
    optionManager = context.getOptionManager();
    clusterCoordinator = context.getClusterCoordinator();
  }

  @Override
  public void setMemoryPerNode(long memoryPerNode) {

    // Divide up memory between queues using admission rate
    // to give more memory to larger queries and less to
    // smaller queries. We assume that large queries are
    // larger than small queries by a factor of
    // largeToSmallRatio.

    double largeToSmallRatio = 4.0; // TODO: add config param.
    double totalUnits = largeToSmallRatio * largeQueueSize + smallQueueSize;
    double memoryUnit = memoryPerNode / totalUnits;
    memoryPerSmallQuery = Math.round( memoryUnit * largeToSmallRatio * largeQueueSize );
    memoryPerLargeQuery = Math.round( memoryUnit * smallQueueSize );
  }

  @Override
  public long getDefaultMemoryPerNode(double cost) {
    return (cost < queueThreshold) ? memoryPerSmallQuery : memoryPerLargeQuery;
  }

  /**
   * This limits the number of "small" and "large" queries that a Drill cluster will run
   * simultaneously, if queueing is enabled. If the query is unable to run, this will block
   * until it can. Beware that this is called under run(), and so will consume a Thread
   * while it waits for the required distributed semaphore.
   *
   * @param totalCost the query plan
   * @throws QueryQueueException
   * @throws QueueTimeoutException
   * @throws ForemanSetupException
   */

  @SuppressWarnings("resource")
  @Override
  public QueueLease queue(QueryId queryId, double cost) throws QueryQueueException, QueueTimeoutException {
    refreshConfig();
    final String queueName;
    DistributedLease lease = null;
    long queryMemory;
    try {
      final DistributedSemaphore distributedSemaphore;

      // get the appropriate semaphore
      if (cost >= queueThreshold) {
        distributedSemaphore = clusterCoordinator.getSemaphore("query.large", largeQueueSize);
        queueName = "large";
        queryMemory = memoryPerLargeQuery;
      } else {
        distributedSemaphore = clusterCoordinator.getSemaphore("query.small", smallQueueSize);
        queueName = "small";
        queryMemory = memoryPerSmallQuery;
      }

      lease = distributedSemaphore.acquire(queueTimeout, TimeUnit.MILLISECONDS);
    } catch (final Exception e) {
      logger.error("Unable to acquire slot for query.", e);
      throw new QueryQueueException("Unable to acquire slot for query.", e);
    }

    if (lease == null) {
      int timeoutSecs = (int) Math.round(queueTimeout/1000.0);
      logger.warn(String.format("Queue timeout: %s after %d seconds.", queueName, timeoutSecs));
      throw new QueueTimeoutException(queryId, queueName, timeoutSecs);
    }
    return new DistributedQueueLease(queryId, queueName, lease, queryMemory);
  }

  private void refreshConfig() {
    long now = System.currentTimeMillis();
    if (now < refreshTime) {
      return;
    }
    queueThreshold = optionManager.getOption(ExecConstants.QUEUE_THRESHOLD_SIZE);
    queueTimeout = optionManager.getOption(ExecConstants.QUEUE_TIMEOUT);
    largeQueueSize = (int) optionManager.getOption(ExecConstants.LARGE_QUEUE_SIZE);
    smallQueueSize = (int) optionManager.getOption(ExecConstants.SMALL_QUEUE_SIZE);
    refreshTime = now + 5000;
  }

  @Override
  public boolean enabled() {
    return true;
  }

  private void release(QueueLease lease) {
    DistributedQueueLease theLease = (DistributedQueueLease) lease;
    for (;;) {
      try {
        theLease.lease.close();
        theLease.lease = null;
        break;
      } catch (final InterruptedException e) {
        // if we end up here, the loop will try again
      } catch (final Exception e) {
        logger.warn("Failure while releasing lease.", e);
        break;
      }
    }
  }

  @Override
  public void close() {
    // Nothing to do.
  }
}