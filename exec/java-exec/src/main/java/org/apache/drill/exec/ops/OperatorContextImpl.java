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
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.services.FileSystemService;
import org.apache.drill.exec.ops.services.OperatorServices;
import org.apache.drill.exec.ops.services.impl.FileSystemServiceImpl;
import org.apache.drill.exec.ops.services.impl.OperatorServicesLegacyImpl;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.work.WorkManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.netty.buffer.DrillBuf;

public class OperatorContextImpl implements OperatorContext, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorContextImpl.class);

  private final FragmentContext context;
  private final BufferAllocator allocator;
  private final ExecutionControls executionControls;
  private final PhysicalOperator popConfig;
  private final BufferManager manager;
  private final FileSystemService fileSystemService;
  private boolean closed = false;
  private final OperatorStats stats;
  private final ExecutorService executor;
  private final ExecutorService scanExecutor;
  private final ExecutorService scanDecodeExecutor;

  /**
   * This lazily initialized executor service is used to submit a {@link Callable task} that needs a proxy user. There
   * is no pool that is created; this pool is a decorator around {@link WorkManager#executor the worker pool} that
   * returns a {@link ListenableFuture future} for every task that is submitted. For the shutdown sequence,
   * see {@link WorkManager#close}.
   */
  private ListeningExecutorService delegatePool;

  public OperatorContextImpl(PhysicalOperator popConfig, FragmentContext context) throws OutOfMemoryException {
    this(popConfig, context, null);
  }

  public OperatorContextImpl(PhysicalOperator popConfig, FragmentContext context, OperatorStats stats)
      throws OutOfMemoryException {
    this.context = context;
    this.allocator = context.getNewChildAllocator(popConfig.getClass().getSimpleName(),
        popConfig.getOperatorId(), popConfig.getInitialAllocation(), popConfig.getMaxAllocation());
    this.popConfig = popConfig;
    this.manager = new BufferManagerImpl(allocator);

    executionControls = context.getExecutionControls();
    if (stats != null) {
      this.stats = stats;
    } else {
      OpProfileDef def =
          new OpProfileDef(popConfig.getOperatorId(), popConfig.getOperatorType(),
                           OperatorUtilities.getChildCount(popConfig));
      this.stats = context.getStats().newOperatorStats(def, allocator);
    }
    fileSystemService = new FileSystemServiceImpl(this.stats);
    executor = context.getDrillbitContext().getExecutor();
    scanExecutor = context.getDrillbitContext().getScanExecutor();
    scanDecodeExecutor = context.getDrillbitContext().getScanDecodeExecutor();
  }

  public String name() {
    return popConfig != null ? popConfig.getClass().getName() : "unknown";
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends PhysicalOperator> T getPopConfig() {
    return (T) popConfig;
  }

  @Override
  public DrillBuf replace(DrillBuf old, int newSize) {
    return manager.replace(old, newSize);
  }

  @Override
  public DrillBuf getManagedBuffer() {
    return manager.getManagedBuffer();
  }

  @Override
  public DrillBuf getManagedBuffer(int size) {
    return manager.getManagedBuffer(size);
  }

  @Override
  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  @Override
  public BufferAllocator getAllocator() {
    if (allocator == null) {
      throw new UnsupportedOperationException("Operator context does not have an allocator");
    }
    return allocator;
  }

  @Override
  public DrillFileSystem newFileSystem(Configuration conf) throws IOException {
    return fileSystemService.newFileSystem(conf);
  }

  /**
   * Creates a DrillFileSystem that does not automatically track operator stats.
   */
  @Override
  public DrillFileSystem newNonTrackingFileSystem(Configuration conf) throws IOException {
    return fileSystemService.newNonTrackingFileSystem(conf);
  }

  // Allow an operator to use the thread pool
  @Override
  public ExecutorService getExecutor() {
    return executor;
  }

  @Override
  public ExecutorService getScanExecutor() {
    return scanExecutor;
  }

  @Override
  public ExecutorService getScanDecodeExecutor() {
    return scanDecodeExecutor;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (closed) {
      logger.debug("Attempted to close Operator context for {}, but context is already closed", popConfig != null ? popConfig.getClass().getName() : null);
      return;
    }
    logger.debug("Closing context for {}", popConfig != null ? popConfig.getClass().getName() : null);

    closed = true;
    Exception ex = null;
    try {
      manager.close();
    } catch (Exception e) {
      ex = e;
    }
    try {
      if (allocator != null) {
        allocator.close();
      }
    } catch (Exception e) {
      ex = ex == null ? e : ex;
    }
    fileSystemService.close();
    if (ex != null) {
      if (ex instanceof UserException) {
        throw (UserException) ex;
      }
      throw UserException.internalError(ex)
        .addContext("Failure closing operator exec context for " + name())
        .build(logger);
    }
  }

  @Override
  public OperatorStats getStats() {
    return stats;
  }

  @Override
  public <RESULT> ListenableFuture<RESULT> runCallableAs(final UserGroupInformation proxyUgi,
                                                         final Callable<RESULT> callable) {
    synchronized (this) {
      if (delegatePool == null) {
        delegatePool = MoreExecutors.listeningDecorator(executor);
      }
    }
    return delegatePool.submit(new Callable<RESULT>() {
      @Override
      public RESULT call() throws Exception {
        final Thread currentThread = Thread.currentThread();
        final String originalThreadName = currentThread.getName();
        currentThread.setName(proxyUgi.getUserName() + ":task-delegate-thread");
        final RESULT result;
        try {
          result = proxyUgi.doAs(new PrivilegedExceptionAction<RESULT>() {
            @Override
            public RESULT run() throws Exception {
              return callable.call();
            }
          });
        } finally {
          currentThread.setName(originalThreadName);
        }
        return result;
      }
    });
  }

  private OperatorServices opServices;

  public OperatorServices asServices() {
    if (opServices == null) {
      opServices = new OperatorServicesLegacyImpl(context.asServices(), this);
    }
    return opServices;
  }
}
