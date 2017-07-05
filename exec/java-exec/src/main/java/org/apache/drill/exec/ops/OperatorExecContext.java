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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.hadoop.conf.Configuration;

import io.netty.buffer.DrillBuf;

/**
 * Narrowed version of the {@link OperatorContext} used to create an
 * easy-to-test version of the operator context that excludes services
 * that require a full Drillbit server.
 */

public interface OperatorExecContext {

  DrillBuf replace(DrillBuf old, int newSize);

  DrillBuf getManagedBuffer();

  DrillBuf getManagedBuffer(int size);

  /**
   * Return the memory allocator for this operator.
   *
   * @return the per-operator memory allocator
   */

  BufferAllocator getAllocator();

  ExecutionControls getExecutionControls();

  OperatorStatReceiver getStatsWriter();

  DrillFileSystem newFileSystem(Configuration conf) throws IOException;

  DrillFileSystem newNonTrackingFileSystem(Configuration conf) throws IOException;

  void close();
}
