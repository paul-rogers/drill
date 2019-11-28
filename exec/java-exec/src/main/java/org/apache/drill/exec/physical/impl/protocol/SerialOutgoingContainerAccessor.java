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
package org.apache.drill.exec.physical.impl.protocol;

import org.apache.drill.exec.record.VectorContainer;

/**
 * Vector container wrapper for scans. Scans include a series
 * of readers, each of which will produce its own vector container.
 * This accessor tracks schema changes across batches to capture
 * changes (or lack thereof) between one reader and the next.
 * Supports containers without a selection vector.
 */

public class SerialOutgoingContainerAccessor extends AbstractContainerAccessor {

  private VectorContainer container;
  private final SchemaTracker schemaTracker = new SchemaTracker();

  /**
   * Define a schema that does not necessarily contain any data.
   * Call this to declare a schema even when there are no results to
   * report.
   */

  public void setSchema(VectorContainer container) {
    this.container = container;
    if (container != null) {
      schemaTracker.trackSchema(container);
    }
  }

  /**
   * Define an output batch. Called each time a new batch is sent
   * downstream. Checks if the schema of this batch is the same as
   * that of any previous batch, and updates the schema version if
   * the schema changes. May be called with the same container
   * as the previous call, or a different one. A schema change occurs
   * unless the vectors are identical across the two containers.
   *
   * @param container the container that holds vectors to be sent
   * downstream
   */

  public void registerBatch(VectorContainer container) {
    setSchema(container);
    batchCount++;
  }

  @Override
  public VectorContainer container() {
    return container;
  }

  @Override
  public int schemaVersion() { return schemaTracker.schemaVersion(); }

  @Override
  public void release() {
    if (container != null) {
      container.zeroVectors();
    }
  }
}
