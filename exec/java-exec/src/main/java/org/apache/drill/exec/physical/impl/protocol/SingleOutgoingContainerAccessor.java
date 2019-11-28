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

import jersey.repackaged.com.google.common.base.Preconditions;

/**
 * Vector container wrapper for operators that produce all batches
 * via a single container.
 * Supports containers without a selection vector.
 */

public class SingleOutgoingContainerAccessor extends AbstractContainerAccessor {

  protected final VectorContainer container;
  private int schemaVersion;

  public SingleOutgoingContainerAccessor(VectorContainer container) {
    this.container = container;
  }

  public void registerBatch(int newVersion) {
    Preconditions.checkArgument(newVersion >= schemaVersion);
    if (newVersion > schemaVersion) {
      container.schemaChanged();
    }
    schemaVersion = newVersion;
    batchCount++;
  }

  public void registerBatch() {
    if (container.isSchemaChanged()) {
      schemaVersion++;
    }
    batchCount++;
  }

  @Override
  public VectorContainer container() {
    return container;
  }

  @Override
  public int schemaVersion() { return schemaVersion; }

  @Override
  public void release() {
    container.zeroVectors();
  }
}
