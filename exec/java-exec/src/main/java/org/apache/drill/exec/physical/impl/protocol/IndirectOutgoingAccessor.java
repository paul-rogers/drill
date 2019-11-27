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
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Vector accessor for an outgoing batch that optionally includes
 * a selection (indirection) vector.
 */
public class IndirectOutgoingAccessor extends SingleOutgoingContainerAccessor {

  private SelectionVector2 sv2;
  private SelectionVector4 sv4;

  public IndirectOutgoingAccessor(VectorContainer container) {
    super(container);
  }

  @Override
  public void registerBatch() {
    Preconditions.checkArgument(container.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE);
    super.registerBatch();
    this.sv2 = null;
    this.sv4 = null;
  }

  public void registerBatch(SelectionVector2 sv2) {
    Preconditions.checkArgument(container.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE);
    super.registerBatch();
    this.sv2 = sv2;
    this.sv4 = null;
  }

  public void registerBatch(SelectionVector4 sv4) {
    Preconditions.checkArgument(container.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE);
    super.registerBatch();
    this.sv2 = null;
    this.sv4 = sv4;
  }

  @Override
  public SelectionVector2 selectionVector2() {
    Preconditions.checkNotNull(sv2);
    return sv2;
  }

  @Override
  public SelectionVector4 selectionVector4() {
    Preconditions.checkNotNull(sv4);
    return sv4;
  }

  @Override
  public void release() {
    super.release();
    if (sv2 != null) {
      sv2.clear();
      sv2 = null;
    }
    if (sv4 != null) {
      sv4.clear();
      sv4 = null;
    }
  }
}
