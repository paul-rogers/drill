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

import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

public class IncomingBatchAccessor extends AbstractContainerAccessor {

  private final RecordBatch incomingBatch;
  private int schemaVersion = -1;

  public IncomingBatchAccessor(RecordBatch incomingBatch) {
    this.incomingBatch = incomingBatch;
  }

  public void acceptBatch() {
    if (container().isSchemaChanged()) {
      schemaVersion++;
    }
  }

  @Override
  public int schemaVersion() {
    return schemaVersion;
  }

  @Override
  public VectorContainer container() {
    return incomingBatch.getContainer();
  }

  @Override
  public SelectionVector2 selectionVector2() {
    return incomingBatch.getSelectionVector2();
  }

  @Override
  public SelectionVector4 selectionVector4() {
    return incomingBatch.getSelectionVector4();
  }

  @Override
  public void release() {
    switch(schema().getSelectionVectorMode()) {
    case TWO_BYTE:
      selectionVector2().clear();
      break;
    case FOUR_BYTE:
      selectionVector4().clear();
      break;
    default:
      break;
    }
    container().zeroVectors();
  }
}
