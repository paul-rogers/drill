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
package org.apache.drill.exec.physical.resultSet;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.resultSet.impl.NullResultVectorCacheImpl;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;

public class VectorTransfer {

  private final VectorContainer inputBatch;
  private final VectorContainer outputContainer;
  private final List<TransferPair> transfers = new ArrayList<>();
  private final ResultVectorCache vectorCache;

  public VectorTransfer(VectorContainer incomingBatch, VectorContainer outputContainer, ResultVectorCache vectorCache) {
    this.inputBatch = incomingBatch;
    this.outputContainer = outputContainer;
    this.vectorCache = vectorCache;
  }

  public VectorTransfer(RecordBatch incomingBatch, VectorContainer outputContainer) {
    this(incomingBatch.getContainer(), outputContainer,
         new NullResultVectorCacheImpl(outputContainer.getAllocator()));
  }

  public void setup() {
    transfers.clear();
    for (VectorWrapper<?> vv : inputBatch) {
      TransferPair tp = vv.getValueVector().makeTransferPair(outputContainer.addFromCacheOrGet(vv.getField(), vectorCache));
      transfers.add(tp);
    }
  }

  public int copyRecords() {
    for (TransferPair tp : transfers) {
      tp.transfer();
    }

    int recordCount = inputBatch.getRecordCount();

    // Transfers preserve buffers and their write indexes, so only set
    // the output container record count.

    outputContainer.setRecordCount(recordCount);
    return recordCount;
  }
}
