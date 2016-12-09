/**
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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.util.LinkedList;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch.SortResults;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;

public class InMemorySorter implements SortResults {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InMemorySorter.class);

  private SortRecordBatchBuilder builder;
  private MSorter mSorter;
  private final FragmentContext context;
  private final BufferAllocator oAllocator;
  private SelectionVector4 sv4;
  private final OperatorCodeGenerator opCg;
  private final int outputBatchSize;
  private int batchCount;

  public InMemorySorter(FragmentContext context, BufferAllocator allocator, OperatorCodeGenerator opCg, int outputBatchSize) {
    this.context = context;
    this.oAllocator = allocator;
    this.opCg = opCg;
    this.outputBatchSize = outputBatchSize;
  }

  public SelectionVector4 sort(LinkedList<BatchGroup.InputBatch> batchGroups, VectorAccessible batch,
                                VectorContainer destContainer) {
    if (builder != null) {
      builder.clear();
      builder.close();
    }
    builder = new SortRecordBatchBuilder(oAllocator);

    while (! batchGroups.isEmpty()) {
      BatchGroup.InputBatch group = batchGroups.pollLast();
      RecordBatchData rbd = new RecordBatchData(group.getContainer(), oAllocator);
      rbd.setSv2(group.getSv2());
      builder.add(rbd);
    }

    try {
      builder.build(context, destContainer);
      sv4 = builder.getSv4();
      mSorter = opCg.createNewMSorter(batch);
      mSorter.setup(context, oAllocator, sv4, destContainer, outputBatchSize);
    } catch (SchemaChangeException e) {
      throw UserException.unsupportedError(e)
            .message("Unexpected schema change - likely code error.")
            .build(logger);
    }

    // For testing memory-leak purpose, inject exception after mSorter finishes setup
    ExternalSortBatch.injector.injectUnchecked(context.getExecutionControls(), ExternalSortBatch.INTERRUPTION_AFTER_SETUP);
    mSorter.sort(destContainer);

    // sort may have prematurely exited due to should continue returning false.
    if (!context.shouldContinue()) {
      return null;
    }

    // For testing memory-leak purpose, inject exception after mSorter finishes sorting
    ExternalSortBatch.injector.injectUnchecked(context.getExecutionControls(), ExternalSortBatch.INTERRUPTION_AFTER_SORT);
    sv4 = mSorter.getSV4();

    destContainer.buildSchema(SelectionVectorMode.FOUR_BYTE);
    return sv4;
  }

  @Override
  public boolean next() {
    boolean more = sv4.next();
    if (more) { batchCount++; }
    return more;
  }

  @Override
  public void close() {
    if (builder != null) {
      builder.clear();
      builder.close();
    }
    if (mSorter != null) {
      mSorter.clear();
    }
  }

  @Override
  public int getBatchCount() {
    return batchCount;
  }

  @Override
  public int getRecordCount() {
    return sv4.getTotalCount();
  }
}