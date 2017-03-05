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
package org.apache.drill.exec.physical.impl.xsort.managed;

import org.apache.drill.exec.ops.OperatorStatReceiver;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;

public class SortMetrics {

  private int peakNumBatches = -1;
  private int inputRecordCount = 0;
  private int inputBatchCount = 0; // total number of batches received so far

  /**
   * Sum of the total number of bytes read from upstream.
   * This is the raw memory bytes, not actual data bytes.
   */

  private long totalInputBytes;

  /**
   * Tracks the minimum amount of remaining memory for use
   * in populating an operator metric.
   */

  private long minimumBufferSpace;
  private OperatorStatReceiver stats;

  public SortMetrics(OperatorStatReceiver stats) {
    this.stats = stats;
  }

  public void updateInputMetrics(int rowCount, int batchSize) {
    inputRecordCount += rowCount;
    inputBatchCount++;
    totalInputBytes += batchSize;
  }

  public void updateMemory(long freeMem) {

    if (minimumBufferSpace == 0) {
      minimumBufferSpace = freeMem;
    } else {
      minimumBufferSpace = Math.min(minimumBufferSpace, freeMem);
    }
    stats.setLongStat(ExternalSortBatch.Metric.MIN_BUFFER, minimumBufferSpace);
  }

  public int getInputRowCount() {
    return inputRecordCount;
  }

  public long getInputBatchCount() {
    return inputBatchCount;
  }

  public long getInputBytes() {
    return totalInputBytes;
  }

  public void updatePeakBatches(int bufferedBatchCount) {
    if (peakNumBatches < bufferedBatchCount) {
      peakNumBatches = bufferedBatchCount;
      stats.setLongStat(ExternalSortBatch.Metric.PEAK_BATCHES_IN_MEMORY, peakNumBatches);
    }
  }

  public void incrMergeCount() {
    stats.addLongStat(ExternalSortBatch.Metric.MERGE_COUNT, 1);
  }

  public void incrSpillCount() {
    stats.addLongStat(ExternalSortBatch.Metric.SPILL_COUNT, 1);
  }

  public void updateWriteBytes(long writeBytes) {
    stats.setDoubleStat(ExternalSortBatch.Metric.SPILL_MB,
        writeBytes / 1024.0D / 1024.0);
  }
}