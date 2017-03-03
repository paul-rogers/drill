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

import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;

public class SortMetrics {

  // WARNING: The enum here is used within this class. But, the members of
  // this enum MUST match those in the (unmanaged) ExternalSortBatch since
  // that is the enum used in the UI to display metrics for the query profile.

  public enum Metric implements MetricDef {
    SPILL_COUNT,            // number of times operator spilled to disk
    RETIRED1,               // Was: peak value for totalSizeInMemory
                            // But operator already provides this value
    PEAK_BATCHES_IN_MEMORY, // maximum number of batches kept in memory
    MERGE_COUNT,            // Number of second+ generation merges
    MIN_BUFFER,             // Minimum memory level observed in operation.
    SPILL_MB;               // Number of MB of data spilled to disk. This
                            // amount is first written, then later re-read.
                            // So, disk I/O is twice this amount.

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  private int peakNumBatches = -1;
  private int inputRecordCount = 0;
  private int inputBatchCount = 0; // total number of batches received so far

  /**
   * Sum of the total number of bytes read from upstream.
   * This is the raw memory bytes, not actual data bytes.
   */

  private long totalInputBytes;

  /**
   * Tracks the maximum density of input batches. Density is
   * the amount of actual data / amount of memory consumed.
   * Low density batches indicate an EOF or something wrong in
   * an upstream operator because a low-density batch wastes
   * memory.
   */

  private int maxDensity;
  private int lastDensity = -1;

  /**
   * Tracks the minimum amount of remaining memory for use
   * in populating an operator metric.
   */

  private long minimumBufferSpace;
  private OperatorStats stats;

  public SortMetrics(OperatorStats stats) {
    this.stats = stats;
  }

  public void updateDensity(RecordBatchSizer sizer) {
    // If the vector is less than 75% full, just ignore it, except in the
    // unfortunate case where it is the first batch. Low-density batches generally
    // occur only at the end of a file or at the end of a DFS block. In such a
    // case, we will continue to rely on estimates created on previous, high-
    // density batches.
    // We actually track the max density seen, and compare to 75% of that since
    // Parquet produces very low density record batches.

    if (sizer.avgDensity() < maxDensity * 3 / 4 && sizer.avgDensity() != lastDensity) {
      ExternalSortBatch.logger.trace("Saw low density batch. Density: {}", sizer.avgDensity());
      lastDensity = sizer.avgDensity();
      return;
    }
    maxDensity = Math.max(maxDensity, sizer.avgDensity());
  }

  void updateInputMetrics(long endMem, RecordBatchSizer sizer) {
    inputRecordCount += sizer.rowCount();
    inputBatchCount++;
    totalInputBytes += sizer.actualSize();

    if (minimumBufferSpace == 0) {
      minimumBufferSpace = endMem;
    } else {
      minimumBufferSpace = Math.min(minimumBufferSpace, endMem);
    }
    stats.setLongStat(Metric.MIN_BUFFER, minimumBufferSpace);
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
      stats.setLongStat(Metric.PEAK_BATCHES_IN_MEMORY, peakNumBatches);
    }
  }

  int mergeCount = 0;
  public void incrMergeCount() {
    stats.addLongStat(Metric.MERGE_COUNT, ++mergeCount);
  }

  public void updateSpillCount(int spillCount) {
    stats.setLongStat(Metric.SPILL_COUNT, spillCount);
  }

  public void updateWriteBytes(long writeBytes) {
    stats.setLongStat(Metric.SPILL_MB,
        (int) Math.round(writeBytes / 1024.0D / 1024.0));
  }
}