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
package org.apache.drill.exec.physical.impl.spill;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SmartAllocationHelper;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

import com.google.common.collect.Sets;

/**
 * Given a record batch or vector container, determines the actual memory
 * consumed by each column, the average row, and the entire record batch.
 */

public class RecordBatchSizer {

  /**
   * Column size information.
   */
  public static class ColumnSize {
    public final String prefix;
    public final MaterializedField metadata;

    /**
     * Actual average column width as determined from actual memory use. This
     * size is larger than the actual data size since this size includes per-
     * column overhead such as any unused vector space, etc.
     */

    public final int estSize;
    public final int valueCount;
    public final int entryCount;
    public final int dataSize;
    public final int estElementCount;

    public ColumnSize(ValueVector v, String prefix, int valueCount) {
      this.prefix = prefix;
      this.valueCount = valueCount;
      metadata = v.getField();

      // The amount of memory consumed by the payload: the actual
      // data stored in the vectors.

      if (v.getField().getDataMode() == DataMode.REPEATED) {

        // Repeated vectors are special: they have an associated offset vector
        // that changes the value count of the contained vectors.

        @SuppressWarnings("resource")
        UInt4Vector offsetVector = ((RepeatedValueVector) v).getOffsetVector();
        entryCount = offsetVector.getAccessor().get(valueCount);
        estElementCount = roundUp(entryCount, valueCount);
        if (v.getField().getType().getMinorType() == MinorType.MAP) {

          // For map, the only data associated with the map vector
          // itself is the offset vector, if any.

          dataSize = offsetVector.getPayloadByteCount(valueCount);
        } else {
          dataSize = v.getPayloadByteCount(valueCount);
        }
      } else {
        if (v.getField().getType().getMinorType() == MinorType.MAP) {
          dataSize = 0;
        } else {
          dataSize = v.getPayloadByteCount(valueCount);
        }
        entryCount = valueCount;
        estElementCount = 1;
      }

      estSize = roundUp(dataSize, valueCount);
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append(prefix)
          .append(metadata.getName())
          .append("(type: ")
          .append(metadata.getType().getMode().name())
          .append(" ")
          .append(metadata.getType().getMinorType().name())
          .append(", count: ")
          .append(valueCount);
      if (metadata.getDataMode() == DataMode.REPEATED) {
        buf.append(", total entries: ")
           .append(entryCount)
           .append(", per-array: ")
           .append(estElementCount);
      }
      buf .append(", actual size: ")
          .append(estSize)
          .append(", data size: ")
          .append(dataSize)
          .append(")");
      return buf.toString();
    }

    public void buildAllocHelper(SmartAllocationHelper allocHelper) {
      int width = 0;
      switch(metadata.getType().getMinorType()) {
      case VAR16CHAR:
      case VARBINARY:
      case VARCHAR:

        // Subtract out the offset vector width
        width = estSize - 4;

        // Subtract out the bits (is-set) vector width
        if (metadata.getDataMode() == DataMode.OPTIONAL) {
          width -= 1;
        }
        break;
      default:
        break;
      }
      String name = prefix + metadata.getName();
      if (metadata.getDataMode() == DataMode.REPEATED) {
        if (width > 0) {
          allocHelper.variableWidthArray(name, width, estElementCount);
        } else {
          allocHelper.fixedWidthArray(name, estElementCount);
        }
      }
      else if (width > 0) {
        allocHelper.variableWidth(name, width);
      }
    }
  }

  private List<ColumnSize> columnSizes = new ArrayList<>();

  /**
   * Number of records (rows) in the batch.
   */
  private int rowCount;
  /**
   * Actual batch size summing all buffers used to store data
   * for the batch.
   */
  private int accountedMemorySize;
  /**
   * Actual row width computed by dividing total batch memory by the
   * record count.
   */
  private int grossRowWidth;
  /**
   * Actual row width computed by summing columns. Use this if the
   * vectors are partially full; prevents overestimating row width.
   */
  private int netRowWidth;
  private boolean hasSv2;
  private int sv2Size;
  private int avgDensity;

  private Set<BufferLedger> ledgers = Sets.newIdentityHashSet();

  private int netBatchSize;

  public RecordBatchSizer(RecordBatch batch) {
    this(batch,
         (batch.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) ?
         batch.getSelectionVector2() : null);
  }

  public RecordBatchSizer(VectorAccessible va) {
    this(va, null);
  }

  public RecordBatchSizer(VectorAccessible va, SelectionVector2 sv2) {
    rowCount = va.getRecordCount();
    for (VectorWrapper<?> vw : va) {
      measureColumn(vw.getValueVector(), "", rowCount);
    }

    for (BufferLedger ledger : ledgers) {
      accountedMemorySize += ledger.getAccountedSize();
    }

    if (rowCount > 0) {
      grossRowWidth = roundUp(accountedMemorySize, rowCount);
    }

    if (sv2 != null) {
      sv2Size = sv2.getBuffer(false).capacity();
      accountedMemorySize += sv2Size;
    }

    computeEstimates();
  }

  private void computeEstimates() {
    grossRowWidth = roundUp(accountedMemorySize, rowCount);
    netRowWidth = roundUp(netBatchSize, rowCount);
    avgDensity = roundUp(netBatchSize * 100, accountedMemorySize);
  }

  public void applySv2() {
    if (hasSv2) {
      return;
    }

    sv2Size = BaseAllocator.nextPowerOfTwo(2 * rowCount);
    accountedMemorySize += sv2Size;
    computeEstimates();
  }

  private void measureColumn(ValueVector v, String prefix, int valueCount) {

    ColumnSize colSize = new ColumnSize(v, prefix, valueCount);
    columnSizes.add(colSize);
    netBatchSize += colSize.dataSize;

    // Maps consume no size themselves. However, their contained
    // vectors do consume space, so visit columns recursively.

    if (v.getField().getType().getMinorType() == MinorType.MAP) {
       expandMap((AbstractMapVector) v, prefix + v.getField().getName() + ".", colSize.entryCount);
    } else {
      v.getLedgers(ledgers);
    }
  }

  private void expandMap(AbstractMapVector mapVector, String prefix, int valueCount) {
    for (ValueVector vector : mapVector) {
      measureColumn(vector, prefix, valueCount);
    }
  }

  public static int roundUp(int num, int denom) {
    if (denom == 0) {
      return 0;
    }
    return (int) Math.ceil((double) num / denom);
  }

  public int rowCount() { return rowCount; }
  public int grossRowWidth() { return grossRowWidth; }
  public int netRowWidth() { return netRowWidth; }
  public int actualSize() { return accountedMemorySize; }
  public boolean hasSv2() { return hasSv2; }
  public int avgDensity() { return avgDensity; }
  public int netSize() { return netBatchSize; }

  public static final int MAX_VECTOR_SIZE = 16 * 1024 * 1024; // 16 MiB

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("Actual batch schema & sizes {\n");
    for (ColumnSize colSize : columnSizes) {
      buf.append("  ");
      buf.append(colSize.toString());
      buf.append("\n");
    }
    buf.append( "  Records: " );
    buf.append(rowCount);
    buf.append(", Total size: ");
    buf.append(accountedMemorySize);
    buf.append(", Data size: ");
    buf.append(netBatchSize);
    buf.append(", Gross row width: ");
    buf.append(grossRowWidth);
    buf.append(", Net row width: ");
    buf.append(netRowWidth);
    buf.append(", Density: ");
    buf.append(avgDensity);
    buf.append("}");
    return buf.toString();
  }

  public SmartAllocationHelper buildAllocHelper() {
    SmartAllocationHelper allocHelper = new SmartAllocationHelper();
    for (ColumnSize colSize : columnSizes) {
      colSize.buildAllocHelper(allocHelper);
    }
    return allocHelper;
  }
}
