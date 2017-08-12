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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;

public class PrimitiveColumnState extends ColumnState {

  private final PrimitiveColumnModel columnModel;
  private ValueVector backupVector;
  private int backupLastWritePosition;

  public PrimitiveColumnState(ResultSetLoaderImpl resultSetLoader,
      PrimitiveColumnModel columnModel) {
    super(resultSetLoader);
    this.columnModel = columnModel;
  }

  @Override
  public void overflowed(AbstractSingleColumnModel model) {
    assert columnModel == model;
    resultSetLoader.overflowed();
  }

  public ValueVector vector() {
    return columnModel.vector();
  }

  public AbstractScalarWriter writer() {
    return (AbstractScalarWriter) columnModel.writer().scalar();
  }

  public ColumnMetadata schema() {
    return columnModel.schema();
  }

  public void allocateVector(ValueVector toAlloc) {
    AllocationHelper.allocate(toAlloc, resultSetLoader.initialRowCount(),
        schema().expectedWidth(), schema().expectedElementCount());
  }

  /**
   * A column within the row batch overflowed. Prepare to absorb the rest of
   * the in-flight row by rolling values over to a new vector, saving the
   * complete vector for later. This column could have a value for the overflow
   * row, or for some previous row, depending on exactly when and where the
   * overflow occurs.
   *
   * @param overflowIndex the index of the row that caused the overflow, the
   * values of which should be copied to a new "look-ahead" vector. If the
   * vector is an array, then the overflowIndex is the position of the first
   * element to be moved, and multiple elements may need to move
   */

  public void rollOver(int overflowIndex) {
    assert state == State.NORMAL;

    // Remember the last write index for the original vector.

    int writeIndex = writer().lastWriteIndex();

    // The last write index for the "full" batch is capped at
    // the value before overflow.

    backupLastWritePosition = Math.max(writeIndex, overflowIndex - 1);

    // Switch buffers between the backup vector and the writer's output
    // vector. Done this way because writers are bound to vectors and
    // we wish to keep the binding.

    if (backupVector == null) {
      backupVector = TypeHelper.getNewVector(schema().schema(), resultSetLoader.allocator(), null);
    }
    allocateVector(backupVector);
    vector().exchange(backupVector);

    // Any overflow value(s) to copy?

    int newIndex = -1;

    // Copy overflow values from the full vector to the new
    // look-ahead vector.

    for (int src = overflowIndex; src <= writeIndex; src++, newIndex++) {
      vector().copyEntry(newIndex + 1, backupVector, src);
    }

    // Tell the writer that it has a new buffer and that it should reset
    // its last write index depending on whether data was copied or not.

    writer().reset(newIndex);

    // At this point, the writer is positioned to write to the look-ahead
    // vector at the position after the copied values. The original vector
    // is saved along with a last write position that is no greater than
    // the retained values.

    // Remember that we did this overflow processing.

    state = State.OVERFLOW;
  }

  /**
   * Writing of a row batch is complete. Prepare the vector for harvesting
   * to send downstream. If this batch encountered overflow, set aside the
   * look-ahead vector and put the full vector buffer back into the active
   * vector.
   */

  public void harvestWithOverflow() {
    switch (state) {
    case NEW_LOOK_AHEAD:

      // If added after overflow, no data to save from the complete
      // batch: the vector does not appear in the completed batch.

      break;

    case OVERFLOW:

      // Otherwise, restore the original, full buffer and
      // last write position.

      vector().exchange(backupVector);
      int temp = backupLastWritePosition;
      backupLastWritePosition = writer().lastWriteIndex();
      writer().reset(temp);

      // Remember that we have look-ahead values stashed away in the
      // backup vector.

      state = State.LOOK_AHEAD;
      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  /**
   * Prepare the column for a new row batch after overflow on the previous
   * batch. Restore the look-ahead buffer to the
   * active vector so we start writing where we left off.
   */

  public void startOverflowBatch() {
    switch (state) {
    case NEW_LOOK_AHEAD:

      // Column is new, was not exchanged with backup vector

      break;

    case LOOK_AHEAD:

      // Restore the look-ahead values to the main vector.

      vector().exchange(backupVector);
      backupVector.clear();
      writer().reset(backupLastWritePosition);
      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // In either case, we are back to normal writing.

    state = State.NORMAL;
  }

  public void reset() {
    vector().clear();
    if (backupVector != null) {
      backupVector.clear();
      backupVector = null;
    }
  }
}

