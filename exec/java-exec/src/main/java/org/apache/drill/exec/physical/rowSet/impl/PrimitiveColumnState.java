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

  public PrimitiveColumnState(ResultSetLoaderImpl resultSetLoader, PrimitiveColumnModel columnModel) {
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

  /**
   * Prepare the column for a new row batch. Clear the previous values.
   * If the previous batch created a look-ahead buffer, restore that to the
   * active vector so we start writing where we left off. Else, reset the
   * write position to the start.
   */

   public void startBatch() {
    switch (state) {
    case NEW_LOOK_AHEAD:

      // Column is new, was not exchanged with backup vector

      break;
    case LOOK_AHEAD:
      vector().exchange(backupVector);
      backupVector.clear();
      break;
    case HARVESTED:

      // Note: do not reset the writer: it is already positioned in the backup
      // vector from when we wrote the overflow row.

      vector().clear();
      allocateVector(vector());
      writer().reset(-1);
      break;
    case START:
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
    state = State.ACTIVE;
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
   * values of which should be copied to a new "look-ahead" vector
   */

  public void rollOver(int overflowIndex) {
    assert state == State.ACTIVE;

    // Remember the last write index for the original vector.

    int writeIndex = writer().lastWriteIndex();

    // Close out the active vector, setting the record count.
    // This will be replaced later when the batch is done, with the
    // final row count. Here we set the count to fill in missing values and
    // set offsets in preparation for carving off the overflow value, if any.

    writer().endWrite();

    // Switch buffers between the backup vector and the writer's output
    // vector. Done this way because writers are bound to vectors and
    // we wish to keep the binding.

    if (backupVector == null) {
      backupVector = TypeHelper.getNewVector(schema().schema(), resultSetLoader.allocator(), null);
    }
    allocateVector(backupVector);
    vector().exchange(backupVector);
    state = State.OVERFLOW;

    // Any overflow value(s) to copy?

    int newIndex = -1;
    if (writeIndex >= overflowIndex) {

      // Copy overflow values from the full vector to the new
      // look-ahead vector.

      newIndex = 0;
      for (int src = overflowIndex; src <= writeIndex; src++, newIndex++) {
        vector().copyEntry(newIndex, backupVector, src);
      }
    }

    // Tell the writer that it has a new buffer and that it should reset
    // its last write index depending on whether data was copied or not.

    writer().reset(newIndex);
  }

  /**
   * Writing of a row batch is complete. Prepare the vector for harvesting
   * to send downstream. If this batch encountered overflow, set aside the
   * look-ahead vector and put the full vector buffer back into the active
   * vector.
   */

  public void harvest() {
    switch (state) {
    case OVERFLOW:
      vector().exchange(backupVector);
      state = State.LOOK_AHEAD;
      break;

    case ACTIVE:

      // If added after overflow, no data to save from the complete
      // batch: the vector does not appear in the completed batch.

      if (addVersion > resultSetLoader.schemaVersion()) {
        state = State.NEW_LOOK_AHEAD;
        return;
      }
      writer().endWrite();
      state = State.HARVESTED;
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  public void reset() {
    vector().clear();
    if (backupVector != null) {
      backupVector.clear();
      backupVector = null;
    }
  }
}

