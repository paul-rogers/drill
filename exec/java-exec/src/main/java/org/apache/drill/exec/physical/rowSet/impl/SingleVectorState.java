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
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;

/**
 * Base class for a single vector. Handles the bulk of work for that vector.
 * Subclasses are specialized for offset vectors or values vectors.
 */

public abstract class SingleVectorState implements VectorState {

  /**
   * State for a scalar value vector. The vector might be for a simple (non-array)
   * vector, or might be the payload part of a scalar array (repeated scalar)
   * vector.
   */

  public static class ValuesVectorState extends SingleVectorState {

    private final ColumnMetadata schema;

    public ValuesVectorState(ColumnMetadata schema, AbstractScalarWriter writer, ValueVector mainVector) {
      super(writer, mainVector);
      this.schema = schema;
    }

    @Override
    public void allocateVector(ValueVector vector, int cardinality) {
      if (schema.isVariableWidth()) {

        // Cap the allocated size to the maximum.

        int size = Math.min(ValueVector.MAX_BUFFER_SIZE, cardinality * schema.expectedWidth());
        ((VariableWidthVector) vector).allocateNew(size, cardinality);
      } else {
        ((FixedWidthVector) vector).allocateNew(cardinality);
      }
    }

    @Override
    protected int copyOverflow(int sourceStartIndex, int sourceEndIndex) {

      // Copy overflow values from the full vector to the new
      // look-ahead vector.

      int newIndex = 0;
      for (int src = sourceStartIndex; src <= sourceEndIndex; src++, newIndex++) {
        mainVector.copyEntry(newIndex, backupVector, src);
      }
      return newIndex;
    }
  }

  /**
   * Special case for an offset vector. Offset vectors are managed like any other
   * vector with respect to overflow and allocation. This means that the loader
   * classes avoid the use of the RepeatedVector class methods, instead working
   * with the offsets vector (here) or the values vector to allow the needed
   * fine control over overflow operations.
   */

  public static class OffsetVectorState extends SingleVectorState {

    public OffsetVectorState(AbstractScalarWriter writer, ValueVector mainVector) {
      super(writer, mainVector);
    }

    public int offsetAt(int sourceStartIndex) {
      return ((UInt4Vector) mainVector).getAccessor().get(sourceStartIndex);
    }

    @Override
    public void allocateVector(ValueVector toAlloc, int cardinality) {
      ((UInt4Vector) toAlloc).allocateNew(cardinality);
    }

    @Override
    protected int copyOverflow(int sourceStartIndex, int sourceEndIndex) {

      // Copy overflow values from the full vector to the new
      // look-ahead vector. Since this is an offset vector, values must
      // be adjusted as they move across.
      //
      // Indexing can be confusing. Offset vectors have values offset
      // from their row by one position. The offset vector position for
      // row i has the start value for row i. The offset vector position for
      // i+1 has the start of the next value. The difference between the
      // two is the element length. As a result, the offset vector always has
      // one more value than the number of rows, and position 0 is always 0.
      //
      // The index passed in here is that of the row that overflowed. That
      // offset vector position contains the offset of the start of the data
      // for the current row. We must subtract that offset from each copied
      // value to adjust the offset for the destination.

      UInt4Vector.Accessor sourceAccessor = ((UInt4Vector) backupVector).getAccessor();
      UInt4Vector.Mutator destMutator = ((UInt4Vector) mainVector).getMutator();
      int offset = sourceAccessor.get(sourceStartIndex);

      // Position zero is special and will be filled in by the writer
      // later.

      int newIndex = 0;
      for (int src = sourceStartIndex; src <= sourceEndIndex; src++, newIndex++) {
        destMutator.set(newIndex + 1, sourceAccessor.get(src) - offset);
      }
      return newIndex;
    }
  }

  private final AbstractScalarWriter writer;
  protected final ValueVector mainVector;
  protected ValueVector backupVector;
  private int fullVectorLastWritePosition;
  private int overflowVectorLastWritePosition;

  public SingleVectorState(AbstractScalarWriter writer, ValueVector mainVector) {
    this.writer = writer;
    this.mainVector = mainVector;
  }

  @Override
  public void allocate(int cardinality) {
    allocateVector(mainVector, cardinality);
  }

  protected abstract void allocateVector(ValueVector vector, int cardinality);

  /**
   * A column within the row batch overflowed. Prepare to absorb the rest of
   * the in-flight row by rolling values over to a new vector, saving the
   * complete vector for later. This column could have a value for the overflow
   * row, or for some previous row, depending on exactly when and where the
   * overflow occurs.
   *
   * @param sourceStartIndex the index of the row that caused the overflow, the
   * values of which should be copied to a new "look-ahead" vector. If the
   * vector is an array, then the overflowIndex is the position of the first
   * element to be moved, and multiple elements may need to move
   */

  @Override
  public int rollOver(int sourceStartIndex, int cardinality) {

    // Remember the last write index for the original vector.
    // This tells us the end of the set of values to move, while the
    // sourceStartIndex above tells us the start.

    int sourceEndIndex = writer.lastWriteIndex();

    // The last write index for the "full" batch is capped at
    // the value before overflow.

    fullVectorLastWritePosition = Math.min(sourceEndIndex, sourceStartIndex - 1);

    // Switch buffers between the backup vector and the writer's output
    // vector. Done this way because writers are bound to vectors and
    // we wish to keep the binding.

    if (backupVector == null) {
      backupVector = TypeHelper.getNewVector(mainVector.getField(), mainVector.getAllocator(), null);
    }
    assert cardinality > 0;
    allocateVector(backupVector, cardinality);
    mainVector.exchange(backupVector);

    // Copy overflow values from the full vector to the new
    // look-ahead vector.

    int newIndex = copyOverflow(sourceStartIndex, sourceEndIndex);

    // Tell the writer that it has a new buffer and that it should reset
    // its last write index depending on whether data was copied or not.

    writer.startWriteAt(newIndex - 1);

    // At this point, the writer is positioned to write to the look-ahead
    // vector at the position after the copied values. The original vector
    // is saved along with a last write position that is no greater than
    // the retained values.

    // Return the next writer index which is one more than the last
    // write position.

    return newIndex;
  }

  protected abstract int copyOverflow(int sourceStartIndex, int sourceEndIndex);

  /**
    * Exchange the data from the backup vector and the main vector, putting
    * the completed buffers back into the main vectors, and stashing the
    * overflow buffers away in the backup vector.
    * Restore the main vector's last write position.
    */

  @Override
  public void harvestWithLookAhead() {
    mainVector.exchange(backupVector);
    overflowVectorLastWritePosition = writer.lastWriteIndex();
    writer.startWriteAt(fullVectorLastWritePosition);
    fullVectorLastWritePosition = -1;
  }

  /**
   * The previous full batch has been sent downstream and the client is
   * now ready to start writing to the next batch. Initialize that new batch
   * with the look-ahead values saved during overflow of the previous batch.
   */

  @Override
  public void startBatchWithLookAhead() {
    mainVector.exchange(backupVector);
    backupVector.clear();
    writer.startWriteAt(overflowVectorLastWritePosition);
    overflowVectorLastWritePosition = -1;
  }

  @Override
  public void reset() {
    mainVector.clear();
    if (backupVector != null) {
      backupVector.clear();
    }
    overflowVectorLastWritePosition = -1;
    fullVectorLastWritePosition = -1;
  }
}