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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.OffsetVectorState;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.ColumnCoordinator;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

public abstract class ColumnState implements ColumnCoordinator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnState.class);

  public static class MapColumnState extends ColumnState {

    @SuppressWarnings("unused")
    private final MapColumnModel columnModel;

    public MapColumnState(ResultSetLoaderImpl resultSetLoader, MapColumnModel columnModel) {
      super(resultSetLoader, new NullVectorState());
      this.columnModel = columnModel;
    }

    @Override
    public void overflowed(AbstractSingleColumnModel model) {
      // Should never occur: map columns can't overflow
      // (only the content columns are subject to overflow.)

      throw new IllegalStateException();
    }

    @Override
    public void startBatch() { }

    @Override
    public void rollOver(int sourceStartIndex) { }
  }

  public static class MapArrayColumnState extends ColumnState {

    @SuppressWarnings("unused")
    private final MapColumnModel columnModel;
    protected SingleVectorState offsetsState;

    public MapArrayColumnState(ResultSetLoaderImpl resultSetLoader, MapColumnModel columnModel) {
      super(resultSetLoader,
          new OffsetVectorState(
              ((AbstractArrayWriter) columnModel.writer().array()).offsetWriter(),
              ((RepeatedMapVector) columnModel.vector()).getOffsetVector()));
      this.columnModel = columnModel;
    }

    @Override
    public void overflowed(AbstractSingleColumnModel model) {

      // Should never occur: map columns can't overflow
      // (only the content columns are subject to overflow.)

      throw new IllegalStateException();
    }

    public int offsetAt(int index) {
      return ((OffsetVectorState) vectorState).offsetAt(index);
    }
  }

  protected enum State {

    /**
     * Column is in the normal state of writing with no overflow
     * in effect.
     */

    NORMAL,

    /**
     * Like NORMAL, but means that the data has overflowed and the
     * column's data for the current row appears in the new,
     * overflow batch. For a client that omits some columns, written
     * columns will be in OVERFLOW state, unwritten columns in
     * NORMAL state.
     */

    OVERFLOW,

    /**
     * Indicates that the column has data saved
     * in the overflow batch.
     */

    LOOK_AHEAD,

    /**
     * Like LOOK_AHEAD, but indicates the special case that the column
     * was added after overflow, so there is no vector for the column
     * in the harvested batch.
     */

    NEW_LOOK_AHEAD
  }

  protected final ResultSetLoaderImpl resultSetLoader;
  protected final int addVersion;
  protected final VectorState vectorState;
  protected State state;
  protected int outerCardinality;

  public ColumnState(ResultSetLoaderImpl resultSetLoader, VectorState vectorState) {
    this.resultSetLoader = resultSetLoader;
    this.vectorState = vectorState;
    this.addVersion = resultSetLoader.bumpVersion();
    state = resultSetLoader.hasOverflow() ?
        State.NEW_LOOK_AHEAD : State.NORMAL;
  }

  public void setCardinality(int outerCardinality) {
    this.outerCardinality = outerCardinality;
  }

  public void allocateVectors() {
    assert outerCardinality != 0;
    vectorState.allocate(outerCardinality);
  }

  /**
   * Prepare the column for a new row batch after overflow on the previous
   * batch. Restore the look-ahead buffer to the
   * active vector so we start writing where we left off.
   */

  public void startBatch() {
    switch (state) {
    case NORMAL:
      vectorState.allocate(outerCardinality);
      break;

    case NEW_LOOK_AHEAD:

      // Column is new, was not exchanged with backup vector

      break;

    case LOOK_AHEAD:

      // Restore the look-ahead values to the main vector.

      vectorState.startBatchWithLookAhead();
      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // In all cases, we are back to normal writing.

    state = State.NORMAL;
  }

  /**
   * A vector overflowed. Move values for the current row from the main vector
   * to the overflow vector.
   *
   * @param valueIndex index within the vector of the values to roll
   * over
   * @param outerCardinality number of values to allocate in the look-ahead
   * vector
   */

  public void rollOver(int sourceStartIndex) {
    assert state == State.NORMAL;

    // If the source index is 0, then we could not fit this one
    // value in the original vector. Nothing will be accomplished by
    // trying again with an an overflow vector. Just fail.
    //
    // Note that this is a judgment call. It is possible to allow the
    // vector to double beyond the limit, but that will require a bit
    // of thought to get right -- and, of course, completely defeats
    // the purpose of limiting vector size to avoid memory fragmentation...

    if (sourceStartIndex == 0) {
      throw UserException
        .memoryError("A single column value is larger than the maximum allowed size of 16 MB")
        .build(logger);
    }

    // Otherwise, do the roll-over to a look-ahead vector.

    vectorState.rollOver(sourceStartIndex, outerCardinality);

    // Remember that we did this overflow processing.

    state = State.OVERFLOW;
  }

  /**
   * Writing of a row batch is complete. Prepare the vector for harvesting
   * to send downstream. If this batch encountered overflow, set aside the
   * look-ahead vector and put the full vector buffer back into the active
   * vector.
   */

  public void harvestWithLookAhead() {
    switch (state) {
    case NEW_LOOK_AHEAD:

      // If added after overflow, no data to save from the complete
      // batch: the vector does not appear in the completed batch.

      break;

    case OVERFLOW:

      // Otherwise, restore the original, full buffer and
      // last write position.

      vectorState.harvestWithLookAhead();

      // Remember that we have look-ahead values stashed away in the
      // backup vector.

      state = State.LOOK_AHEAD;
      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

//  public void allocateOffsetVector(ValueVector toAlloc, int newLength) {
//    AllocationHelper.allocate(toAlloc, newLength, 0, 1);
//  }

  public void reset() {
    vectorState.reset();
  }
}
