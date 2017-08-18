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

import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.OffsetVectorState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.ValuesVectorState;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Vector state for a scalar array (repeated scalar) vector. Manages both the
 * offsets vector and data vector during overflow and other operations.
 */

public class RepeatedVectorState implements VectorState {
  private final ColumnMetadata schema;
  private final AbstractArrayWriter arrayWriter;
  private final OffsetVectorState offsetsState;
  private final ValuesVectorState valuesState;

  public RepeatedVectorState(AbstractSingleColumnModel columnModel) {
    this.schema = columnModel.schema();

    // Get the repeated vector

    @SuppressWarnings("resource")
    RepeatedValueVector vector = (RepeatedValueVector) columnModel.vector();

    // Create the offsets state with the offset vector portion of the repeated
    // vector, and the offset writer portion of the array writer.

    arrayWriter = (AbstractArrayWriter) columnModel.writer().array();
    offsetsState = new OffsetVectorState(arrayWriter.offsetWriter(), vector.getOffsetVector());

    // Then, create the values state using the value (data) portion of the repeated
    // vector, and the scalar (value) portion of the array writer.

    AbstractScalarWriter colWriter = (AbstractScalarWriter) arrayWriter.scalar();
    valuesState = new ValuesVectorState(schema, colWriter, vector.getDataVector());
  }

  @Override
  public void allocate(int cardinality) {
    offsetsState.allocate(cardinality);
    valuesState.allocate(childCardinality(cardinality));
  }

  private int childCardinality(int cardinality) {
    return cardinality * schema.expectedElementCount();
  }

  /**
   * The column is a scalar or an array of scalars. We need to roll over both the column
   * values and the offsets that point to those values. The index provided is
   * the index into the offset vector. We use that to obtain the index of the
   * values to roll-over.
   * <p>
   * Data structure:
   * <p><pre></code>
   * PrimitiveColumnModel
   * +- PrimitiveColumnState
   * .  +- RepeatedVectorState (this class)
   * .  .  +- OffsetVectorState
   * .  .  .  +- OffsetVectorWriter (A)
   * .  .  .  +- Offset vector (B)
   * .  .  .  +- Backup (e.g. look-ahead) offset vector
   * .  .  +- ValuesVectorState
   * .  .  .  +- Scalar (element) writer (C)
   * .  .  .  +- Data (elements) vector (D)
   * .  .  .  +- Backup elements vector
   * +- Array Writer
   * .  +- ColumnWriterIndex (for array as a whole)
   * .  +- OffsetVectorWriter (A)
   * .  .  +- Offset vector (B)
   * .  +- ArrayElementWriterIndex
   * .  +- ScalarWriter (D)
   * .  .  +- ArrayElementWriterIndex
   * .  .  +- Scalar vector (D)
   * </code></pre>
   * <p>
   * The top group of objects point into the writer objects in the second
   * group. Letters in parens show the connections.
   * <p>
   * To perform the roll-over, we must:
   * <ul>
   * <li>Copy values from the current vectors to a set of new, look-ahead
   * vectors.</li>
   * <li>Swap buffers between the main and "backup" vectors, effectively
   * moving the "full" batch to the sidelines, putting the look-ahead vectors
   * into play in order to finish writing the current row.</li>
   * <li>Update the writers to point to the look-ahead buffers, including
   * the initial set of data copied into those vectors.</li>
   * <li>Update the vector indexes to point to the next write positions
   * after the values copied during roll-over.</li>
   * </ul>
   *
   * @param sourceStartIndex index into the offset vector for the array
   * @param cardinality the number of outer elements to create in the look-ahead
   * vector
   */

  @Override
  public int rollOver(int sourceStartIndex, int cardinality) {

    // Obtain the start of the data portion of the overflow. Do
    // this BEFORE swapping out the offset vector. If we are at row
    // n, then the offset at row n is the start position of data for
    // row n.

    int dataStartIndex = offsetsState.offsetAt(sourceStartIndex);

    // Swap out the two vectors. The index presented to the caller
    // is that of the data vector: the next position in the data
    // vector to be set into the data vector writer index.

    int newIndex = valuesState.rollOver(dataStartIndex, childCardinality(cardinality));
    offsetsState.rollOver(sourceStartIndex, cardinality);

    // The array introduces a new vector index level for the
    // (one and only) child writer. Adjust that vector index to point to the
    // next array write position. This position must reflect any array entries
    // already written.

    arrayWriter.resetElementIndex(newIndex);
    return newIndex;
  }

  @Override
  public void harvestWithLookAhead() {
    offsetsState.harvestWithLookAhead();
    valuesState.harvestWithLookAhead();
  }

  @Override
  public void startBatchWithLookAhead() {
    offsetsState.startBatchWithLookAhead();
    valuesState.startBatchWithLookAhead();
  }

  @Override
  public void reset() {
    offsetsState.reset();
    valuesState.reset();
  }
}