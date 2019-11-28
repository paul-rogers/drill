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
package org.apache.drill.exec.physical.resultSet.impl;

import org.apache.drill.exec.physical.impl.protocol.BatchAccessor;
import org.apache.drill.exec.physical.resultSet.ResultSetReader;
import org.apache.drill.exec.physical.resultSet.model.ReaderBuilder;
import org.apache.drill.exec.physical.resultSet.model.ReaderIndex;
import org.apache.drill.exec.physical.resultSet.model.single.DirectRowIndex;
import org.apache.drill.exec.physical.resultSet.model.single.SimpleReaderBuilder;
import org.apache.drill.exec.physical.rowSet.HyperRowIndex;
import org.apache.drill.exec.physical.rowSet.IndirectRowIndex;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.physical.rowSet.RowSetReaderImpl;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class ResultSetReaderImpl implements ResultSetReader {

  @VisibleForTesting
  protected enum State {
      START,
      BATCH,
      DETACHED,
      CLOSED
  }

  private State state = State.START;
  private int priorSchemaVersion;
  private final BatchAccessor batch;
  private RowSetReaderImpl rowSetReader;

  public ResultSetReaderImpl(BatchAccessor batch) {
    this.batch = batch;
  }

  @Override
  public void start() {
    Preconditions.checkState(state != State.CLOSED, "Reader is closed");
    Preconditions.checkState(state != State.BATCH,
        "Call detach/release before starting another batch");
    Preconditions.checkState(state == State.START ||
        priorSchemaVersion <= batch.schemaVersion());
    boolean newSchema = state == State.START ||
        priorSchemaVersion != batch.schemaVersion();
    state = State.BATCH;

    // If new schema, discard the old reader (if any, and create
    // a new one that matches the new schema. If not a new schema,
    // then the old reader is reused: it points to vectors which
    // Drill requires be the same vectors as the previous batch,
    // but with different buffers.

    if (newSchema) {
      rowSetReader = ReaderBuilder.buildReader(batch);
      priorSchemaVersion = batch.schemaVersion();
    } else {
      rowSetReader.newBatch();
      rebind();
    }
  }

  /**
   * A somewhat awkward method to reset the current reader index.
   * The index differs in form for each kind of SV. If the reader
   * has the correct index, reset it in a way unique to each kind
   * of SV. Else (for the non-hyper case), replace the index with
   * the right kind.
   */

  private void rebind() {
    ReaderIndex currentIndex = rowSetReader.index();
    switch (batch.schema().getSelectionVectorMode()) {
    case FOUR_BYTE:
      ((HyperRowIndex) currentIndex).bind(batch.selectionVector4());
      return;
    case NONE:
      if (currentIndex instanceof DirectRowIndex) {
        ((DirectRowIndex) currentIndex).resetRowCount();
        return;
      }
      break;
    case TWO_BYTE:
      if (currentIndex instanceof IndirectRowIndex) {
        ((IndirectRowIndex) currentIndex).bind(batch.selectionVector2());
        return;
      }
      break;
    default:
      throw new IllegalStateException();
    }
    rowSetReader.bindIndex(SimpleReaderBuilder.readerIndex(batch));
  }

  @Override
  public RowSetReader reader() {
    Preconditions.checkState(state == State.BATCH, "Call start() before requesting the reader.");
    return rowSetReader;
  }

  @Override
  public void detach() {
    if (state != State.START) {
      Preconditions.checkState(state == State.BATCH || state == State.DETACHED);
      state = State.DETACHED;
    }
  }

  @Override
  public void release() {
    if (state != State.START && state != State.DETACHED) {
      detach();
      batch.release();
    }
  }

  @Override
  public void close() {
    if (state != State.CLOSED) {
      release();
      state = State.CLOSED;
    }
  }

  @VisibleForTesting
  protected State state() { return state; }

  @Override
  public BatchAccessor inputBatch() { return batch; }
}
