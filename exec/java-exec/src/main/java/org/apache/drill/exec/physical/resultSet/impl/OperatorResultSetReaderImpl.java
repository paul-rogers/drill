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

import org.apache.drill.exec.physical.resultSet.OperatorResultSetReader;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.IndirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class OperatorResultSetReaderImpl implements OperatorResultSetReader {

  public interface UpstreamSource {
    boolean next();
    int schemaVersion();
    VectorContainer batch();
    SelectionVector2 sv2();
    void release();
  }

  @VisibleForTesting
  protected enum State {
      START,
      PENDING,
      BATCH,
      DETACHED,
      EOF,
      CLOSED
  }

  private State state = State.START;
  private int priorSchemaVersion;
  private final UpstreamSource source;
  private RowSetReader rowSetReader;

  public OperatorResultSetReaderImpl(UpstreamSource source) {
    this.source = source;
  }

  @Override
  public TupleMetadata schema() {
    switch (state) {
      case CLOSED:
        return null;
      case START:
        if (!next()) {
          return null;
        }
        state = State.PENDING;
        break;
      default:
    }
    return rowSetReader.tupleSchema();
  }

  @Override
  public boolean next() {
    switch (state) {
      case PENDING:
        state = State.BATCH;
        return true;
      case BATCH:
        source.release();
        break;
      case CLOSED:
        throw new IllegalStateException("Reader is closed");
      case EOF:
        return false;
      case START:
        break;
      default:
        source.release();
    }
    if (!source.next()) {
      state = State.EOF;
      return false;
    }

    int sourceSchemaVersion = source.schemaVersion();
    Preconditions.checkState(sourceSchemaVersion > 0);
    Preconditions.checkState(priorSchemaVersion <= sourceSchemaVersion);
    state = State.BATCH;

    // If new schema, discard the old reader (if any, and create
    // a new one that matches the new schema. If not a new schema,
    // then the old reader is reused: it points to vectors which
    // Drill requires be the same vectors as the previous batch,
    // but with different buffers.
    boolean newSchema = state == State.START ||
        priorSchemaVersion != sourceSchemaVersion;
    if (newSchema) {
      rowSetReader = createRowSet().reader();
      priorSchemaVersion = sourceSchemaVersion;
    } else {
      rowSetReader.newBatch();
    }
    return true;
  }

  // TODO: Build the reader without the need for a row set
  private RowSet createRowSet() {
    VectorContainer container = source.batch();
    switch (container.getSchema().getSelectionVectorMode()) {
    case FOUR_BYTE:
      throw new IllegalArgumentException("Build from SV4 not yet supported");
    case NONE:
      return DirectRowSet.fromContainer(container);
    case TWO_BYTE:
      return IndirectRowSet.fromSv2(container, source.sv2());
    default:
      throw new IllegalStateException("Invalid selection mode");
    }
  }

  @Override
  public int schemaVersion() { return source.schemaVersion(); }

  @Override
  public RowSetReader reader() {
    Preconditions.checkState(state == State.BATCH, "Not in batch-ready state.");
    return rowSetReader;
  }

  @Override
  public void detach() {
    if (state == State.BATCH) {
      state = State.DETACHED;
    }
  }

  @Override
  public void close() {
    source.release();
    state = State.CLOSED;
  }

  @VisibleForTesting
  protected State state() { return state; }
}
