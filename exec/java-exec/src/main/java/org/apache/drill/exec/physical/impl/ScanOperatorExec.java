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
package org.apache.drill.exec.physical.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.physical.impl.OperatorRecordBatch.BatchAccessor;
import org.apache.drill.exec.physical.impl.OperatorRecordBatch.OperatorExec;
import org.apache.drill.exec.physical.impl.OperatorRecordBatch.VectorContainerAccessor;
import org.apache.drill.exec.physical.rowSet.RowSetMutator;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.RowSetMutatorImpl;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.RowReader;

/**
 * Implementation of the revised scan operator that uses a mutator aware of
 * batch sizes. This is the successor to {@link ScanBatch} and should be used
 * by all new scan implementations.
 * <p>
 * Acts as an adapter between the operator protocol and the row reader
 * protocol. Provides the row set mutator used to construct record batches.
 * <p>
 * Provides the option to continue a schema from one batch to the next.
 * This can reduce spurious schema changes in formats, such as JSON, with
 * varying fields. It is not, however, a complete solution as the outcome
 * still depends on the order of file scans and the division of files across
 * readers.
 * <p>
 * Provides the option to infer the schema from the first batch. The "quick path"
 * to obtain the schema will read one batch, then use that schema as the returned
 * schema, returning the full batch in the next call to <tt>next()</tt>.
 * <p>
 * Error handling in this class is minimal: the enclosing record batch iterator
 * is responsible for handling exceptions. Error handling relies on the fact
 * that the iterator will call <tt>close()</tt> regardless of which exceptions
 * are thrown.
 */

public class ScanOperatorExec implements OperatorExec {

  public static class ImplicitColumn {
    public final String colName;
    public final String value;

    public ImplicitColumn(String colName, String value) {
      this.colName = colName;
      this.value = value;
    }
  }

  public static class ScanOptions {
    public List<ImplicitColumn> implicitColumns;
    public boolean restartSchemaForEachFile;
  }

  private enum State { START, SCHEMA, SCHEMA_EOF, READER, READER_EOF, END, FAILED, CLOSED }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanOperatorExec.class);

  private State state = State.START;
  private final OperatorExecContext context;
  private final Iterator<RowReader> readers;
  private final ScanOptions options;
  private final VectorContainerAccessor containerAccessor = new VectorContainerAccessor();
  private RowSetMutator rowSetMutator;
  private RowReader reader;
  private int readerCount;

  public ScanOperatorExec(OperatorExecContext context,
                          Iterator<RowReader> readers,
                          List<Map<String, String>> implicitColumns) {
    this(context, readers,
        convertImplicitCols(implicitColumns));
  }

  public ScanOperatorExec(OperatorExecContext context,
            Iterator<RowReader> readers,
            ScanOptions options) {
    this.context = context;
    this.readers = readers;
    this.options = options;
  }

  public static ScanOptions convertImplicitCols(List<Map<String, String>> implicitColumns) {
    List<ImplicitColumn> newForm = new ArrayList<>();
    for ( Map<String, String> map : implicitColumns ) {
      for (Map.Entry<String, String> entry : map.entrySet()) {
        newForm.add(new ImplicitColumn(entry.getKey(), entry.getValue()));
      }
    }
    ScanOptions options = new ScanOptions();
    options.restartSchemaForEachFile = true;
    options.implicitColumns = newForm;
    return options;
  }

  @Override
  public void bind() { }

  @Override
  public BatchAccessor batchAccessor() { return containerAccessor; }

  @Override
  public void start() { }

  @Override
  public boolean buildSchema() {
    assert state == State.START;
    nextBatch();
    switch (state) {
    case READER:
      state = State.SCHEMA;
      return true;
    case READER_EOF:
      state = State.SCHEMA_EOF;
      return true;
    case END:
      if (readerCount == 0) {
        // return false; // When empty batches are supported
        throw UserException.executionError(
            new ExecutionSetupException("A scan batch must contain at least one reader."))
          .build(logger);
      }
      return false;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  @Override
  public boolean next() {
    try {
      switch (state) {
      case SCHEMA:
        // Use batch previously read.
        state = State.READER;
        return true;
      case SCHEMA_EOF:
        state = State.READER_EOF;
        return true;
      case READER:
      case READER_EOF:
        break;
      case END:
        return false;
      default:
        throw new IllegalStateException("Unexpected state: " + state);
      }

      nextBatch();
      return state != State.END;
    } finally {
      state = State.FAILED;
    }
  }

  private void nextBatch() {
    for (;;) {
      if (state == State.READER_EOF) {
        closeReader();
        reader = readers.next();
        if (reader == null) {
          state = State.END;
          return;
        }
        openReader();
        state = State.READER;
      }
      if (readBatch()) {
        return;
      }
      state = State.READER_EOF;
    }
  }

  private void openReader() {
    readerCount++;
    if (rowSetMutator == null) {
      rowSetMutator = new RowSetMutatorImpl(context.getAllocator());
    }
    try {
      reader.open(context, rowSetMutator);
    } catch (UserException e) {
      reader = null;
      throw e;
    } catch (Throwable t) {
      reader = null;
      throw UserException.executionError(t)
        .addContext("Open failed for reader", reader.getClass().getSimpleName())
        .build(logger);
    }
  }

  private boolean readBatch() {
    try {
      rowSetMutator.startBatch();
      if (!reader.next()) {
        rowSetMutator.close();
        return false;
      }
    } catch (UserException e) {
      throw e;
    } catch (Throwable t) {
      throw UserException.executionError(t)
        .addContext("Read failed for reader", reader.getClass().getSimpleName())
        .build(logger);
    }
    buildOutputBatch();
    return true;
  }

  private void buildOutputBatch() {
    VectorContainer container = rowSetMutator.harvest();
    if (options.implicitColumns != null) {
      container = mergeImplicitColumns(container);
    }
    containerAccessor.setContainer(container, rowSetMutator.schemaVersion());
  }

  private VectorContainer mergeImplicitColumns(VectorContainer container) {
    List<ImplicitColumn> implicitCols = options.implicitColumns;
    int colCount = implicitCols.size();
    if (colCount == 0) {
      return container;
    }
    RowSetMutator overlay = new RowSetMutatorImpl(context.getAllocator());
    overlay.startBatch();
    TupleLoader writer = overlay.writer();
    int rowCount = container.getRecordCount();
    for (int i = 0; i < colCount; i++) {
      ImplicitColumn col = implicitCols.get(i);
      MajorType type = MajorType.newBuilder()
          .setMinorType(MinorType.VARCHAR)
          .setMode(DataMode.REQUIRED)
          .setPrecision(col.value.length())
          .build();
      MaterializedField colSchema = MaterializedField.create(col.colName, type);
      writer.schema().addColumn(colSchema);
    }
    for (int i = 0; i < rowCount; i++) {
      overlay.startRow();
      for (int j = 0; j < colCount; j++) {
        writer.column(j).setString(implicitCols.get(j).value);
      }
      overlay.saveRow();
    }
    VectorContainer implicitContainer = overlay.harvest();
    return container.merge(implicitContainer);
  }

  private void closeReader() {
    if (options.restartSchemaForEachFile) {
      rowSetMutator.close();
      rowSetMutator = null;
    }
    if (reader != null) {
      try {
        reader.close();
        reader = null;
      } catch (UserException e) {
        reader = null;
        throw e;
      } catch (Throwable t) {
        reader = null;
        throw UserException.executionError(t)
          .addContext("Close failed for reader", reader.getClass().getSimpleName())
          .build(logger);
      }
    }
  }

  @Override
  public void cancel() {
    switch (state) {
    case FAILED:
    case CLOSED:
      break;
    default:
      state = State.FAILED;
      closeAll();
    }
  }

  @Override
  public void close() {
    if (state == State.CLOSED) {
      return;
    }
    state = State.CLOSED;
    closeAll();
  }

  private void closeAll() {

    // Will not throw exceptions

    rowSetMutator.close();

    // May throw an unchecked exception

    closeReader();
  }
}
