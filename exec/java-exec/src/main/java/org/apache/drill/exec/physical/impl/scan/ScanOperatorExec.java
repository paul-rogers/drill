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
package org.apache.drill.exec.physical.impl.scan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.BatchAccessor;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.OperatorExec;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.OperatorExecServices;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.VectorContainerAccessor;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.SelectColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.TableColumn;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;

import com.google.common.annotations.VisibleForTesting;

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
 * <h4>Schema Versions</h4>
 * Readers may change schemas from time to time. To track such changes,
 * this implementation tracks a batch schema version, maintained by comparing
 * one schema with the next.
 * <p>
 * Readers can discover columns as they read data, such as with any JSON-based
 * format. In this case, the row set mutator also provides a schema version,
 * but a fine-grained one that changes each time a column is added.
 * <p>
 * The two schema versions serve different purposes and are not interchangeable.
 * For example, if a scan reads two files, both will build up their own schemas,
 * each increasing its internal version number as work proceeds. But, at the
 * end of each batch, the schemas may (and, in fact, should) be identical,
 * which is the schema version downstream operators care about.
 */

public class ScanOperatorExec implements OperatorExec {

  private static class SchemaNegotiatorImpl implements SchemaNegotiator {

    private final ScanOperatorExec opExec;
    private final List<SelectColumn> selection = new ArrayList<>();
    private final List<TableColumn> tableSchema = new ArrayList<>();

    private SchemaNegotiatorImpl(ScanOperatorExec opExec) {
      this.opExec = opExec;
    }

    @Override
    public void addSelectColumn(String name) {
      addSelectColumn(SchemaPath.getSimplePath(name));
    }

    @Override
    public void addSelectColumn(SchemaPath path) {
      addSelectColumn(path, ColumnType.ANY);
    }

    @Override
    public void addSelectColumn(SchemaPath path, ColumnType type) {

      // Can't yet handle column type
      // Always treated as ANY: the planner will sort out the meaning.

      selection.add(new SelectColumn(path));
    }

    @Override
    public void addTableColumn(String name, MajorType type) {
      addTableColumn(MaterializedField.create(name, type));
    }

    @Override
    public void addTableColumn(MaterializedField schema) {
      tableSchema.add(new TableColumn(tableSchema.size(), schema));
    }

    @Override
    public ResultSetLoader build() {
      ProjectionPlanner planner = new ProjectionPlanner(opExec.context.getFragmentContext().getOptionSet());
      if (opExec.options.selection != null) {
        if (! selection.isEmpty()) {
          throw new IllegalStateException("Select list provided by scan operator; cannot also be provided by reader");
        }
        planner.queryCols(opExec.options.selection);
      } else {
        planner.selection(selection);
      }
      planner.tableSchema(tableSchema);
      ScanProjection projection = planner.build();
      opExec.scanProjector = new ScanProjector.Builder(opExec.context.allocator(), projection)
          .build();
      return opExec.scanProjector.tableLoader();
    }
  }

  // TODO: Move into the projection planner

  public static class ScanOptions {
//    public List<ImplicitColumnDefn> implicitColumns;
//    public boolean reuseSchemaAcrossReaders;
    public List<SchemaPath> selection;
  }

  private enum State { START, NEXT_READER, LOOK_AHEAD, READER, READER_EOF, END, FAILED, CLOSED }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanOperatorExec.class);

  private State state = State.START;
  private OperatorExecServices context;
  private final Iterator<RowBatchReader> readers;
  private final ScanOptions options;
  private final VectorContainerAccessor containerAccessor = new VectorContainerAccessor();
  private ScanProjector scanProjector;
  private RowBatchReader reader;
  private int readerCount;

  private VectorContainer lookahead;

//  public ScanOperatorExec(Iterator<RowBatchReader> readers,
//                          List<Map<String, String>> implicitColumns) {
//    this(readers,
//        convertImplicitCols(implicitColumns));
//  }

  public ScanOperatorExec(Iterator<RowBatchReader> readers,
                          ScanOptions options) {
    this.readers = readers;
    this.options = options;
  }

  public ScanOperatorExec(Iterator<RowBatchReader> iterator) {
    this(iterator, new ScanOptions());
  }

//  public static ScanOptions convertImplicitCols(List<Map<String, String>> implicitColumns) {
//    List<ImplicitColumnDefn> newForm = new ArrayList<>();
//    for ( Map<String, String> map : implicitColumns ) {
//      for (Map.Entry<String, String> entry : map.entrySet()) {
//        newForm.add(new ImplicitColumnDefn(entry.getKey(), entry.getValue()));
//      }
//    }
//    ScanOptions options = new ScanOptions();
//    options.implicitColumns = newForm;
//    return options;
//  }

  @Override
  public void bind(OperatorExecServices context) {
    this.context = context;
  }

  @Override
  public BatchAccessor batchAccessor() { return containerAccessor; }

  @VisibleForTesting
  public OperatorExecServices context() { return context; }

  @Override
  public boolean buildSchema() {
    assert state == State.START;
    state = State.NEXT_READER;
    nextBatch();
    if (state == State.END) {
      if (readerCount == 0) {
        // return false; // When empty batches are supported
        throw UserException.executionError(
            new ExecutionSetupException("A scan batch must contain at least one reader."))
          .build(logger);
      }
      return false;
    }

    // If the reader returned an empty first batch (schema only),
    // just pass that empty batch along.

    if (containerAccessor.getRowCount() == 0) {
      return true;
    }

    // The reader returned actual data. Just forward the schema
    // in a dummy container, saving the data for next time.

    assert lookahead == null;
    lookahead = new VectorContainer(context.allocator(), containerAccessor.getSchema());
    lookahead.setRecordCount(0);
    lookahead.exchange(containerAccessor.getOutgoingContainer());
    state = State.LOOK_AHEAD;
    return true;
  }

  @Override
  public boolean next() {
    try {
      switch (state) {

      case LOOK_AHEAD:
        // Use batch previously read.
        assert lookahead != null;
        lookahead.exchange(containerAccessor.getOutgoingContainer());
        assert lookahead.getRecordCount() == 0;
        lookahead = null;
        state = State.READER;
        return true;

      case READER:
      case READER_EOF:
        nextBatch();
        return state != State.END;

      case END:
        return false;

      default:
        throw new IllegalStateException("Unexpected state: " + state);
      }
    } catch(Throwable t) {
      state = State.FAILED;
      throw t;
    }
  }

  /**
   * Read another batch from the list of row readers. Keeps opening,
   * reading from, and closing readers as needed to locate a batch, or
   * until all readers are exhausted. Terminates when a batch is read,
   * or all readers are exhausted.
   */

  private void nextBatch() {
    for (;;) {

      // Done with the current reader?

      if (state == State.READER_EOF) {
        closeReader();
        state = State.NEXT_READER;
      }

      // Need to open another reader?

      if (state == State.NEXT_READER) {
        if (! openReader()) {
          state = State.END;
          return;
        }
        state = State.READER;
      }

      // Read a batch.

      if (readBatch()) {
        return;
      }

      // No more batches from the current reader.

      state = State.READER_EOF;

      // The reader is done, but the mutator may have one final
      // row representing an overflow from the previous batch.

      if (scanProjector.tableLoader().rowCount() > 0) {
        prepareBatch();
        return;
      }
    }
  }

  /**
   * Open the next available reader, if any, preparing both the
   * reader and row set mutator.
   * @return true if another reader is active, false if no more
   * readers are available
   */

  private boolean openReader() {

    // Get the next reader, if any.

    if (! readers.hasNext()) {
      containerAccessor.setContainer(null);
      return false;
    }
    reader = readers.next();
    readerCount++;

    // Open the reader. This can fail. if it does, clean up.

    try {
      reader.open(context, new SchemaNegotiatorImpl(this));
      return true;

    // When catching errors, leave the reader member set;
    // we must close it on close() later.

    } catch (UserException e) {
      throw e;
    } catch (Throwable t) {
      throw UserException.executionError(t)
        .addContext("Open failed for reader", reader.getClass().getSimpleName())
        .build(logger);
    }
  }

  /**
   * Read a batch from the current reader.
   * @return true if a batch was read, false if the reader hit EOF
   */

  private boolean readBatch() {

    // Prepare for the batch.

    scanProjector.tableLoader().startBatch();

    // Try to read a batch. This may fail. If so, clean up the
    // mess.

    try {
      if (!reader.next()) {
        return false;
      }
    } catch (UserException e) {
      throw e;
    } catch (Throwable t) {
      throw UserException.executionError(t)
        .addContext("Read failed for reader", reader.getClass().getSimpleName())
        .build(logger);
    }

    // Have a batch. Prepare it for return.

    prepareBatch();
    return true;
  }

  private void prepareBatch() {

    // Add implicit columns, if any.
    // Identify the output container and its schema version.

    containerAccessor.setContainer(scanProjector.harvest());
  }

//  /**
//   * Add implicit columns to the given vector. This is done by creating a
//   * second row set mutator, using that to define and write the implicit
//   * columns, then merging the reader's container with the implicit columns
//   * to produce the final batch.
//   *
//   * @param container container with the data from the reader
//   * @return new container with the original data merged with the implicit
//   * columns
//   */
//
//  private VectorContainer mergeImplicitColumns(VectorContainer container) {
//
//    // Setup. Do nothing if the implicit columns list is empty.
//
//    List<ImplicitColumnDefn> implicitCols = options.implicitColumns;
//    int colCount = implicitCols.size();
//    if (colCount == 0) {
//      return container;
//    }
//
//    // Build the implicit column schema.
//
//    ResultSetLoader overlay = new ResultSetLoaderImpl(context.allocator());
//    overlay.startBatch();
//    TupleLoader writer = overlay.writer();
//    int rowCount = container.getRecordCount();
//    for (ImplicitColumnDefn col : implicitCols) {
//      MajorType type = MajorType.newBuilder()
//          .setMinorType(MinorType.VARCHAR)
//          .setMode(DataMode.REQUIRED)
//          // Note: can't set width because width varies across files
//          //.setPrecision(col.value.length())
//          .build();
//      MaterializedField colSchema = MaterializedField.create(col.colName, type);
//      writer.schema().addColumn(colSchema);
//    }
//
//    // Populate the columns. This is a bit of a waste; would be nice to
//    // use a single value with multiple pointers..
//
//    for (int i = 0; i < rowCount; i++) {
//      overlay.startRow();
//      for (int j = 0; j < colCount; j++) {
//        writer.column(j).setString(implicitCols.get(j).value);
//      }
//      overlay.saveRow();
//    }
//
//    // Merge the two containers.
//
//    VectorContainer implicitContainer = overlay.harvest();
//    return container.merge(implicitContainer);
//  }

  /**
   * Close the current reader.
   */

  private void closeReader() {

    try {
      scanProjector.close();
    } catch (Throwable t) {
      logger.warn("Exception while closing table loader", t);
    }

    // Close the reader. This can fail.

    try {
      reader.close();
    } catch (UserException e) {
      throw e;
    } catch (Throwable t) {
      throw UserException.executionError(t)
        .addContext("Close failed for reader", reader.getClass().getSimpleName())
        .build(logger);
    } finally {
      reader = null;
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

      // Close early.

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

  /**
   * Close reader and release row set mutator resources. May be called
   * twice: once when canceling, once when closing. Designed to be
   * safe on the second call.
   */

  private void closeAll() {

    // May throw an unchecked exception

    try {
      if (reader != null) {
        closeReader();
      }
    } finally {

      if (lookahead != null) {
        lookahead.clear();
        lookahead = null;
      }

      // Will not throw exceptions

      if (scanProjector != null) {
        scanProjector.close();
        scanProjector = null;
      }
    }
  }

  public ResultSetLoader getMutator() {
    if (scanProjector == null) {
      return null;
    }
    return scanProjector.tableLoader();
  }
}
