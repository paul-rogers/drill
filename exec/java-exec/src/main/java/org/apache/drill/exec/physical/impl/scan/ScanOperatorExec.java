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
import org.apache.drill.exec.physical.impl.scan.ScanProjection.TableSchemaType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
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

    private final ReaderState readerState;
    private final List<SelectColumn> selection = new ArrayList<>();
    private final List<TableColumn> tableSchema = new ArrayList<>();

    private SchemaNegotiatorImpl(ReaderState readerState) {
      this.readerState = readerState;
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
      ScanOperatorExec scanOp = readerState.scanOp();
      ProjectionPlanner planner = new ProjectionPlanner(scanOp.context.getFragmentContext().getOptionSet());
      if (scanOp.options.selection != null) {
        if (! selection.isEmpty()) {
          throw new IllegalStateException("Select list provided by scan operator; cannot also be provided by reader");
        }
        planner.queryCols(scanOp.options.selection);
      } else {
        planner.selection(selection);
      }
      planner.tableSchema(tableSchema);
      readerState.projection = planner.build();
      readerState.scanProjector = new ScanProjector.Builder(scanOp.context.allocator(), readerState.projection)
          .resultSetOptions(new ResultSetLoaderImpl.OptionBuilder().setInventory(scanOp.inventory))
          .build();
      return readerState.scanProjector.tableLoader();
    }
  }

  // TODO: Move into the projection planner

  public static class ScanOptions {
//    public List<ImplicitColumnDefn> implicitColumns;
//    public boolean reuseSchemaAcrossReaders;
    public List<SchemaPath> selection;
  }

  /**
   * Manages a row batch reader through its lifecycle. Created when the reader
   * is opened, discarded when the reader is closed. Encapsulates state that
   * follows the life of the reader.
   */

  public static class ReaderState {
    private enum State { START, LOOK_AHEAD, ACTIVE, EOF, CLOSED };

    private final ScanOperatorExec scanOp;
    private final RowBatchReader reader;
    private State state = State.START;
    protected ScanProjection projection;
    protected ScanProjector scanProjector;
    private boolean earlySchema;
    private VectorContainer lookahead;

    public ReaderState(ScanOperatorExec scanOp, RowBatchReader reader) {
      this.scanOp = scanOp;
      this.reader = reader;
    }

    protected ScanOperatorExec scanOp() { return scanOp; }

    /**
     * Open the next available reader, if any, preparing both the
     * reader and row set mutator.
     * @return true if another reader is active, false if no more
     * readers are available
     */

    private boolean open() {

      // Open the reader. This can fail. if it does, clean up.

      try {
        reader.open(scanOp.context, new SchemaNegotiatorImpl(this));

      // When catching errors, leave the reader member set;
      // we must close it on close() later.

      } catch (UserException e) {
        throw e;
      } catch (Throwable t) {
        throw UserException.executionError(t)
          .addContext("Open failed for reader", reader.getClass().getSimpleName())
          .build(logger);
      }

      earlySchema = projection.tableSchemaType() == TableSchemaType.EARLY;
      if (earlySchema) {
        // TODO: Setup the empty batch
      }
      return true;
    }

    /**
     * Prepare the schema for this reader. Called for the first reader within a
     * scan batch, if the reader returns <tt>true</tt> from <tt>open()</tt>. If
     * this is an early-schema reader, then the result set loader already has
     * the proper value vectors set up. If this is a late-schema reader, we must
     * read one batch to get the schema, then set aside the data for the next
     * call to <tt>next()</tt>.
     * <p>
     * Semantics for all readers:
     * <ul>
     * <li>If the file was not found, <tt>open()</tt> returned false and this
     * method should never be called.</li>
     * </ul>
     * <p>
     * Semantics for early-schema readers:
     * <ul>
     * <li>If if turned out that the file was
     * empty when trying to read the schema, <tt>open()</tt> returned false
     * and this method should never be called.</tt>
     * <li>Otherwise, if a schema was available, then the schema is already
     * set up in the result set loader as the result of schema negotiation, and
     * this method simply returns <tt>true</tt>.
     * </ul>
     * <p>
     * Semantics for late-schema readers:
     * <ul>
     * <li>This method will ask the reader to
     * read a batch. If the reader hits EOF before finding any data, this method
     * will return false, indicating that no schema is available.</li>
     * <li>If the reader can read enough of the file to
     * figure out the schema, but the file has no data, then this method will
     * return <tt>true</tt> and a schema will be available. The first call to
     * <tt>next()</tt> will report EOF.</li>
     * <li>Otherwise, this method returns true, sets up an empty batch with the
     * schema, saves the data batch, and will return that look-ahead batch on the
     * first call to <tt>next()</tt>.</li>
     * </ul>
     * @return true if the schema was read, false if EOF was reached while trying
     * to read the schema.
     */
    protected boolean buildSchema() {

      if (earlySchema) {
        return true;
      }

      // Late schema. Read a batch.

      if (! next()) {
        return false;
      }
      if (scanOp.containerAccessor.getRowCount() == 0) {
        return true;
      }

      // The reader returned actual data. Just forward the schema
      // in a dummy container, saving the data for next time.

      assert lookahead == null;
      lookahead = new VectorContainer(scanOp.context.allocator(), scanOp.containerAccessor.getSchema());
      lookahead.setRecordCount(0);
      lookahead.exchange(scanOp.containerAccessor.getOutgoingContainer());
      state = State.LOOK_AHEAD;
      return true;
    }

    protected boolean next() {
      switch (state) {
      case LOOK_AHEAD:
        // Use batch previously read.
        assert lookahead != null;
        lookahead.exchange(scanOp.containerAccessor.getOutgoingContainer());
        assert lookahead.getRecordCount() == 0;
        lookahead = null;
        state = State.ACTIVE;
        return true;

      case ACTIVE:
        if (readBatch()) {
          return true;
        }
        state = State.EOF;

        // The reader is done, but the mutator may have one final
        // row representing an overflow from the previous batch.

        if (scanProjector.tableLoader().rowCount() > 0) {
          prepareBatch();
          return true;
        }
        return false;

      case EOF:
        return false;

      default:
        throw new IllegalStateException("Unexpected state: " + state);
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

      // TODO: Need to retain value vectors across files if no schema change

      return true;
    }

    private void prepareBatch() {

      // Add implicit columns, if any.
      // Identify the output container and its schema version.

      scanOp.containerAccessor.setContainer(scanProjector.harvest());
    }

    public boolean isEof() {
      return state == State.EOF;
    }

    /**
     * Close the current reader.
     */

    private void close() {
      if (state == State.CLOSED) {
        return;
      }

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

        // Will not throw exceptions

        if (lookahead != null) {
          lookahead.clear();
          lookahead = null;
        }
        state = State.CLOSED;
      }
    }

    public ResultSetLoader tableLoader() {
      return scanProjector.tableLoader();
    }
  }

  private enum State { START, READER, END, FAILED, CLOSED }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanOperatorExec.class);

  private State state = State.START;
  private OperatorExecServices context;
  private final Iterator<RowBatchReader> readers;
  private final ScanOptions options;
  private final VectorContainerAccessor containerAccessor = new VectorContainerAccessor();
  private ResultVectorCache inventory;
  private int readerCount;
  private ReaderState readerState;

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
    inventory = new ResultVectorCache(context.allocator());
  }

  @Override
  public BatchAccessor batchAccessor() { return containerAccessor; }

  @VisibleForTesting
  public OperatorExecServices context() { return context; }

  @Override
  public boolean buildSchema() {
    assert state == State.START;
    nextSchema();
    if (state != State.END) {
      return true;
    }
    if (readerCount == 0) {
      // return false; // When empty batches are supported
      throw UserException.executionError(
          new ExecutionSetupException("A scan batch must contain at least one reader."))
        .build(logger);
    }
    return false;
  }

  /**
   * Spin though readers looking for the first that has enough data
   * to provide a schema. Skips empty, missing or otherwise "null"
   * readers.
   */

  private void nextSchema() {
    for (;;) {

      // If have a reader, load the schema

      if (readerState != null) {
        if (readerState.buildSchema()) {
          return;
        }

        // Done with the current reader

        closeReader();
      }

      // Need to open another reader?

      if (! openReader()) {
        state = State.END;
        return;
      }
      state = State.READER;
    }
  }

  @Override
  public boolean next() {
    try {
      switch (state) {

      case READER:
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

      // If have a reader, read a batch

      if (readerState != null) {
        if (readerState.next()) {
          return;
        }

        // Done with the current reader

        closeReader();
      }

      // Need to open another reader?

      if (! openReader()) {
        state = State.END;
        return;
      }
      state = State.READER;
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
    RowBatchReader reader = readers.next();
    readerCount++;

    // Open the reader. This can fail.

    readerState = new ReaderState(this, reader);
    readerState.open();
    return true;
  }

//  private boolean readBatch() {
//
//    if (! readerState.next()) {
//      return false;
//    }
//
//    // Have a batch. Prepare it for return.
//
//    prepareBatch();
//    return true;
//  }
//
//  private void prepareBatch() {
//
//    // Add implicit columns, if any.
//    // Identify the output container and its schema version.
//
//    containerAccessor.setContainer(scanProjector.harvest());
//  }

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
      readerState.close();
    } finally {
      readerState = null;
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
      if (readerState != null) {
        closeReader();
      }
    } finally {

    }
  }

  public ResultSetLoader getMutator() {
    if (readerState == null) {
      return null;
    }
    return readerState.tableLoader();
  }
}
