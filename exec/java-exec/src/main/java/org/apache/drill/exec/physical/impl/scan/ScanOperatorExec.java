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
import org.apache.drill.exec.physical.impl.scan.ScanProjection.SelectColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.TableColumn;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.TableSchemaType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.hadoop.fs.Path;

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


  /**
   * Implementation of the schema negotiation between scan operator and
   * batch reader. Anticipates that the select list (and/or the list of
   * predefined fields (implicit, partition) might be set by the scanner.
   * For now, all readers have their own implementation of the select
   * set.
   * <p>
   * Handles both early- and late-schema readers. Early-schema readers
   * provide a table schema, late-schema readers do not.
   * <p>
   * If the reader (or, later, the scanner) has a SELECT list, then that
   * select list is pushed down into the result set loader created for
   * the reader.
   * <p>
   * Also handles parsing out various column types, filling in null
   * columns and (via the vector cache), minimizing changes across
   * readers. In the worst case, a reader might have a column "c" in
   * one file, might skip "c" in the second file, and "c" may appear again
   * in a third file. This negotiator, along with the scan projection
   * and vector cache, "smooths out" schema changes by preserving the vector
   * for "c" across all three files. In the first and third files "c" is
   * a vector written by the reader, in the second, it is a null column
   * filled in by the scan projector (assuming, of course, that "c"
   * is nullable or an array.)
   */

  private static class SchemaNegotiatorImpl implements SchemaNegotiator {

    private final ReaderState readerState;
    private final List<SelectColumn> selection = new ArrayList<>();
    private final List<TableColumn> tableSchema = new ArrayList<>();
    private TableSchemaType tableSchemaType = TableSchemaType.LATE;
    private Path filePath;
    private Path rootPath;
    private MajorType nullType;

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
      // Always treated as ANY: the projection planner will sort out the meaning.

      selection.add(new SelectColumn(path));
    }

    @Override
    public void setTableSchemaType(TableSchemaType type) {
      if (type == TableSchemaType.LATE && ! tableSchema.isEmpty()) {
        throw new IllegalArgumentException("Can't set schema to LATE after providing table columns.");
      }
      tableSchemaType = type;
    }

    @Override
    public void addTableColumn(String name, MajorType type) {
      addTableColumn(MaterializedField.create(name, type));
    }

    @Override
    public void addTableColumn(MaterializedField schema) {
      tableSchemaType = TableSchemaType.EARLY;
      tableSchema.add(new TableColumn(tableSchema.size(), schema));
    }

    @Override
    public void setFilePath(Path filePath) {
      this.filePath = filePath;
    }

    @Override
    public void setSelectionRoot(Path rootPath) {
      this.rootPath = rootPath;
    }

    @Override
    public void setNullType(MajorType type) {
      nullType = type;
    }

    @Override
    public ResultSetLoader build() {
      return readerState.buildSchema(this);
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
    protected ResultSetLoader tableLoader;
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

        // The reader can return a "soft" failure: the open worked, but
        // the file is empty, non-existent or some other form of "no data."
        // Handle this by immediately moving to EOF. The scanner will quietly
        // pass over this reader and move onto the next, if any.

        if (! reader.open(scanOp.context, new SchemaNegotiatorImpl(this))) {

          // If we had a soft failure, then there should be no schema.
          // The reader should not have negotiated one. Not a huge
          // problem, but something is out of whack.

          assert tableLoader == null;
          if (tableLoader != null) {
            logger.warn("Reader " + reader.getClass().getSimpleName() +
                " returned false from open, but negotiated a schema.");
          }
          state = State.EOF;
          return false;
        }

      // When catching errors, leave the reader member set;
      // we must close it on close() later.

      } catch (UserException e) {
        throw e;
      } catch (Throwable t) {
        throw UserException.executionError(t)
          .addContext("Open failed for reader", reader.getClass().getSimpleName())
          .build(logger);
      }

      state = State.ACTIVE;

      // Storage plugins are extensible: a novice developer may not
      // have known to create the table loader. Fail in this case.

      if (tableLoader == null) {
        throw UserException.executionError(
            new IllegalStateException("Reader " + reader.getClass().getSimpleName() +
                " failed to create a table loader"))
          .build(logger);
      }
      return true;
    }

    /**
     * Callback from the schema negotiator to build the schema from information from
     * both the table and scan operator. Returns the result set loader to be used
     * by the reader to write to the table's value vectors.
     *
     * @param schemaNegotiator builder given to the reader to provide it's
     * schema information
     * @return the result set loader to be used by the reader
     */

    protected ResultSetLoader buildSchema(SchemaNegotiatorImpl schemaNegotiator) {

      // Plan the projection: (table columns), (static columns) --> (output schema)
      // Where (output schema) is the (select list) expanded as needed and
      // decorated with type information.

      projection = buildProjectionPlan(schemaNegotiator);
      earlySchema = projection.tableSchemaType() == TableSchemaType.EARLY;

      // Build and return the result set loader to be used by the reader.

      return scanOp.buildTableLoader(projection, schemaNegotiator.nullType);
    }

    private ScanProjection buildProjectionPlan(
        SchemaNegotiatorImpl schemaNegotiator) {

      ProjectionPlanner planner = new ProjectionPlanner(scanOp.context.getFragmentContext().getOptionSet());

      // Either the scan operator, or reader, but not both,
      // can provide the selection list.
      // The select list should be provided for the group of readers,
      // but there is no good way to do that at present.

      if (scanOp.options.selection != null) {
        if (! schemaNegotiator.selection.isEmpty()) {
          throw new IllegalStateException(
              "Select list provided by scan operator; cannot also be provided by reader");
        }
        planner.queryCols(scanOp.options.selection);
      } else {
        planner.selection(schemaNegotiator.selection);
      }

      // Provide the table schema (if any) provided by the reader.
      // For the projection planner, a null list means late schema.

      if (schemaNegotiator.tableSchemaType == TableSchemaType.EARLY) {
        planner.tableSchema(schemaNegotiator.tableSchema);
      }

      // Populate the file and selection root, if any, for implicit
      // columns and partition columns.
      // TODO: Clean up API to pass either a string or path both places..

      if (schemaNegotiator.filePath != null) {
        planner.setSource(schemaNegotiator.filePath,
            schemaNegotiator.rootPath == null ? null :
              Path.getPathWithoutSchemeAndAuthority(schemaNegotiator.rootPath).toString());
      }

      // Create the projection plan. This is the full projection
      // plan for early-schema tables, a partial plan for late-schema,
      // since late-schema does not know the schema until the first
      // batch is read.

      return planner.build();
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

        return tableLoader.rowCount() > 0;

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

      tableLoader.startBatch();

      // Try to read a batch. This may fail. If so, clean up the
      // mess.

      try {
        return reader.next();
      } catch (UserException e) {
        throw e;
      } catch (Throwable t) {
        throw UserException.executionError(t)
          .addContext("Read failed for reader", reader.getClass().getSimpleName())
          .build(logger);
      }
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
        if (tableLoader != null) {
          tableLoader.close();
          tableLoader = null;
        }
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
      return tableLoader;
    }
  }

  private enum State { START, READER, END, FAILED, CLOSED }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanOperatorExec.class);

  private final Iterator<RowBatchReader> readers;
  private final ScanOptions options;
  private final VectorContainerAccessor containerAccessor = new VectorContainerAccessor();
  private State state = State.START;
  private OperatorExecServices context;
  private ScanProjector scanProjector;
  private int readerCount;
  private ReaderState readerState;

  public ScanOperatorExec(Iterator<RowBatchReader> readers,
                          ScanOptions options) {
    this.readers = readers;
    this.options = options == null ? new ScanOptions() : options;
  }

  public ScanOperatorExec(Iterator<RowBatchReader> iterator) {
    this(iterator, new ScanOptions());
  }

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
    nextSchema();
    if (state != State.END) {
      return true;
    }

    // Reader count check done here because readers are passed as
    // an iterator, not list. We don't know the count until we've
    // seen EOF from the iterator.

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
    nextAction(true);
  }

  private ResultSetLoader buildTableLoader(ScanProjection projection, MajorType nullType) {

    // If this is the first reader, create the scan projector
    // using the null type provided by the first reader. The
    // null type should be per group of readers, but there is
    // no good way to do that at present.

    if (scanProjector == null) {
      scanProjector = new ScanProjector(context.allocator(), nullType);
    }

    // Create the table loader

    ResultSetLoader tableLoader = scanProjector.makeTableLoader(projection);

    // Bind the output container to the output of the scan operator.

    containerAccessor.setContainer(scanProjector.output());

    return tableLoader;
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
    nextAction(false);
  }

  private void nextAction(boolean schema) {
    for (;;) {

      // If have a reader, read a batch

      if (readerState != null) {
        boolean ok;
        if (schema) {
          ok = readerState.buildSchema();
        } else {
          ok = readBatch();
        }
        if (ok) {
          break;
        }
        closeReader();
      }

      // Another reader available?

      if (! nextReader()) {
        state = State.END;
        return;
      }
      state = State.READER;

      // Is the reader usable?

      if (! readerState.open()) {
        closeReader();
      }
    }
  }

  private boolean readBatch() {
    if (! readerState.next()) {
      return false;
    }

    // Have a batch. Prepare it for return.

    // Add implicit columns, if any.
    // Identify the output container and its schema version.

    scanProjector.publish();

    // Late schema readers may change their schema between batches

    if (! readerState.earlySchema) {
      containerAccessor.setContainer(scanProjector.output());
    }
    return true;
  }

  /**
   * Open the next available reader, if any, preparing both the
   * reader and row set mutator.
   * @return true if another reader is active, false if no more
   * readers are available
   */

  private boolean nextReader() {

    // Get the next reader, if any.

    if (! readers.hasNext()) {
      containerAccessor.setContainer(null);
      return false;
    }
    RowBatchReader reader = readers.next();
    readerCount++;

    // Open the reader. This can fail.

    readerState = new ReaderState(this, reader);
    return true;
  }

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
