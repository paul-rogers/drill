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
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.protocol.BatchAccessor;
import org.apache.drill.exec.physical.impl.protocol.OperatorExec;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch;
import org.apache.drill.exec.physical.impl.protocol.VectorContainerAccessor;
import org.apache.drill.exec.physical.impl.scan.project.FileMetadataColumnsParser;
import org.apache.drill.exec.physical.impl.scan.project.FileMetadataColumnsParser.FileMetadataProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjectionBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
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
   * and vector cache, "smoothes out" schema changes by preserving the vector
   * for "c" across all three files. In the first and third files "c" is
   * a vector written by the reader, in the second, it is a null column
   * filled in by the scan projector (assuming, of course, that "c"
   * is nullable or an array.)
   */

  private static class SchemaNegotiatorImpl implements SchemaNegotiator {

    private final ReaderState readerState;
    private TupleMetadata tableSchema;
    private Path filePath;
    private int batchSize = ValueVector.MAX_ROW_COUNT;

    private SchemaNegotiatorImpl(ReaderState readerState) {
      this.readerState = readerState;
    }

    @Override
    public String getUserName() {
      return readerState.scanOp.userName();
    }

    @Override
    public void setFilePath(Path filePath) {
      this.filePath = filePath;
    }

    @Override
    public ResultSetLoader build() {
      return readerState.buildSchema(this);
    }

    @Override
    public OperatorContext context() {
      return readerState.scanOp.context;
    }

    @Override
    public void setTableSchema(TupleMetadata schema) {
      tableSchema = schema;
    }

    @Override
    public void setBatchSize(int maxRecordsPerBatch) {
      batchSize = maxRecordsPerBatch;
    }

//    @Override
//    public boolean[] columnsArrayProjectionMap() {
//      return readerState.scanOp.scanProjector.columnsArrayProjectionMap();
//    }
  }

  /**
   * Manages a row batch reader through its lifecycle. Created when the reader
   * is opened, discarded when the reader is closed. Encapsulates state that
   * follows the life of the reader.
   */

  private static class ReaderState {
    private enum State { START, LOOK_AHEAD, ACTIVE, EOF, CLOSED };

    private final ScanOperatorExec scanOp;
    private final RowBatchReader reader;
    private State state = State.START;
    protected ResultSetLoader tableLoader;
    private boolean earlySchema;
    private VectorContainer lookahead;

    public ReaderState(ScanOperatorExec scanOp, RowBatchReader reader) {
      this.scanOp = scanOp;
      this.reader = reader;
    }

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

        if (! reader.open(new SchemaNegotiatorImpl(this))) {

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
        throw UserException.executionError( // TODO: Test this path
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

      earlySchema = schemaNegotiator.tableSchema != null;

      // Build and return the result set loader to be used by the reader.

      ScanProjector projector = scanOp.scanProjector;
      projector.startFile(schemaNegotiator.filePath);
      tableLoader = projector.makeTableLoader(schemaNegotiator.tableSchema,
                                              schemaNegotiator.batchSize);
      return tableLoader;
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

        // Bind the output container to the output of the scan operator.
        // This returns an empty batch with the schema filled in.

        scanOp.containerAccessor.setContainer(scanOp.scanProjector.output());
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
      lookahead = new VectorContainer(scanOp.context.getAllocator(), scanOp.containerAccessor.getSchema());
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
        return readBatch();

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
        if (! reader.next()) {
          state = State.EOF;

          // If the reader has no more rows, the table loader may still have
          // a lookahead row.

          if ( tableLoader.writer().rowCount() == 0) {
            return false;
          }
        }
      } catch (UserException e) {
        throw e;
      } catch (Throwable t) {
        throw UserException.executionError(t)
          .addContext("Read failed for reader", reader.getClass().getSimpleName())
          .build(logger);
      }

      // Have a batch. Prepare it for return.

      // Add implicit columns, if any.
      // Identify the output container and its schema version.

      scanOp.scanProjector.publish();

      // Late schema readers may change their schema between batches.
      // Early schema changes only on the first batch of the next
      // reader.

      if (! earlySchema || tableLoader.batchCount() == 1) {
        scanOp.containerAccessor.setContainer(scanOp.scanProjector.output());
      }
      return true;
    }

    /**
     * Close the current reader.
     */

    private void close() {
      if (state == State.CLOSED) {
        return; // TODO: Test this path
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

        try {
          if (tableLoader != null) {
            tableLoader.close();
            tableLoader = null;
          }
        } catch (Throwable t) {
          logger.warn("Exception while closing table loader", t);
        }

        // Will not throw exceptions

        if (lookahead != null) {
          lookahead.clear(); // TODO: Test this path
          lookahead = null;
        }
        state = State.CLOSED;
      }
    }
  }

  public static class ScanOperatorExecBuilder {
    protected String scanRootDir;
    protected List<SchemaPath> projection = new ArrayList<>();
    protected MajorType nullType;
    protected Iterator<RowBatchReader> readerIter;
    protected List<RowBatchReader> readers;
    protected boolean useLegacyWildcardExpansion = true;
    protected String userName;

    /**
     * Specify the selection root for a directory scan, if any.
     * Used to populate partition columns.
     * @param rootPath Hadoop file path for the directory
     */

    public ScanOperatorExecBuilder setSelectionRoot(Path rootPath) {
      this.scanRootDir = rootPath.toString();
      return this;
    }

    public ScanOperatorExecBuilder setSelectionRoot(String rootPath) {
      this.scanRootDir = rootPath;
      return this;
    }

    /**
     * Specify the type to use for projected columns that do not
     * match any data source columns. Defaults to nullable int.
     */

    public ScanOperatorExecBuilder setNullType(MajorType type) {
      this.nullType = type;
      return this;
    }

    public ScanOperatorExecBuilder useLegacyWildcardExpansion(boolean flag) {
      useLegacyWildcardExpansion = flag;
      return this;
    }

    public ScanOperatorExecBuilder setReaderIterator(Iterator<RowBatchReader> readerIter) {
      this.readerIter = readerIter;
      return this;
    }

    public ScanOperatorExecBuilder addReader(RowBatchReader reader) {
      assert readerIter == null;
      if (readers == null) {
        readers = new ArrayList<>();
      }
      readers.add(reader);
      return this;
    }

    public ScanOperatorExecBuilder addReaders(List<RowBatchReader> readerList) {
      assert readerIter == null;
      if (readers == null) {
        readers = new ArrayList<>();
      }
      readers.addAll(readerList);
      return this;
    }

    @VisibleForTesting
    public ScanOperatorExecBuilder projectAll() {
      return addProjection(ScanLevelProjection.WILDCARD);
    }

    public ScanOperatorExecBuilder addProjection(String colName) {
      return addProjection(SchemaPath.getSimplePath(colName));
    }

    public ScanOperatorExecBuilder addProjection(SchemaPath path) {
      projection.add(path);
      return this;
    }

    public ScanOperatorExecBuilder addProjection(List<SchemaPath> projection) {
      projection.addAll(projection);
      return this;
    }

    @VisibleForTesting
    public ScanOperatorExecBuilder setProjection(String[] projection) {
      for (String col : projection) {
        addProjection(col);
      }
      return this;
    }

    public ScanOperatorExec build() {
      if (readers == null && readerIter == null) {
        throw new IllegalArgumentException("No readers provided");
      }
      if (projection.isEmpty()) {
        logger.warn("No projection list specified: assuming SELECT *");
        projectAll();
      }
      return new ScanOperatorExec(this);
    }

    public OperatorRecordBatch buildRecordBatch(FragmentContext context, PhysicalOperator config) {
      return new OperatorRecordBatch(context, config, build());
    }

    protected Iterator<RowBatchReader> readers() {
      return (readerIter != null)? readerIter : readers.iterator();
    }

    public ScanOperatorExecBuilder setUserName(String userName) {
      this.userName = userName;
      return this;
    }
  }

  private enum State { START, READER, END, FAILED, CLOSED }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanOperatorExec.class);

  private final ScanOperatorExecBuilder builder;
  private final Iterator<RowBatchReader> readers;
  private final VectorContainerAccessor containerAccessor = new VectorContainerAccessor();
  private State state = State.START;
  private OperatorContext context;
  private ScanProjector scanProjector;
  private int readerCount;
  private ReaderState readerState;

  public ScanOperatorExec(ScanOperatorExecBuilder builder) {
    this.builder = builder;
    this.readers = builder.readers();
  }

  public static ScanOperatorExecBuilder builder() {
    return new ScanOperatorExecBuilder();
  }

  @Override
  public void bind(OperatorContext context) {
    this.context = context;
    ScanProjectionBuilder scanProjBuilder = new ScanProjectionBuilder();
    FileMetadataColumnsParser parser = new FileMetadataColumnsParser(context.getFragmentContext().getOptionSet());
    parser.useLegacyWildcardExpansion(builder.useLegacyWildcardExpansion);
    parser.setScanRootDir(builder.scanRootDir);
    scanProjBuilder.addParser(parser);
    scanProjBuilder.projectedCols(builder.projection);
    ScanLevelProjection scanProj = scanProjBuilder.build();
    FileMetadataProjection metadataPlan = parser.getProjection();
    scanProjector = new ScanProjector(context.getAllocator(), scanProj, metadataPlan, builder.nullType);
  }

  @Override
  public BatchAccessor batchAccessor() { return containerAccessor; }

  @VisibleForTesting
  public OperatorContext context() { return context; }

  public String userName() { return builder.userName; }

  @Override
  public boolean buildSchema() {
    assert state == State.START;

    // Spin though readers looking for the first that has enough data
    // to provide a schema. Skips empty, missing or otherwise "null"
    // readers.

    nextAction(true);
    if (state != State.END) {
      return true;
    }

    // Reader count check done here because readers are passed as
    // an iterator, not list. We don't know the count until we've
    // seen EOF from the iterator.

    if (readerCount == 0) {
      // return false; // When empty batches are supported
      throw UserException.executionError( // TODO: Test this path
          new ExecutionSetupException("A scan batch must contain at least one reader."))
        .build(logger);
    }
    return false;
  }

  @Override
  public boolean next() {
    try {
      switch (state) {

      case READER:
        // Read another batch from the list of row readers. Keeps opening,
        // reading from, and closing readers as needed to locate a batch, or
        // until all readers are exhausted. Terminates when a batch is read,
        // or all readers are exhausted.

        nextAction(false);
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

  private void nextAction(boolean schema) {
    for (;;) {

      // If have a reader, read a batch

      if (readerState != null) {
        boolean ok;
        if (schema) {
          ok = readerState.buildSchema();
        } else {
          ok = readerState.next();
        }
        if (ok) {
          break;
        }
        closeReader();
      }

      // Another reader available?

      if (! nextReader()) {
        closeProjector();
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
      closeProjector();
    }
  }

  private void closeProjector() {
    if (scanProjector != null) {
      scanProjector.close();
      scanProjector = null;
    }
  }
}
