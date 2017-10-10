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
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.protocol.BatchAccessor;
import org.apache.drill.exec.physical.impl.protocol.OperatorExec;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch;
import org.apache.drill.exec.physical.impl.protocol.VectorContainerAccessor;

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

  public static class ScanOperatorExecBuilder {
    protected Iterator<RowBatchReader> readerIter;
    protected List<RowBatchReader> readers;
    protected String userName;
    protected ScanSchema manager;

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

     public ScanOperatorExec build() {
      if (readers == null && readerIter == null) {
        throw new IllegalArgumentException("No readers provided");
      }
      if (manager == null) {
        throw new IllegalStateException("Schema manager is missing");
      }
      return new ScanOperatorExec(this, manager);
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

    public ScanOperatorExecBuilder setSchemaManager(
        ScanSchema schemaManager) {
      this.manager = schemaManager;
      return this;
    }
  }

  private enum State { START, READER, END, FAILED, CLOSED }

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanOperatorExec.class);

  private final ScanOperatorExecBuilder builder;
  protected final ScanSchema manager;
  private final Iterator<RowBatchReader> readers;
  protected final VectorContainerAccessor containerAccessor = new VectorContainerAccessor();
  private State state = State.START;
  protected OperatorContext context;
  private int readerCount;
  private ReaderState readerState;

  public ScanOperatorExec(ScanOperatorExecBuilder builder, ScanSchema manager) {
    this.builder = builder;
    this.readers = builder.readers();
    this.manager = manager;
  }

  public static ScanOperatorExecBuilder builder() {
    return new ScanOperatorExecBuilder();
  }

  @Override
  public void bind(OperatorContext context) {
    this.context = context;
    manager.build(context);
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
      manager.close();
      state = State.CLOSED;
    }
  }
}
