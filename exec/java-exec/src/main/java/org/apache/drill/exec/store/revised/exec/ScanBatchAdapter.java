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
package org.apache.drill.exec.store.revised.exec;

import java.util.Iterator;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.revised.Sketch;
import org.apache.drill.exec.store.revised.Sketch.Deserializer;
import org.apache.drill.exec.store.revised.Sketch.ResultSetMaker;
import org.apache.drill.exec.store.revised.Sketch.RowBatch;
import org.apache.drill.exec.store.revised.Sketch.ScanOperation;

// TODO: Handle stats
// TODO: close/abandon readers
// TODO: Implicit columns

public class ScanBatchAdapter implements CloseableRecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanBatchAdapter.class);

  public static interface DeserializerFactory<T extends Deserializer<?>> {
    T newDeserializer(ScanOperation scanOp);
  }

  public abstract static class AbstractSingleDeserializerFactory<C extends AbstractSubScan, T extends Deserializer<C>> implements DeserializerFactory<T> {

    private C opConfig;
    private boolean first = true;

    public AbstractSingleDeserializerFactory(C opConfig) {
      this.opConfig = opConfig;
    }

    @Override
    public T newDeserializer(ScanOperation scanOp) {
      if (first) {
        first = false;
        return newDeserializer(opConfig, scanOp);
      }
      return null;
    }

    protected abstract T newDeserializer(C opConfig, ScanOperation scanOp);
  }

  public abstract static class AbstractListDeserializerFactory<C extends AbstractSubScan, E, T extends Deserializer<C>> implements DeserializerFactory<T> {

    private C opConfig;
    private Iterator<E> iter;

    public AbstractListDeserializerFactory(C opConfig, Iterator<E> iter) {
      this.opConfig = opConfig;
      this.iter = iter;
    }

    @Override
    public T newDeserializer(ScanOperation scanOp) {
      if (! iter.hasNext()) {
        return null;
      }
      return newDeserializer(opConfig, iter.next(), scanOp);
    }

    protected abstract T newDeserializer(C opConfig, E entry, ScanOperation scanOp);
  }

  public static class ScanOperationImpl implements ScanOperation {

    private ScanBatchAdapter scanBatch;

    public ScanOperationImpl(ScanBatchAdapter scanBatch) {
      this.scanBatch = scanBatch;
    }

    @Override
    public ResultSetMaker resultSet() {
      return scanBatch.resultSet;
    }

    @Override
    public DrillConfig config() {
      return fragmentContext().getConfig();
    }

    @Override
    public OptionManager sessionOptions() {
      return fragmentContext().getOptions();
    }

    @Override
    public OptionManager systemOptions() {
      return globalContext().getOptionManager();
    }

    @Override
    public FragmentContext fragmentContext() {
      return scanBatch.fragContext;
    }

    @Override
    public DrillbitContext globalContext() {
      return fragmentContext().getDrillbitContext();
    }
  }

  enum ScanState { START, NEW_SCAN, SCAN, EOF, END };

  DeserializerFactory<?> factory;
  ResultSetMaker resultSet = new ResultSetMakerImpl();
  ScanOperationImpl scanOp;
  Deserializer<?> deserializer;
  RowBatch batch;
  FragmentContext fragContext;
  ScanState state = ScanState.START;

  public ScanBatchAdapter(DeserializerFactory<?> factory, FragmentContext fragContext) throws ExecutionSetupException {
    this.factory = factory;
    this.fragContext = fragContext;
    this.scanOp = new ScanOperationImpl(this);
  }

  @Override
  public FragmentContext getContext() {
    return fragContext;
  }

  @Override
  public BatchSchema getSchema() {
    assert batch != null;
    return batch.batchSchema();
  }

  @Override
  public int getRecordCount() {
    assert batch != null;
    return batch.rowCount();
  }

  @Override
  public void kill(boolean sendUpstream) {
    if (state != ScanState.END) {
      resultSet.abandon();
      state = ScanState.END;
    }
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    assert state == ScanState.SCAN || state == ScanState.NEW_SCAN || state == ScanState.EOF;
    assert batch != null;
    return batch.vectorContainer();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    assert state == ScanState.SCAN || state == ScanState.NEW_SCAN || state == ScanState.EOF;
    assert batch != null;
    int colId = batch.findPath(path);
    assert colId >= 0;
    return batch.vectorId(colId);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IterOutcome next() {
    try {
      return innerNext();
    } catch (UserException e) {
      // If the implementation was playing well with others it will have
      // created a proper user exception.
      throw e;

    } catch (Exception e) {

      // Otherwise, something went wrong and we don't know what.

      String className;
      if (deserializer != null) {
        className = deserializer.getClass().getName();
      } else {
        className = factory.getClass().getName();
      }
      throw UserException.unsupportedError(e)
          .addContext("Unexpected error in data source extension: " + className)
          .build(logger);
    }
  }

  private IterOutcome innerNext() throws Exception {
    for (;;) {
      switch (state) {
      case END:
        assert false;
      case EOF:
        return IterOutcome.NONE;
      case START:
      case NEW_SCAN:
        if (startScan()) {
          state = ScanState.SCAN;
          return IterOutcome.OK_NEW_SCHEMA;
        } else {
          state = ScanState.EOF;
          return IterOutcome.NONE;
        }
      case SCAN:
        if (scanBatch())
          return IterOutcome.OK;
        state = ScanState.NEW_SCAN;
        break;
      default:
        throw new IllegalStateException("State: " + state);
      }
    }
  }

  private boolean startScan() throws Exception {
    deserializer = factory.newDeserializer(scanOp);
    if (deserializer == null) {
      state = ScanState.EOF;
      return false;
    }
    return scanBatch();
  }

  private boolean scanBatch() throws Exception {
    batch = deserializer.readBatch();
    if (batch == null) {
      deserializer.close();
      deserializer = null;
    }
    return batch != null;
  }

  @Override
  public WritableBatch getWritableBatch() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return batch.sv2();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return batch.sv4();
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return batch.vectorIterator();
  }

  @Override
  public void close() throws Exception {
    resultSet.close();
  }
}