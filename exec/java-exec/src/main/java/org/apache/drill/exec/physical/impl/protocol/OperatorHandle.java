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
package org.apache.drill.exec.physical.impl.protocol;

import java.util.Iterator;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.BatchAccessor;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.OperatorExec;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

public interface OperatorHandle {

  BatchAccessor batchAccessor();
  boolean buildSchema();
  boolean next();
  void cancel(boolean sendUpstream);
  void close();

  public static class RecordBatchAccessor implements BatchAccessor {

    private RecordBatch batch;
    private int schemaVersion;

    public RecordBatchAccessor(RecordBatch batch) {
      this.batch = batch;
    }

    @Override
    public BatchSchema getSchema() { return batch.getSchema(); }

    @Override
    public int schemaVersion() { return schemaVersion; }

    public void bumpVersion( ) { schemaVersion++; }

    @Override
    public int getRowCount() { return batch.getRecordCount(); }

    @Override
    public VectorContainer getOutgoingContainer() { return batch.getOutgoingContainer(); }

    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      return batch.getValueVectorId(path);
    }

    @Override
    public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
       return batch.getValueAccessorById(clazz, ids);
    }

    @Override
    public WritableBatch getWritableBatch() { return batch.getWritableBatch(); }

    @Override
    public SelectionVector2 getSelectionVector2() { return batch.getSelectionVector2(); }

    @Override
    public SelectionVector4 getSelectionVector4() { return batch.getSelectionVector4(); }

    @Override
    public Iterator<VectorWrapper<?>> iterator() { return batch.iterator(); }

    @Override
    public void release() {
      getOutgoingContainer().zeroVectors();
    }
  }

  public static class RecordBatchOperatorHandle implements OperatorHandle {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchOperatorHandle.class);
    private final CloseableRecordBatch batch;
    private final RecordBatchAccessor batchAccessor;

    public RecordBatchOperatorHandle(CloseableRecordBatch batch) {
      this.batch = batch;
      batchAccessor = new RecordBatchAccessor(batch);
    }

    @Override
    public BatchAccessor batchAccessor() { return batchAccessor; }

    @Override
    public boolean buildSchema() {
      IterOutcome outcome = batch.next();
      switch (outcome) {
      case OK_NEW_SCHEMA:
        batchAccessor.bumpVersion();
        return true;
      case NONE:
        return false;
      default:
        throw handleErrors(outcome);
      }
    }

    @Override
    public boolean next() {
      IterOutcome outcome = batch.next();
      switch (outcome) {
      case OK_NEW_SCHEMA:
        batchAccessor.bumpVersion();
        return true;
      case OK:
        return true;
      case NONE:
        return false;
      default:
        throw handleErrors(outcome);
      }
    }

    private RuntimeException handleErrors(IterOutcome outcome) {
      String label = batch.getClass().getSimpleName() + ".next()";
      switch (outcome) {
      case OK_NEW_SCHEMA:
      case OK:
      case NONE:
        // Should not get here.
        return new IllegalStateException();
      case OUT_OF_MEMORY:
        return UserException.memoryError("Out-of-Memory received from {}", label)
          .build(logger);
      case STOP:
        return UserException.executionError(null)
          .addContext("STOP received from", label)
          .build(logger);
      default:
        return new IllegalStateException("Unexpected outcome: " + outcome + " from " + label);
      }
    }

    @Override
    public void cancel(boolean sendUpstream) {
      batch.kill(sendUpstream);
    }

    @Override
    public void close() {
      try {
        batch.close();
      } catch (UserException e) {
        throw e;
      } catch (Exception e) {
        throw UserException.executionError(e)
          .addContext("Received from " + batch.getClass().getSimpleName() + ".close()")
          .build(logger);
      }
    }
  }

  public static class OperatorExecHandle implements OperatorHandle {

    private final OperatorExec opExec;

    public OperatorExecHandle(OperatorExec opExec) {
      this.opExec = opExec;
    }

    @Override
    public BatchAccessor batchAccessor() { return opExec.batchAccessor(); }

    @Override
    public boolean buildSchema() { return opExec.buildSchema(); }

    @Override
    public boolean next() { return opExec.next(); }

    @Override
    public void cancel(boolean sendUpstream) { opExec.cancel(); }

    @Override
    public void close() { opExec.close(); }
  }
}
