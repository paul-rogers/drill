package org.apache.drill.exec.store.revised.proto;

import java.util.Iterator;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.store.revised.Sketch.ResultSetMaker;
import org.apache.drill.exec.store.revised.Sketch.RowBatch;
import org.apache.drill.exec.store.revised.Sketch.RowBatchMaker;
import org.apache.drill.exec.store.revised.Sketch.RowSetMaker;
import org.apache.drill.exec.store.revised.proto.ProtoPlugin.ProtoSubScanPop;

// TODO: Handle multiple scan pops
// TODO: Handle stats
// TODO: close/abandon readers
// TODO: Implicit columns

public class ScanBatchAdapter  implements CloseableRecordBatch {

  enum ScanState { START, NEW_SCAN, SCAN, EOF, END };

  ResultSetMaker resultSet;
  RowSetMaker rowSet;
  RowBatchMaker batchMaker;
  RowBatch batch;
  ProtoSubScanPop scanPop;
  FragmentContext fragContext;
  OperatorContext opContext;
  ScanState state = ScanState.START;

  public ScanBatchAdapter(ProtoSubScanPop subScanConfig, FragmentContext fragContext,
      OperatorContext opContext) throws ExecutionSetupException {
    scanPop = subScanConfig;
    this.fragContext = fragContext;
    this.opContext = opContext;
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
    switch (state) {
    case END:
      assert false;
    case EOF:
      return IterOutcome.NONE;
    case START:
    case NEW_SCAN:
      return startScan();
    case SCAN:
      return scanBatch();
    default:
      throw new IllegalStateException("State: " + state);
    }
  }

  private IterOutcome startScan() {
    if ( scanIndex >= scanPops.size()) {
      state = ScanState.EOF;
      return IterOutcome.NONE;
    }
    P scanPop = scanPops.get(scanIndex++);
    deserializer =
    // TODO Auto-generated method stub
    return null;
  }

  private IterOutcome scanBatch() {
    // TODO Auto-generated method stub
    return null;
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