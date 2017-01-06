package org.apache.drill.exec.store.revised.proto;

import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

public class ProtoScanBatch implements CloseableRecordBatch {

  @Override
  public FragmentContext getContext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BatchSchema getSchema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getRecordCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void kill(boolean sendUpstream) {
    // TODO Auto-generated method stub

  }

  @Override
  public VectorContainer getOutgoingContainer() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IterOutcome next() {
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub

  }

}
