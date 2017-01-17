package org.apache.drill.exec.store.revised;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.store.revised.Sketch.RowBatch;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;

public class RowBatchImpl implements RowBatch {

  @Override
  public RowSchema schema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int rowCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public BatchSchema batchSchema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addSv2() {
    // TODO Auto-generated method stub

  }

  @Override
  public void addSv4() {
    // TODO Auto-generated method stub

  }

  @Override
  public SelectionVector2 sv2() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SelectionVector4 sv4() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int findPath(SchemaPath path) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public TypedFieldId vectorId(int n) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VectorWrapper<?> getVector(int n) {
    // TODO Auto-generated method stub
    return null;
  }

}
