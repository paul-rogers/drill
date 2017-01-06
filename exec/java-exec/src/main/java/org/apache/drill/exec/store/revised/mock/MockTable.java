package org.apache.drill.exec.store.revised.mock;

import org.apache.drill.exec.store.revised.ExtendableLogicalScanPop;
import org.apache.drill.exec.store.revised.Sketch.LogicalTable;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;

public class MockTable implements LogicalTable {

  private String name;

  public MockTable( String name ) {
    this.name = name;
  }
  @Override
  public String name() {
    return name;
  }

  @Override
  public int capabilites() {
    return READ;
  }

  @Override
  public boolean staticSchema() {
    return false;
  }

  @Override
  public RowSchema schema() {
    return null;
  }
  @Override
  public <P> ExtendableLogicalScanPop<P> scan() {
    // TODO Auto-generated method stub
    return null;
  }

}
