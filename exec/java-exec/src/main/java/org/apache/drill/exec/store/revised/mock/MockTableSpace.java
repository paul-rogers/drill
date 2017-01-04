package org.apache.drill.exec.store.revised.mock;

import java.util.Iterator;

import org.apache.drill.exec.store.revised.Sketch.LogicalTable;
import org.apache.drill.exec.store.revised.Sketch.StorageSpace;
import org.apache.drill.exec.store.revised.Sketch.TableSpace;

public class MockTableSpace implements TableSpace {

  @Override
  public Iterator<LogicalTable> tables() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public LogicalTable table(String name) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public StorageSpace storage() {
    // TODO Auto-generated method stub
    return null;
  }

}
