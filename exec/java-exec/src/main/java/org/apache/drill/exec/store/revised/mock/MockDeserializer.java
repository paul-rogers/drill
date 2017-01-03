package org.apache.drill.exec.store.revised.mock;

import org.apache.drill.exec.store.revised.AbstractDeserializer;
import org.apache.drill.exec.store.revised.Sketch.RowBatchReceiver;
import org.apache.drill.exec.store.revised.Sketch.RowBuilder;

public class MockDeserializer extends AbstractDeserializer {

  private int totalRows;

  @Override
  public void readBatch() throws Exception {
    if ( receiver().rowCount() >= totalRows ) {
      receiver().close();
      return;
    }
    RowBatchReceiver batch = receiver().rowSet( ).batch();
    int n = 10;
    for ( int i = 0; i < n; i++ ) {
      RowBuilder row = batch.row();
      row.accept();
    }
    batch.close();
  }

}
