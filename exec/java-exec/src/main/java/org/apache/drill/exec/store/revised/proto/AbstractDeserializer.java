package org.apache.drill.exec.store.revised.proto;

import org.apache.drill.exec.store.revised.Sketch.Deserializer;
import org.apache.drill.exec.store.revised.Sketch.ResultSetMaker;
import org.apache.drill.exec.store.revised.Sketch.RowSetMaker;
import org.apache.drill.exec.store.revised.Sketch.ScanOperation;

public abstract class AbstractDeserializer<P> implements Deserializer<P> {

  public ScanOperation<P> service;
  protected RowSetMaker rowSet;

  @Override
  public void bind(ScanOperation<P> service) {
    this.service = service;
  }

  @Override
  public void open() throws Exception {
  }

  @Override
  public void close() throws Exception {
  }

  public P definition() { return service.definition(); }
  public ResultSetMaker resultSet() { return service.resultSet(); }

}
