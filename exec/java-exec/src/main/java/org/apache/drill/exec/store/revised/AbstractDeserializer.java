package org.apache.drill.exec.store.revised;

import org.apache.drill.exec.store.revised.Sketch.Deserializer;
import org.apache.drill.exec.store.revised.Sketch.ScanReceiver;
import org.apache.drill.exec.store.revised.Sketch.ScanOperation;

public abstract class AbstractDeserializer implements Deserializer {

  private ScanOperation scanService;

  @Override
  public void bind(ScanOperation service) {
    scanService = service;
  }

  public ScanOperation scanService() { return scanService; }
  public ScanReceiver receiver() { return scanService.receiver(); }

  @Override
  public void open() throws Exception {
  }

  @Override
  public void close() throws Exception {
  }

}
