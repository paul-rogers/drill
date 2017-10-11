package org.apache.drill.exec.physical.impl.scan.managed;

public interface ManagedReader {

  boolean open(SchemaNegotiator negotiator);
  boolean next();
  void close();
}
