package org.apache.drill.exec.vector.accessor2.impl;

import org.apache.drill.exec.vector.complex.RepeatedMapVector;

public class RepeatedMapWriterImpl extends AbstractObjectWriter {

  private class ArrayWriterImpl extends AbstractArrayWriter {

  }

  private class MapWriterImpl extends AbstractTupleWriter {

  }

  public RepeatedMapWriterImpl(ExtendableRowIndex rowIndex,
      RepeatedMapVector vector) {
    MapWriterImpl mapWriter = new MapWriterImpl(sth, vector);
    MapObjectWriter mapObjWriter = new MapObjectWriter(mapWriter);
    arrayWriter = new ArrayObjectWriter(rowIndex, mapObjWriter);
  }

  @Override
  public ObjectType type() {
    return ObjectType.ARRAY;
  }

  @Override
  public void set(Object value) {
    // TODO Auto-generated method stub

  }

}
