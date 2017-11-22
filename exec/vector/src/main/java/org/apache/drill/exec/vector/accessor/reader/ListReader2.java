package org.apache.drill.exec.vector.accessor.reader;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.joda.time.Period;

public class ListReader2 {

  public class ObjectListReader extends AbstractArrayReader {

    @Override
    public ObjectType entryType() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setPosn(int index) {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean next() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Object getObject() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getAsString() {
      // TODO Auto-generated method stub
      return null;
    }

  }


  private static class ScalarListElementReader extends BaseElementReader {
    private final BaseScalarReader baseReader;

    public ScalarListElementReader(BaseScalarReader scalarReader) {
      this.baseReader = scalarReader;
      nullStateReader = new NullStateReader() {

        @Override
        public void bindIndex(ColumnReaderIndex rowIndex) { }

        @Override
        public void bindVector(ValueVector vector) { }

        @Override
        public void bindVectorAccessor(VectorAccessor va) { }

        @Override
        public boolean isNull() {
          return baseReader.isNull();
        }
      };
    }

    @Override
    public ValueType valueType() { return baseReader.valueType(); }

    @Override
    public void bindVector(ValueVector vector) {
      baseReader.bindVector(vector);
    }

    @Override
    public void bindVectorAccessor(MajorType majorType, VectorAccessor va) {
      super.bindVectorAccessor(majorType, va);
      baseReader.bindVectorAccessor(majorType, va);
    }

    @Override
    public void bindIndex(ColumnReaderIndex rowIndex) {
      super.bindIndex(rowIndex);
      baseReader.bindIndex(rowIndex);
    }

    private void setPosn(int index) {
      readerIndex.setPosn(vectorIndex.vectorIndex(index));
    }

    @Override
    public boolean isNull(int index) {
      setPosn(index);
      return nullStateReader.isNull();
    }

    @Override
    public int getInt(int index) {
      setPosn(index);
      return baseReader.getInt();
    }

    @Override
    public long getLong(int index) {
      setPosn(index);
      return baseReader.getLong();
    }

    @Override
    public double getDouble(int index) {
      setPosn(index);
      return baseReader.getDouble();
    }

    @Override
    public String getString(int index) {
      setPosn(index);
      return baseReader.getString();
    }

    @Override
    public byte[] getBytes(int index) {
      setPosn(index);
      return baseReader.getBytes();
    }

    @Override
    public BigDecimal getDecimal(int index) {
      setPosn(index);
      return baseReader.getDecimal();
    }

    @Override
    public Period getPeriod(int index) {
      setPosn(index);
      return baseReader.getPeriod();
    }
  }

  public static class ScalarListReader extends AbstractArrayReader {
    private final BaseScalarReader baseReader;

    @Override
    public ObjectType entryType() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setPosn(int index) {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean next() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Object getObject() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getAsString() {
      // TODO Auto-generated method stub
      return null;
    }

  }
}
