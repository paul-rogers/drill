package org.apache.drill.exec.store.revised;

import java.math.BigDecimal;

import org.apache.drill.exec.store.revised.Sketch.ColumnBuilder;

public class AbstractColumnBuilder implements ColumnBuilder {

  @Override
  public void setString(String value) {
    throw unsupported("String");
  }

  private IllegalArgumentException unsupported(String type) {
    return new IllegalArgumentException( "Type " + type + " not supported by " + getClass().getSimpleName() );
  }

  @Override
  public void setInt(int value) {
    throw unsupported("int");
  }

  @Override
  public void setLong(long value) {
    throw unsupported("long");
  }

  @Override
  public void setFloat(float value) {
    throw unsupported("float");
  }

  @Override
  public void setDouble(double value) {
    throw unsupported("double");
  }

  @Override
  public void setDecimal(BigDecimal value) {
    throw unsupported("BigDecimal");
  }

  @Override
  public void setNull() {
    throw unsupported("null");
  }

}
