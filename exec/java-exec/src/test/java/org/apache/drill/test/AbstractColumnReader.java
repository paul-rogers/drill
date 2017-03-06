package org.apache.drill.test;

import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.test.TestRowSet.ColumnReader;

import com.google.common.base.Charsets;

public abstract class AbstractColumnReader extends ColumnAccessor implements ColumnReader {

  @Override
  public boolean isNull() {
    return false;
  }

  @Override
  public int getInt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString() {
    throw new UnsupportedOperationException();
  }

  public static class IntColumnReader extends AbstractColumnReader {

    private IntVector.Accessor accessor;

    @Override
    protected void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.accessor = ((IntVector) vector).getAccessor();
    }

    @Override
    public int getInt() {
      return accessor.get(rowIndex());
    }
  }

  public static class VarCharColumnReader extends AbstractColumnReader {

    private VarCharVector.Accessor accessor;

    @Override
    protected void bind(RowIndex rowIndex, ValueVector vector) {
      bind(rowIndex);
      this.accessor = ((VarCharVector) vector).getAccessor();
    }

    @Override
    public String getString() {
      return new String(accessor.get(rowIndex()), Charsets.UTF_8);
    }
  }

}