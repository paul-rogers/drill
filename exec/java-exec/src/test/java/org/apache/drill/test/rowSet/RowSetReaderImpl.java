/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.test.rowSet;

import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.AbstractColumnReader;
import org.apache.drill.exec.vector.accessor.ColumnAccessor.RowIndex;
import org.apache.drill.exec.vector.accessor.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;

/**
 * Implements a row set reader on top of a {@link RowSet}
 * container.
 */

public class RowSetReaderImpl implements RowSetReader {

  public static abstract class AbstractRowIndex implements RowIndex {
    protected int rowIndex = -1;

    public void advance() { rowIndex++; }
    public int getIndex() { return rowIndex; }
  }

  public static class DirectRowIndex extends AbstractRowIndex {

    @Override
    public int getRow() {
      return rowIndex;
    }
  }

  public static class Sv2RowIndex extends AbstractRowIndex {

    private SelectionVector2 sv2;

    public Sv2RowIndex(SelectionVector2 sv2) {
      this.sv2 = sv2;
    }

    @Override
    public int getRow() {
      return sv2.getIndex(rowIndex);
    }
  }

  private RowSet rowSet;
  private AbstractColumnReader readers[];
  private AbstractRowIndex rowIndex;

  public RowSetReaderImpl(RowSet recordSet) {
    if (recordSet.getSv2() == null) {
      rowIndex = new DirectRowIndex();
    } else {
      rowIndex = new Sv2RowIndex(recordSet.getSv2());
    }
    this.rowSet = recordSet;
    ValueVector[] valueVectors = recordSet.vectors();
    readers = new AbstractColumnReader[valueVectors.length];
    for (int i = 0; i < readers.length; i++) {
      readers[i] = ColumnAccessorFactory.newReader(valueVectors[i].getField().getType());
      readers[i].bind(rowIndex, valueVectors[i]);
    }
  }

  @Override
  public boolean valid() {
    int index = rowIndex.getIndex();
    return index >= 0  && index < rowSet.rowCount();
  }

  @Override
  public boolean next() {
    if (rowIndex.getIndex() >= rowSet.rowCount())
      return false;
    rowIndex.advance();
    return valid();
  }

  @Override
  public ColumnReader column(int colIndex) {
    return readers[colIndex];
  }

  @Override
  public ColumnReader column(String colName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int index() {
    return rowIndex.getIndex();
  }

  @Override
  public int size() {
    return rowSet.rowCount();
  }

  @Override
  public Object get(int colIndex) {
    ColumnReader colReader = column(colIndex);
    if (colReader.isNull()) {
      return null;
    }
    switch (colReader.getType()) {
    case BYTES:
      return colReader.getBytes();
    case DOUBLE:
      return colReader.getDouble();
    case INTEGER:
      return colReader.getInt();
    case LONG:
      return colReader.getLong();
    case STRING:
      return colReader.getString();
    default:
      throw new IllegalArgumentException("Unsupported type " + colReader.getType());
    }
  }

  @Override
  public String getAsString(int colIndex) {
    ColumnReader colReader = column(colIndex);
    if (colReader.isNull()) {
      return "null";
    }
    switch (colReader.getType()) {
    case BYTES:
      StringBuilder buf = new StringBuilder()
          .append("[");
      byte value[] = colReader.getBytes();
      int len = Math.min(value.length, 20);
      for (int i = 0; i < len;  i++) {
        if (i > 0) {
          buf.append(", ");
        }
        buf.append((int) value[i]);
      }
      if (value.length > len) {
        buf.append("...");
      }
      buf.append("]");
      return buf.toString();
    case DOUBLE:
      return Double.toString(colReader.getDouble());
    case INTEGER:
      return Integer.toString(colReader.getInt());
    case LONG:
      return Long.toString(colReader.getLong());
    case STRING:
      return "\"" + colReader.getString() + "\"";
    case DECIMAL:
      return colReader.getDecimal().toPlainString();
    default:
      throw new IllegalArgumentException("Unsupported type " + colReader.getType());
    }
  }

  @Override
  public int width() {
    return rowSet.schema().count();
  }

  @Override
  public int offset() {
    return rowIndex.getRow();
  }
}
