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

import java.math.BigDecimal;

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.api.Exp.ColumnWriterIterator;
import org.apache.drill.exec.vector.accessor.api.Exp.TupleWriterIterator;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.joda.time.Period;

/**
 * Implements a row set writer on top of a {@link RowSet}
 * container.
 */

public class RowSetWriterImpl extends AbstractRowSetAccessor implements RowSetWriter {

  private final AbstractColumnWriter writers[];

  public RowSetWriterImpl(AbstractSingleRowSet recordSet, AbstractRowIndex rowIndex) {
    super(rowIndex, recordSet.schema().access());
    ValueVector[] valueVectors = recordSet.vectors();
    writers = new AbstractColumnWriter[valueVectors.length];
    int posn = 0;
    for (int i = 0; i < writers.length; i++) {
      writers[posn] = ColumnAccessorFactory.newWriter(valueVectors[i].getField().getType());
      writers[posn].bind(rowIndex, valueVectors[i]);
      posn++;
    }
  }

  @Override
  public ColumnWriter column(int colIndex) {
    return writers[colIndex];
  }

  @Override
  public ColumnWriter column(String colName) {
    int index = schema.columnIndex(colName);
    if (index == -1) {
      return null; }
    return writers[index];
  }

  @Override
  public void done() { index.setRowCount(); }

  @Override
  public void set(int colIndex, Object value) {
    ColumnWriter colWriter = column(colIndex);
    if (value == null) {
      colWriter.setNull();
    } else if (value instanceof Integer) {
      colWriter.setInt((Integer) value);
    } else if (value instanceof Long) {
      colWriter.setLong((Long) value);
    } else if (value instanceof String) {
      colWriter.setString((String) value);
    } else if (value instanceof BigDecimal) {
      colWriter.setDecimal((BigDecimal) value);
    } else if (value instanceof Period) {
      colWriter.setPeriod((Period) value);
    } else if (value instanceof byte[]) {
      colWriter.setBytes((byte[]) value);
    } else if (value instanceof Byte) {
      colWriter.setInt((Byte) value);
    } else if (value instanceof Short) {
      colWriter.setInt((Short) value);
    } else if (value instanceof Double) {
      colWriter.setDouble((Double) value);
    } else if (value instanceof Float) {
      colWriter.setDouble((Float) value);
    } else {
      throw new IllegalArgumentException("Unsupported type " +
                value.getClass().getSimpleName() + " for column " + colIndex);
    }
  }

  @Override
  public boolean setRow(Object...values) {
    if (! index.valid()) {
      throw new IndexOutOfBoundsException("Write past end of row set");
    }
    for (int i = 0; i < values.length;  i++) {
      set(i, values[i]);
    }
    return next();
  }

  @Override
  public int width() { return writers.length; }

  @Override
  public ColumnWriterIterator array(int colIndex) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ColumnWriterIterator array(String colName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TupleWriterIterator mapList(int mapIndex) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TupleWriterIterator mapList(String mapName) {
    // TODO Auto-generated method stub
    return null;
  }
}