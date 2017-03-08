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

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.AbstractColumnWriter;
import org.apache.drill.exec.vector.accessor.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.ColumnAccessor.RowIndex;
import org.apache.drill.test.rowSet.TestRowSet.RowSetWriter;

public class RowSetWriterImpl implements RowSetWriter, RowIndex {

  private TestRowSet recordSet;
  private AbstractColumnWriter writers[];
  private int rowIndex;

  public RowSetWriterImpl(TestRowSet recordSet) {
    this.recordSet = recordSet;
    ValueVector[] valueVectors = recordSet.vectors();
    writers = new AbstractColumnWriter[valueVectors.length];
    for (int i = 0; i < writers.length; i++) {
      writers[i] = ColumnAccessorFactory.newWriter(valueVectors[i].getField().getType());
      writers[i].bind(this, valueVectors[i]);
    }
  }

  @Override
  public int getRow() {
    return rowIndex;
  }

  @Override
  public void advance() {
    rowIndex++;
  }

  @Override
  public ColumnWriter column(int colIndex) {
    return writers[colIndex];
  }

  @Override
  public ColumnWriter column(String colName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void done() {
    recordSet.setRowCount(rowIndex);
  }

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
    } else if (value instanceof byte[]) {
      colWriter.setBytes((byte[]) value);
    } else {
      throw new IllegalArgumentException("Unsupported type " +
                value.getClass().getSimpleName() + " for column " + colIndex);
    }
  }

  @Override
  public void setRow(Object...values) {
    for (int i = 0; i < values.length;  i++) {
      set(i, values[i]);
    }
    advance();
  }
}