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

import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.api.Exp.ColumnReaderIterator;
import org.apache.drill.exec.vector.accessor.api.Exp.TupleReaderIterator;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnReader;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnAccessor.VectorAccessor;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.test.rowSet.HyperRowSetImpl.HyperRowIndex;
import org.apache.drill.test.rowSet.RowSet.HyperRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

/**
 * Implements a row set reader on top of a {@link RowSet}
 * container.
 */

public class RowSetReaderImpl extends AbstractRowSetAccessor implements RowSetReader {


  public static class HyperVectorAccessor implements VectorAccessor {

    private final HyperRowIndex rowIndex;
    private final ValueVector[] vectors;

    public HyperVectorAccessor(HyperVectorWrapper<ValueVector> hvw, HyperRowIndex rowIndex) {
      this.rowIndex = rowIndex;
      vectors = hvw.getValueVectors();
    }

    @Override
    public ValueVector vector() {
      return vectors[rowIndex.batch()];
    }
  }

  private final AbstractColumnReader readers[];

  public RowSetReaderImpl(SingleRowSet recordSet, AbstractRowIndex rowIndex) {
    super(rowIndex, recordSet.schema().access());
    ValueVector[] valueVectors = recordSet.vectors();
    readers = new AbstractColumnReader[valueVectors.length];
    for (int i = 0; i < readers.length; i++) {
      readers[i] = ColumnAccessorFactory.newReader(valueVectors[i].getField().getType());
      readers[i].bind(rowIndex, valueVectors[i]);
    }
  }

  public RowSetReaderImpl(HyperRowSet recordSet, HyperRowIndex rowIndex) {
    super(rowIndex, recordSet.schema().access());
    readers = new AbstractColumnReader[schema.count()];
    for (int i = 0; i < readers.length; i++) {
      MaterializedField field = schema.column(i);
      readers[i] = ColumnAccessorFactory.newReader(field.getType());
      HyperVectorWrapper<ValueVector> hvw = recordSet.getHyperVector(i);
      readers[i].bind(rowIndex, field, new HyperVectorAccessor(hvw, rowIndex));
    }
  }

  @Override
  public ColumnReader column(int colIndex) {
    return readers[colIndex];
  }

  @Override
  public ColumnReader column(String colName) {
    int index = schema.columnIndex(colName);
    if (index == -1) {
      return null; }
    return readers[index];
  }

  @Override
  public Object get(int colIndex) {
    ColumnReader colReader = column(colIndex);
    if (colReader.isNull()) {
      return null; }
    switch (colReader.valueType()) {
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
      throw new IllegalArgumentException("Unsupported type " + colReader.valueType());
    }
  }

  @Override
  public String getAsString(int colIndex) {
    ColumnReader colReader = column(colIndex);
    if (colReader.isNull()) {
      return "null";
    }
    switch (colReader.valueType()) {
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
      throw new IllegalArgumentException("Unsupported type " + colReader.valueType());
    }
  }

  @Override
  public int width() {
    return readers.length;
  }

  @Override
  public ColumnReaderIterator array(int colIndex) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ColumnReaderIterator array(String colName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TupleReaderIterator mapList(int mapIndex) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TupleReaderIterator mapList(String mapName) {
    // TODO Auto-generated method stub
    return null;
  }
}