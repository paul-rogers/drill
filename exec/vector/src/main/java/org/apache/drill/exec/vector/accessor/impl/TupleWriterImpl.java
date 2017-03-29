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
package org.apache.drill.exec.vector.accessor.impl;

import java.math.BigDecimal;

import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.joda.time.Period;

public class TupleWriterImpl extends AbstractTupleAccessor implements TupleWriter {

  private final AbstractColumnWriter writers[];

  public TupleWriterImpl(AccessSchema schema, AbstractColumnWriter writers[]) {
    super(schema);
    this.writers = writers;
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
}
