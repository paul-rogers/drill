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
package org.apache.drill.exec.vector.accessor.writer;

import java.math.BigDecimal;

import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessors.UInt1ColumnWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.joda.time.Period;

public class NullableScalarWriter extends AbstractScalarWriter {

  private final NullableVector vector;
  private final UInt1ColumnWriter isSetWriter;
  private final BaseScalarWriter baseWriter;

  public NullableScalarWriter(NullableVector nullableVector, BaseScalarWriter baseWriter) {
    vector = nullableVector;
    isSetWriter = new UInt1ColumnWriter(nullableVector.getBitsVector());
    this.baseWriter = baseWriter;
  }

  public static ScalarObjectWriter build(NullableVector nullableVector, BaseScalarWriter baseWriter) {
    return new ScalarObjectWriter(
        new NullableScalarWriter(nullableVector, baseWriter));
  }

  @Override
  public ValueVector vector() { return vector; }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    isSetWriter.bindIndex(index);
    baseWriter.bindIndex(index);
  }

  @Override
  public ValueType valueType() {
    return baseWriter.valueType();
  }

  @Override
  public void restartRow() {
    isSetWriter.restartRow();
    baseWriter.restartRow();
  }

  @Override
  public void setNull() {
    isSetWriter.setInt(0);
    baseWriter.skipNulls();
  }

  @Override
  public void setInt(int value) {
    isSetWriter.setInt(1);
    baseWriter.setInt(value);
  }

  @Override
  public void setLong(long value) {
    isSetWriter.setInt(1);
    baseWriter.setLong(value);
  }

  @Override
  public void setDouble(double value) {
    isSetWriter.setInt(1);
    baseWriter.setDouble(value);
  }

  @Override
  public void setString(String value) {
    isSetWriter.setInt(1);
    baseWriter.setString(value);
  }

  @Override
  public void setBytes(byte[] value, int len) {
    isSetWriter.setInt(1);
    baseWriter.setBytes(value, len);
  }

  @Override
  public void setDecimal(BigDecimal value) {
    isSetWriter.setInt(1);
    baseWriter.setDecimal(value);
  }

  @Override
  public void setPeriod(Period value) {
    isSetWriter.setInt(1);
    baseWriter.setPeriod(value);
  }

  @Override
  public void startWriteAt(int index) {
    isSetWriter.startWriteAt(index);
    baseWriter.startWriteAt(index);
  }

  @Override
  public int lastWriteIndex() {
    return baseWriter.lastWriteIndex();
  }

  @Override
  public void bindListener(ColumnWriterListener listener) {
    baseWriter.bindListener(listener);
  }

  @Override
  public void startWrite() {
    isSetWriter.startWrite();
    baseWriter.startWrite();
  }

  @Override
  public void startRow() {
    // Skip calls for performance: they do nothing for
    // scalar writers -- the only kind supported here.
//    isSetWriter.startValue();
//    baseWriter.startValue();
  }

  @Override
  public void saveValue() {
    // Skip calls for performance: they do nothing for
    // scalar writers -- the only kind supported here.
//    isSetWriter.saveValue();
    baseWriter.saveValue();
  }

  @Override
  public void endWrite() {
    isSetWriter.endWrite();
    // Avoid back-filling null values.
    baseWriter.skipNulls();
    baseWriter.endWrite();
  }

  @Override
  public void dump(HierarchicalFormatter format) {
    format.extend();
    super.dump(format);
    format.attribute("isSetWriter");
    isSetWriter.dump(format);
    format.attribute("baseWriter");
    baseWriter.dump(format);
    format.endObject();
  }
}
