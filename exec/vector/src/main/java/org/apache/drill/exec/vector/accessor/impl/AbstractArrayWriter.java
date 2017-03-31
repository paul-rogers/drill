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

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.joda.time.Period;

public abstract class AbstractArrayWriter extends AbstractColumnAccessor implements ArrayWriter {

  public static class ArrayColumnWriter extends AbstractColumnWriter {

    private final AbstractArrayWriter arrayWriter;

    public ArrayColumnWriter(AbstractArrayWriter arrayWriter) {
      this.arrayWriter = arrayWriter;
    }

    @Override
    public ValueType valueType() {
      return ValueType.ARRAY;
    }

    @Override
    public void bind(RowIndex rowIndex, ValueVector vector) {
      arrayWriter.bind(rowIndex, vector);
    }

    @Override
    public ArrayWriter array() {
      return arrayWriter;
    }
  }

  protected abstract BaseRepeatedValueVector.BaseRepeatedMutator mutator();

  @Override
  public int size() {
    return mutator().getInnerValueCountAt(vectorIndex.index());
  }

  public void start() {
    mutator().setValueCount(vectorIndex.index());
  }

  @Override
  public boolean valid() {
    // Not implemented yet
    return true;
  }

  @Override
  public void setInt(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDouble(double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setString(String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBytes(byte[] value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDecimal(BigDecimal value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPeriod(Period value) {
    throw new UnsupportedOperationException();
  }
}
