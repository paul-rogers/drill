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

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.joda.time.Period;

/**
 * Column writer implementation that acts as the basis for the
 * generated, vector-specific implementations. All set methods
 * throw an exception; subclasses simply override the supported
 * method(s).
 */

public abstract class AbstractScalarWriter implements ScalarWriter, WriterEvents {

  public static class ScalarObjectWriter extends AbstractObjectWriter {

    private AbstractScalarWriter scalarWriter;

    public ScalarObjectWriter(AbstractScalarWriter scalarWriter) {
      this.scalarWriter = scalarWriter;
    }

    @Override
    public void bindIndex(ColumnWriterIndex index) {
      scalarWriter.bindIndex(index);
    }

    @Override
    public ObjectType type() {
      return ObjectType.SCALAR;
    }

    @Override
    public void set(Object value) {
      scalarWriter.setObject(value);
    }

    public void start() {
      scalarWriter.startWrite();
    }

    @Override
    public ScalarWriter scalar() {
      return scalarWriter;
    }

    @Override
    public void startWrite() {
      scalarWriter.startWrite();
    }

    @Override
    public void startValue() {
      scalarWriter.startValue();
    }

    @Override
    public void endValue() {
      scalarWriter.endValue();
    }

    @Override
    public void endWrite() {
      scalarWriter.endWrite();
    }
  }

  public abstract void bindIndex(ColumnWriterIndex index);

  public abstract void bindVector(ValueVector vector);

  @Override
  public void setObject(Object value) {
    if (value == null) {
      setNull();
    } else if (value instanceof Integer) {
      setInt((Integer) value);
    } else if (value instanceof Long) {
      setLong((Long) value);
    } else if (value instanceof String) {
      setString((String) value);
    } else if (value instanceof BigDecimal) {
      setDecimal((BigDecimal) value);
    } else if (value instanceof Period) {
      setPeriod((Period) value);
    } else if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      setBytes(bytes, bytes.length);
    } else if (value instanceof Byte) {
      setInt((Byte) value);
    } else if (value instanceof Short) {
      setInt((Short) value);
    } else if (value instanceof Double) {
      setDouble((Double) value);
    } else if (value instanceof Float) {
      setDouble((Float) value);
    } else {
      throw new IllegalArgumentException("Unsupported type " +
                value.getClass().getSimpleName());
    }
  }

  @Override
  public void startWrite() { }

  @Override
  public void startValue() { }

  @Override
  public void endValue() { }

  @Override
  public void endWrite() {
    finish();
  }

  /**
   * Overridden by generated classes to finish up writing. Such as
   * setting the final element count.
   *
   * @throws VectorOverflowException should not actually occur
   */

  public void finish() { }
}
