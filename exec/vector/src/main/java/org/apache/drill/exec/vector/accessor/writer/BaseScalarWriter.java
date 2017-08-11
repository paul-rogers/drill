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

import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.joda.time.Period;

/**
 * Column writer implementation that acts as the basis for the
 * generated, vector-specific implementations. All set methods
 * throw an exception; subclasses simply override the supported
 * method(s).
 */

public abstract class BaseScalarWriter extends AbstractScalarWriter {

  public static abstract class BaseVarWidthWriter extends BaseScalarWriter {
    protected final OffsetVectorWriter offsetsWriter;

    public BaseVarWidthWriter(UInt4Vector offsetVector) {
      offsetsWriter = new OffsetVectorWriter(offsetVector);
    }

    @Override
    public int lastWriteIndex() { return offsetsWriter.lastWriteIndex() - 1; }

    @Override
    public void setLastWriteIndex(int index) {
      offsetsWriter.setLastWriteIndex(index);
    }
  }

  protected ColumnWriterIndex vectorIndex;
  protected ColumnWriterListener listener;
  protected int lastWriteIndex;
  protected long bufAddr;
  protected int capacity;

  @Override
  public void bindIndex(ColumnWriterIndex vectorIndex) {
    this.vectorIndex = vectorIndex;
  }

  @Override
  public void bindListener(ColumnWriterListener listener) {
    this.listener = listener;
  }

  @Override
  public void startWrite() { lastWriteIndex = -1; }

  @Override
  public int lastWriteIndex() { return lastWriteIndex; }

  public void setLastWriteIndex(int index) {
    lastWriteIndex = index;
  }

  protected void overflowed() {
    if (listener == null) {
      throw new UnsupportedOperationException("Overflow");
    } else {
      listener.overflowed(this);
    }
  }

  @Override
  public void setNull() {
    throw new UnsupportedOperationException("Vector is not nullable");
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
  public void setBytes(byte[] value, int len) {
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
