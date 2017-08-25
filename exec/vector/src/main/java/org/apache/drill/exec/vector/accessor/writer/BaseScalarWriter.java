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
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.joda.time.Period;

/**
 * Column writer implementation that acts as the basis for the
 * generated, vector-specific implementations. All set methods
 * throw an exception; subclasses simply override the supported
 * method(s).
 */

public abstract class BaseScalarWriter extends AbstractScalarWriter {

  /**
   * Base class for variable-width (VarChar, VarBinary, etc.) writers.
   * Handles the additional complexity that such writers work with
   * both an offset vector and a data vector. The offset vector is
   * written using a specialized offset vector writer. The last write
   * index is defined as the the last write position in the offset
   * vector; not the last write position in the variable-width
   * vector.
   */

  public static abstract class BaseVarWidthWriter extends BaseScalarWriter {
    protected final OffsetVectorWriter offsetsWriter;

    public BaseVarWidthWriter(UInt4Vector offsetVector) {
      offsetsWriter = new OffsetVectorWriter(offsetVector);
    }

    @Override
    public void saveValue() { offsetsWriter.saveValue(); }

    @Override
    public int lastWriteIndex() { return offsetsWriter.lastWriteIndex(); }

    @Override
    public void skipNulls() { offsetsWriter.skipNulls(); }

    @Override
    public void restartRow() { offsetsWriter.restartRow(); }

    @Override
    public void dump(HierarchicalFormatter format) {
      format.extend();
      super.dump(format);
      format.attribute("offsetsWriter");
      offsetsWriter.dump(format);
      format.endObject();
    }
  }

  /**
   * Indicates the position in the vector to write. Set via an object so that
   * all writers (within the same subtree) can agree on the write position.
   * For example, all top-level, simple columns see the same row index.
   * All columns within a repeated map see the same (inner) index, etc.
   */

  protected ColumnWriterIndex vectorIndex;

  /**
   * Listener invoked if the vector overflows. If not provided, then the writer
   * does not support vector overflow.
   */

  protected ColumnWriterListener listener;

  /**
   * The largest position to which the writer has written data. Used to allow
   * "fill-empties" (AKA "back-fill") of missing values one each value write
   * and at the end of a batch. Note that this is the position of the last
   * write, not the next write position. Starts at -1 (no last write).
   */

  protected int lastWriteIndex;

  /**
   * Cached direct memory location of the start of data for the vector
   * being written. Updated each time the buffer is reallocated.
   */

  protected long bufAddr;

  /**
   * Capacity, in values, of the currently allocated buffer that backs
   * the vector. Updated each time the buffer changes. The capacity is in
   * values (rather than bytes) to streamline the per-write logic.
   */

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

  public void skipNulls() {
    lastWriteIndex = vectorIndex.vectorIndex();
  }

  protected void overflowed() {
    if (listener == null) {
      throw new UnsupportedOperationException("Overflow");
    } else {
      listener.overflowed(this);
    }
  }

  @Override
  public void restartRow() {
    lastWriteIndex = Math.min(lastWriteIndex, vectorIndex.vectorIndex() - 1);
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

  @Override
  public void dump(HierarchicalFormatter format) {
    format.extend();
    super.dump(format);
    format
      .attribute("vectorIndex", vectorIndex)
      .attributeIdentity("listener", listener)
      .attribute("lastWriteIndex", lastWriteIndex)
      .attribute("capacity", capacity)
      .endObject();
  }
}
