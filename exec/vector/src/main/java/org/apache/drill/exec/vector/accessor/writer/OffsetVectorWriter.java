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

import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessors;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Specialized column writer for the (hidden) offset vector used
 * with variable-length or repeated vectors. See comments in the
 * <tt>ColumnAccessors.java</tt> template file for more details.
 * <p>
 * Note that the <tt>lastWriteIndex</tt> tracked here corresponds
 * to the data values; it is one less than the actual offset vector
 * last write index due to the nature of offset vector layouts. The selection
 * of last write index basis makes roll-over processing easier as only this
 * writer need know about the +1 translation required for writing.
 */

public class OffsetVectorWriter extends BaseScalarWriter {

  private static final int VALUE_WIDTH = UInt4Vector.VALUE_WIDTH;

  private UInt4Vector vector;

  /**
   * Offset into the data vector that this offset vector targets. Caching
   * the value here avoids the need to query the offset vector for every
   * new value. This is the value for the current row.
   */

  private int targetOffset;

  /**
   * Cached offset value for the next row, rotated into the current
   * row at the completion of each value.
   */

  private int nextOffset;

  public OffsetVectorWriter(UInt4Vector vector) {
    this.vector = vector;
  }

  @Override
  public ValueVector vector() { return vector; }

  @Override
  public void startWrite() {

    // Special handling for first value. Alloc vector if needed.
    // Offset vectors require a 0 at position 0. The (end) offset
    // for row 0 starts at position 1, which is handled in
    // writeOffset() below.

    targetOffset = 0;
    nextOffset = 0;
    lastWriteIndex = -1;
    setAddr(vector.getBuffer());
    if (capacity < ColumnAccessors.MIN_BUFFER_SIZE) {
      vector.reallocRaw(ColumnAccessors.MIN_BUFFER_SIZE * VALUE_WIDTH);
      setAddr(vector.getBuffer());
    }
    PlatformDependent.putInt(bufAddr, targetOffset);
  }

  @Override
  public void startWriteAt(int newIndex) {
    startWrite();
    lastWriteIndex = newIndex;
    nextOffset = PlatformDependent.getInt(bufAddr + (lastWriteIndex + 1) * VALUE_WIDTH);
  }

  private final void setAddr(final DrillBuf buf) {
    bufAddr = buf.addr();
    capacity = buf.capacity() / VALUE_WIDTH;
  }

  public int targetOffset() { return targetOffset; }

  @Override
  public ValueType valueType() {
    return ValueType.INTEGER;
  }

  /**
   * Return the write offset, which is one greater than the index reported
   * by the vector index.
   *
   * @return the offset in which to write the current offset of the end
   * of the current data value
   */

  private final int writeIndex() {
    int valueIndex = vectorIndex.vectorIndex();
    int writeIndex = valueIndex + 1;
    if (lastWriteIndex + 1 == valueIndex && writeIndex < capacity) {
      lastWriteIndex = valueIndex;
      return writeIndex;
    }
    if (writeIndex >= capacity) {
      int size = (writeIndex + 1) * VALUE_WIDTH;
      if (size > ValueVector.MAX_BUFFER_SIZE) {
        throw new IllegalStateException("Offset vectors should not overflow");
      } else {
        if (size < ColumnAccessors.MIN_BUFFER_SIZE) {
          size = ColumnAccessors.MIN_BUFFER_SIZE;
        }
        setAddr(vector.reallocRaw(BaseAllocator.nextPowerOfTwo(size)));
      }
    }
    while (lastWriteIndex < valueIndex - 1) {
      PlatformDependent.putInt(bufAddr + (++lastWriteIndex + 1) * VALUE_WIDTH, targetOffset);
    }
    lastWriteIndex = valueIndex;
    return writeIndex;
  }

  public final void setOffset(final int curOffset) {
    final int writeIndex = writeIndex();
    PlatformDependent.putInt(bufAddr + writeIndex * VALUE_WIDTH, curOffset);

    // Next offset is set aside for now. Allows rewriting the present
    // value any number of times using the current offset.

    nextOffset = curOffset;
  }

  @Override
  public void skipNulls() {

    // To skip nulls, must carry forward the the last end offset.

    setOffset(targetOffset);
  }

  @Override
  public void restartRow() {
    super.restartRow();
    targetOffset = PlatformDependent.getInt(bufAddr + (lastWriteIndex + 1) * VALUE_WIDTH);
    nextOffset = targetOffset;
  }

  @Override
  public final void saveValue() {

    // Value is ending. Advance the offset to the next position.

    targetOffset = nextOffset;
  }

  @Override
  public final void endWrite() {
    final int finalIndex = writeIndex();
    vector.getBuffer().writerIndex(finalIndex * VALUE_WIDTH);
  }

  @Override
  public void bindListener(ColumnWriterListener listener) {

    // No listener for overflow vectors as they can't overflow.

    throw new UnsupportedOperationException();
  }

  @Override
  public void dump(HierarchicalFormatter format) {
    format.extend();
    super.dump(format);
    format
      .attribute("targetOffset", targetOffset)
      .attribute("nextOffset", nextOffset)
      .endObject();
  }
}
