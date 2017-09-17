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
 * <p>
 * The states illustrated in the base class apply here as well,
 * remembering that the end offset for a row (or array position)
 * is written one ahead of the vector index.
 * <p>
 * The vector index does create an interesting dynamic for the child
 * writers. From the child writer's perspective, the states described in
 * the super class are the only states of interest. Here we want to
 * take the perspective of the parent.
 * <p>
 * The offset vector is an implementation of a repeat level. A repeat
 * level can occur for a single array, or for a collection of columns
 * within a repeated map. (A repeat level also occurs for variable-width
 * fields, but this is a bit harder to see, so let's ignore that for
 * now.)
 * <p>
 * The key point to realize is that each repeat level introduces an
 * isolation level in terms of indexing. That is, empty values in the
 * outer level have no affect on indexing in the inner level. In fact,
 * the nature of a repeated outer level means that there are no empties
 * in the inner level.
 * <p>
 * To illustrate:<pre><code>
 *       Offset Vector          Data Vector   Indexes
 *  lw, v > | 10 |   - - - - - >   | X |        10
 *          | 12 |   - - +         | X | < lw'  11
 *          |    |       + - - >   |   | < v'   12
 * </code></pre>
 * In the above, the client has just written an array of two elements
 * at the current write position. The data starts at offset 10 in
 * the data vector, and the next write will be at 12. The end offset
 * is written one ahead of the vector index.
 * <p>
 * From the data vector's perspective, its last-write (lw') reflects
 * the last element written. If this is an array of scalars, then the
 * write index is automatically incremented, as illustrated by v'.
 * (For map arrays, the index must be incremented by calling
 * <tt>save()</tt> on the map array writer.)
 * <p>
 * Suppose the client now skips some arrays:<pre><code>
 *       Offset Vector          Data Vector
 *     lw > | 10 |   - - - - - >   | X |        10
 *          | 12 |   - - +         | X | < lw'  11
 *          |    |       + - - >   |   | < v'   12
 *          |    |                 |   |        13
 *      v > |    |                 |   |        14
 * </code></pre>
 * The last write position does not move and there are gaps in the
 * offset vector. The vector index points to the current row. Note
 * that the data vector last write and vector indexes do not change,
 * this reflects the fact that the the data vector's vector index
 * (v') matches the tail offset
 * <p>
 * The
 * client now writes a three-element vector:<pre><code>
 *       Offset Vector          Data Vector
 *          | 10 |   - - - - - >   | X |        10
 *          | 12 |   - - +         | X |        11
 *          | 12 |   - - + - - >   | Y |        12
 *          | 12 |   - - +         | Y |        13
 *  lw, v > | 12 |   - - +         | Y | < lw'  14
 *          | 15 |   - - - - - >   |   | < v'   15
 * </code></pre>
 * Quite a bit just happened. The empty offset slots were back-filled
 * with the last write offset in the data vector. The client wrote
 * three values, which advanced the last write and vector indexes
 * in the data vector. And, the last write index in the offset
 * vector also moved to reflect the update of the offset vector.
 * Note that as a result, multiple positions in the offset vector
 * point to the same location in the data vector. This is fine; we
 * compute the number of entries as the difference between two successive
 * offset vector positions, so the empty positions have become 0-length
 * arrays.
 * <p>
 * Note that, for an array of scalars, when overflow occurs,
 * we need only worry about two
 * states in the data vector. Either data has been written for the
 * row (as in the third example above), and so must be moved to the
 * roll-over vector, or no data has been written and no move is
 * needed. We never have to worry about missing values because the
 * cannot occur in the data vector.
 * <p>
 * See {@link ObjectArrayWriter} for information about arrays of
 * maps (arrays of multiple columns.)
 */

public class OffsetVectorWriter extends BaseScalarWriter {

  private static final int VALUE_WIDTH = UInt4Vector.VALUE_WIDTH;

  private UInt4Vector vector;

  protected int lastWriteIndex;

  /**
   * Offset of the first value for the current row. Used during
   * overflow or if the row is restarted.
   */

  private int rowStartOffset;

  /**
   * Cached value of the end offset for the current value. Used
   * primarily for variable-width columns to allow the column to be
   * rewritten multiple times within the same row. The start offset
   * value is updated with the end offset only when the value is
   * committed in {@link @endValue()}.
   */

  private int nextOffset;

  public OffsetVectorWriter(UInt4Vector vector) {
    this.vector = vector;
  }

  @Override
  public ValueVector vector() { return vector; }

  @Override
  public int lastWriteIndex() { return lastWriteIndex; }

  @Override
  public void startWrite() {

    // Special handling for first value. Alloc vector if needed.
    // Offset vectors require a 0 at position 0. The (end) offset
    // for row 0 starts at position 1, which is handled in
    // writeOffset() below.

    nextOffset = 0;
    lastWriteIndex = -1;
    rowStartOffset = 0;
    setAddr(vector.getBuffer());
    if (capacity < ColumnAccessors.MIN_BUFFER_SIZE) {
      vector.reallocRaw(ColumnAccessors.MIN_BUFFER_SIZE * VALUE_WIDTH);
      setAddr(vector.getBuffer());
    }
    PlatformDependent.putInt(bufAddr, nextOffset);
  }

  private final void setAddr(final DrillBuf buf) {
    bufAddr = buf.addr();
    capacity = buf.capacity() / VALUE_WIDTH;
  }

  public int nextOffset() { return nextOffset; }
  public int rowStartOffset() { return rowStartOffset; }

  @Override
  public ValueType valueType() { return ValueType.INTEGER; }

  @Override
  public void startRow() { rowStartOffset = nextOffset; }

  /**
   * Return the write offset, which is one greater than the index reported
   * by the vector index.
   *
   * @return the offset in which to write the current offset of the end
   * of the current data value
   */

  protected final int writeIndex() {
    final int valueIndex = vectorIndex.vectorIndex();
    final int writeIndex = valueIndex + 1;
    if (lastWriteIndex + 1 < valueIndex || writeIndex >= capacity) {
      prepareWrite(writeIndex);
    }
    return writeIndex;
  }

  /**
   * Prepare a write in which either the next write index is past the
   * end of the buffer (and so the buffer must be resized), empties must
   * be filled, or both. For an offset vector, empties are filled with
   * the next offset value.
   *
   * @param writeIndex target write index. This is the position we wish
   * to write, which is one greater than the current position given by
   * the vector index
   */

  private final void prepareWrite(int writeIndex) {
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
    while (lastWriteIndex < writeIndex - 2) {
      PlatformDependent.putInt(bufAddr + (++lastWriteIndex + 1) * VALUE_WIDTH, nextOffset);
    }
  }

  public final void setNextOffset(final int newOffset) {
    final int writeIndex = writeIndex();
    PlatformDependent.putInt(bufAddr + writeIndex * VALUE_WIDTH, newOffset);
    nextOffset = newOffset;
    lastWriteIndex = writeIndex - 1;
  }

  @Override
  public void skipNulls() {

    // Nothing to do. Fill empties logic will fill in missing
    // offsets.
  }

  @Override
  public void restartRow() {
    nextOffset = rowStartOffset;
    lastWriteIndex = Math.min(lastWriteIndex, vectorIndex.vectorIndex() - 1);
  }

  @Override
  public void preRollover() {
    final int valueCount = vectorIndex.rowStartIndex() + 1;
    prepareWrite(valueCount);
    vector.getBuffer().writerIndex(valueCount * VALUE_WIDTH);
  }

  @Override
  public void postRollover() {
    final int newNext = nextOffset - rowStartOffset;
    final int newLastWriteIndex = Math.max(lastWriteIndex - vectorIndex.rowStartIndex(), -1);
    startWrite();
    nextOffset = newNext;
    lastWriteIndex = newLastWriteIndex;
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
      .attribute("lastWriteIndex", lastWriteIndex)
      .attribute("nextOffset", nextOffset)
      .endObject();
  }
}
