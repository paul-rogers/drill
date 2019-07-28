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
package org.apache.drill.exec.vector;

import java.util.Collections;
import java.util.Iterator;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import io.netty.buffer.DrillBuf;

public abstract class BaseValueVector implements ValueVector {

  /**
   * Physical maximum allocation. This is the value prior to Drill 1.11.
   * This size causes memory fragmentation. Please use
   * {@link ValueVector#MAX_BUFFER_SIZE} in new code.
   */

  @Deprecated
  public static final int MAX_ALLOCATION_SIZE = Integer.MAX_VALUE;
  public static final int INITIAL_VALUE_ALLOCATION = 4096;

  protected final BufferAllocator allocator;
  protected final MaterializedField field;

  protected BaseValueVector(MaterializedField field, BufferAllocator allocator) {
    this.field = Preconditions.checkNotNull(field, "field cannot be null");
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
  }

  @Override
  public String toString() {
    return super.toString() + "[field = " + field + ", ...]";
  }

  @Override
  public void clear() {
    getMutator().reset();
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  public MaterializedField getField(String ref) {
    return getField().withPath(ref);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(getField().getName(), allocator);
  }

  @Override
  public SerializedField getMetadata() {
    return getMetadataBuilder().build();
  }

  protected SerializedField.Builder getMetadataBuilder() {
    return getField().getAsBuilder()
        .setValueCount(getAccessor().getValueCount())
        .setBufferLength(getBufferSize());
  }

  public abstract static class BaseAccessor implements ValueVector.Accessor {
    protected BaseAccessor() { }

    @Override
    public boolean isNull(int index) {
      return false;
    }
  }

  public abstract static class BaseMutator implements ValueVector.Mutator {
    protected BaseMutator() { }

    @Override
    public void generateTestData(int values) {}

    //TODO: consider making mutator stateless(if possible) on another issue.
    @Override
    public void reset() {}

    // TODO: If mutator becomes stateless, remove this method.
    @Override
    public void exchange(ValueVector.Mutator other) { }
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.emptyIterator();
  }

  public static boolean checkBufRefs(final ValueVector vv) {
    for(final DrillBuf buffer : vv.getBuffers(false)) {
      if (buffer.refCnt() <= 0) {
        throw new IllegalStateException("zero refcount");
      }
    }

    return true;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  public static void fillBitsVector(UInt1Vector bits, int valueCount) {

    // Create a new bits vector, all values non-null

    bits.allocateNew(valueCount);
    UInt1Vector.Mutator bitsMutator = bits.getMutator();
    for (int i = 0; i < valueCount; i++) {
      bitsMutator.set(i, 1);
    }
    bitsMutator.setValueCount(valueCount);
  }

  @Override
  public void toNullable(ValueVector nullableVector) {
    throw new UnsupportedOperationException();
  }

  /**
   * Backfill missing offsets from the given last written position to the
   * given current write position. Used by the "new" size-safe column
   * writers to allow skipping values. The <tt>set()</tt> and <tt>setSafe()</tt>
   * <b>do not</b> fill empties. See DRILL-5529.
   * @param lastWrite the position of the last valid write: the offset
   * to be copied forward
   * @param index the current write position filling occurs up to,
   * but not including, this position
   */

  public static void fillEmptyOffsets(UInt4Vector offsetVector, int lastWrite, int index) {
    // If last write was 2, offsets are [0, 3, 6]
    // If next write is 4, offsets must be: [0, 3, 6, 6, 6]
    // Remember the offsets are one more than row count.

    if (index <= lastWrite) {
      return;
    }
    final int fillOffset = offsetVector.getAccessor().get(lastWrite+1);
    final UInt4Vector.Mutator offsetMutator = offsetVector.getMutator();
    for (int i = lastWrite + 1; i < index; i++) {
      offsetMutator.setSafe(i + 1, fillOffset);
    }
  }
}

