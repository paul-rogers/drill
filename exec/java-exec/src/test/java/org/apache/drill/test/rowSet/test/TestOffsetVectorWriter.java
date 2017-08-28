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
package org.apache.drill.test.rowSet.test;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.writer.OffsetVectorWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

/**
 * The offset vector writer is unique: it follows the same API as
 * the other writers, but has a unique twist because offsets are written
 * into the slot one after the other vectors. That is, if we are writing
 * row 5, the offset vector writer writes to position 6. This is done to
 * write the end offset of row 5 as the start offset of row 6. (It does,
 * however, waste space as we need twice the number of elements in the
 * offset vector as other vectors when writing power-of-two record
 * counts.)
 */

public class TestOffsetVectorWriter extends SubOperatorTest {

  public static class TestIndex implements ColumnWriterIndex {

    public int index;

    @Override
    public int vectorIndex() { return index; }

    @Override
    public void nextElement() { }

    @Override
    public void resetTo(int newIndex) { }
  }

  /**
   * Basic test to write a contiguous set of offsets, enough to cause
   * the vector to double in size twice, then read back the values.
   */

  @Test
  public void testWrite() {
    MaterializedField field =
        SchemaBuilder.columnSchema("x", MinorType.UINT4, DataMode.REQUIRED);
    try (UInt4Vector vector = new UInt4Vector(field, fixture.allocator())) {

      // Party on the bytes of the vector so we start dirty

      vector.allocateNew(1000);
      for (int i = 0; i < 900; i++) {
        vector.getMutator().set(i, 0xdeadbeef);
      }
      assertNotEquals(0, vector.getAccessor().get(0));

      TestIndex index = new TestIndex();
      OffsetVectorWriter writer = new OffsetVectorWriter(vector);
      writer.bindIndex(index);

      assertEquals(ValueType.INTEGER, writer.valueType());

      // Start write sets initial position to 0.

      writer.startWrite();
      assertEquals(0, vector.getAccessor().get(0));

      // Pretend to write offsets for values of width 10. We write
      // the end position of each field.
      // Write enough that the vector is resized.

      long origAddr = vector.getBuffer().addr();
      for (int i = 0; i < 3000; i++) {
        index.index = i;
        int startOffset = writer.currentStartOffset();
        assertEquals(i * 10, startOffset);
        writer.setOffset(startOffset + 10);
      }
      writer.endWrite();

      // Should have been reallocated.

      assertNotEquals(origAddr, vector.getBuffer().addr());

      // Verify values

      for (int i = 0; i < 3001; i++) {
        assertEquals(i * 10, vector.getAccessor().get(i));
      }
    }
  }

  /**
   * The <tt>startWriteAt</tt> method is used during vector overflow to
   * specify the position at which to start writing. This allows skipping
   * over values copied from the overflowed vector.
   */

  @Test
  public void testStartWriteAt() {
    MaterializedField field =
        SchemaBuilder.columnSchema("x", MinorType.UINT4, DataMode.REQUIRED);
    try (UInt4Vector vector = new UInt4Vector(field, fixture.allocator())) {

      TestIndex index = new TestIndex();
      OffsetVectorWriter writer = new OffsetVectorWriter(vector);
      writer.bindIndex(index);

      // Party on the bytes of the vector so we start dirty

      vector.allocateNew(1000);
      for (int i = 0; i < 900; i++) {
        vector.getMutator().set(i, 0xdeadbeef);
      }

      // Simulate doing an overflow of three values.

      vector.getMutator().set(1, 10);
      vector.getMutator().set(2, 20);
      vector.getMutator().set(3, 30);

      // Start write. This will fill in position 0.

      writer.startWriteAt(2);
      assertEquals(30, writer.currentStartOffset());

      // Simulate resuming with a few more values.

      for (int i = 3; i < 10; i++) {
        index.index = i;
        writer.setOffset((i + 1) * 10);
      }
      writer.endWrite();

      // Verify the results

      for (int i = 0; i < 11; i++) {
        assertEquals(i * 10, vector.getAccessor().get(i));
      }
    }
  }

  /**
   * Offset vectors have specific behavior when back-filling missing values:
   * the last offset must be carried forward into the missing slots. The
   * slots cannot be zero-filled, or entries will end up with a negative
   * length.
   */

  @Test
  public void testFillEmpties() {
    MaterializedField field =
        SchemaBuilder.columnSchema("x", MinorType.UINT4, DataMode.REQUIRED);
    try (UInt4Vector vector = new UInt4Vector(field, fixture.allocator())) {

      // Party on the bytes of the vector so we start dirty

      vector.allocateNew(1000);
      for (int i = 0; i < 900; i++) {
        vector.getMutator().set(i, 0xdeadbeef);
      }

      TestIndex index = new TestIndex();
      OffsetVectorWriter writer = new OffsetVectorWriter(vector);
      writer.bindIndex(index);

      // Start write sets initial position to 0.

      writer.startWrite();
      assertEquals(0, vector.getAccessor().get(0));

      // Pretend to write offsets for values of width 10, but
      // skip four out of five values, forcing backfill.
      // The loop will cause the vector to double in size.
      // The number of values is odd, forcing the writer to
      // back-fill at the end as well as between values.

      long origAddr = vector.getBuffer().addr();
      for (int i = 5; i < 3001; i += 5) {
        index.index = i;
        int startOffset = writer.currentStartOffset();
        assertEquals((i/5 - 1) * 10, startOffset);
        writer.setOffset(startOffset + 10);
      }
      index.index = 3003;
      writer.endWrite();

      // Should have been reallocated.

      assertNotEquals(origAddr, vector.getBuffer().addr());

      // Verify values

      for (int i = 0; i < 3004; i++) {
        assertEquals(((i-1)/5) * 10, vector.getAccessor().get(i));
      }
    }
  }
}
