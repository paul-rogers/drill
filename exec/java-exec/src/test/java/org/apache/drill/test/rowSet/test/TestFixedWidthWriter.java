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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessors.IntColumnWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

/**
 * Test the int writer as a typical example of a fixed-width
 * writer. Exercises normal writing, writing after a (simulated)
 * overflow, and filling in empty values.
 */

public class TestFixedWidthWriter extends SubOperatorTest {

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
   * Basic test to write a contiguous set of values, enough to cause
   * the vector to double in size twice, then read back the values.
   */

  @Test
  public void testWrite() {
    MaterializedField field =
        SchemaBuilder.columnSchema("x", MinorType.INT, DataMode.REQUIRED);
    try (IntVector vector = new IntVector(field, fixture.allocator())) {

      // Party on the bytes of the vector so we start dirty

      vector.allocateNew(1000);
      for (int i = 0; i < 900; i++) {
        vector.getMutator().set(i, 0xdeadbeef);
      }
      assertNotEquals(0, vector.getAccessor().get(0));

      TestIndex index = new TestIndex();
      IntColumnWriter writer = new IntColumnWriter(vector);
      writer.bindIndex(index);

      assertEquals(ValueType.INTEGER, writer.valueType());

      writer.startWrite();

      // Write integers.
      // Write enough that the vector is resized.

      long origAddr = vector.getBuffer().addr();
      for (int i = 0; i < 3000; i++) {
        index.index = i;
        writer.setInt(i * 10);
      }
      writer.endWrite();

      // Should have been reallocated.

      assertNotEquals(origAddr, vector.getBuffer().addr());

      // Verify values

      for (int i = 0; i < 300; i++) {
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
        SchemaBuilder.columnSchema("x", MinorType.INT, DataMode.REQUIRED);
    try (IntVector vector = new IntVector(field, fixture.allocator())) {

      TestIndex index = new TestIndex();
      IntColumnWriter writer = new IntColumnWriter(vector);
      writer.bindIndex(index);

      // Party on the bytes of the vector so we start dirty

      vector.allocateNew(1000);
      for (int i = 0; i < 900; i++) {
        vector.getMutator().set(i, 0xdeadbeef);
      }

      // Simulate doing an overflow of three values.

      vector.getMutator().set(0, 0);
      vector.getMutator().set(1, 10);
      vector.getMutator().set(2, 20);

      // Start write at after position 2.

      writer.startWriteAt(2);

      // Simulate resuming with a few more values.

      for (int i = 3; i < 10; i++) {
        index.index = i;
        writer.setInt(i * 10);
      }
      writer.endWrite();

      // Verify the results

      for (int i = 0; i < 10; i++) {
        assertEquals(i * 10, vector.getAccessor().get(i));
      }
    }
  }

  /**
   * Required, fixed-width vectors are back-filling with 0 to fill in missing
   * values. While using zero is not strictly SQL compliant, it is better
   * than failing. (The SQL solution would be to fill with nulls, but a
   * required vector does not support nulls...)
   */

  @Test
  public void testFillEmpties() {
    MaterializedField field =
        SchemaBuilder.columnSchema("x", MinorType.INT, DataMode.REQUIRED);
    try (IntVector vector = new IntVector(field, fixture.allocator())) {

      // Party on the bytes of the vector so we start dirty

      vector.allocateNew(1000);
      for (int i = 0; i < 900; i++) {
        vector.getMutator().set(i, 0xdeadbeef);
      }

      TestIndex index = new TestIndex();
      IntColumnWriter writer = new IntColumnWriter(vector);
      writer.bindIndex(index);
      writer.startWrite();

      // Write values, skipping four out of five positions,
      // forcing backfill.
      // The loop will cause the vector to double in size.
      // The number of values is odd, forcing the writer to
      // back-fill at the end as well as between values.

      long origAddr = vector.getBuffer().addr();
      for (int i = 5; i < 3001; i += 5) {
        index.index = i;
        writer.setInt(i * 10);
      }
      index.index = 3003;
      writer.endWrite();

      // Should have been reallocated.

      assertNotEquals(origAddr, vector.getBuffer().addr());

      // Verify values

      for (int i = 0; i < 3004; i++) {
        assertEquals(
            (i%5) == 0 ? i * 10 : 0,
            vector.getAccessor().get(i));
      }
    }
  }
}
