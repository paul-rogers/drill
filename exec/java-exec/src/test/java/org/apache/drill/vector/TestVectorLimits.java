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
package org.apache.drill.vector;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.bouncycastle.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.DrillBuf;

public class TestVectorLimits extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
  }

  /**
   * Test a vector directly using the vector mutator to ensure
   * that the <tt>setBounded</tt> method works for the maximum
   * row count.
   * <p>
   * This test is a proxy for all the other fixed types, since all
   * share the same code template.
   */

  @Test
  public void testFixedVector() {

    // Create a non-nullable int vector: a typical fixed-size vector

    @SuppressWarnings("resource")
    IntVector vector = new IntVector(makeField(MinorType.INT), fixture.allocator() );

    // Sanity test of generated constants.

    assertTrue( IntVector.MAX_COUNT <= ValueVector.MAX_VALUE_COUNT );
    assertEquals( 4, IntVector.VALUE_WIDTH );
    assertTrue( IntVector.NET_MAX_SIZE <= ValueVector.MAX_BUFFER_SIZE );

    // Allocate a default size, small vector. Forces test of
    // the auto-grow (setSafe()) aspect of setBounded().

    vector.allocateNew( );

    // Write to the vector until it complains. At that point,
    // we should have written up to the static fixed value count
    // (which is computed to stay below the capacity limit.)

    IntVector.Mutator mutator = vector.getMutator();
    for (int i = 0; i < 2 * IntVector.MAX_VALUE_COUNT; i++) {
      if (! mutator.setBounded(i, i)) {
        assertEquals(IntVector.MAX_VALUE_COUNT, i);
        break;
      }
    }

    // The vector should be below the allocation limit. Since this
    // is an int vector, in practice the size will be far below
    // the overall limit (if the limit stays at 16 MB.) But, it should
    // be at the type-specific limit since we filled up the vector.

    assertEquals(IntVector.NET_MAX_SIZE, vector.getBuffer().getActualMemoryConsumed());
    vector.close();
  }

  private MaterializedField makeField(MinorType dataType) {
    MajorType type = MajorType.newBuilder()
        .setMinorType(dataType)
        .setMode(DataMode.REQUIRED)
        .build();

    return MaterializedField.create("foo", type);
  }

  /**
   * Baseline test for a variable-width vector using <tt>setSafe</tt> and
   * loading the vector up to the maximum size. Doing so will cause the vector
   * to have a buffer that exceeds the maximum size, demonstrating the
   * need for <tt>setBounded()</tt>.
   */

  @Test
  public void variableVectorBaseline() {

    // Create a non-nullable VarChar vector: a typical variable-size vector

    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField(MinorType.VARCHAR), fixture.allocator() );
    vector.allocateNew( );

    // A 16 MB value can hold 64K values of up to 256 bytes each.
    // To force a size overflow, write values much larger.
    // Write the maximum number of values which will silently
    // allow the vector to grow beyond the critical size of 16 MB.
    // Doing this in production would lead to memory fragmentation.
    // So, this is what the setBounded() method assures we don't do.

    byte dummyValue[] = new byte[512];
    Arrays.fill(dummyValue, (byte) 'X');
    VarCharVector.Mutator mutator = vector.getMutator();
    for (int i = 0; i < 2 * ValueVector.MAX_VALUE_COUNT; i++) {
      mutator.setSafe(i, dummyValue, 0, dummyValue.length);
    }

    // The vector should be above the allocation limit.
    // This is why code must migrate to the setBounded() call
    // away from the setSafe() call.

    assertTrue(ValueVector.MAX_BUFFER_SIZE < vector.getBuffer().getActualMemoryConsumed());
    vector.close();
  }

  /**
   * Test a vector directly using the vector mutator to ensure
   * that the <tt>setBounded</tt> method works for the maximum
   * vector size.
   */

  @Test
  public void testWideVariableVector() {

    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField(MinorType.VARCHAR), fixture.allocator() );
    vector.allocateNew( );

    // A 16 MB value can hold 64K values of up to 256 bytes each.
    // To force a size overflow, write values much larger.
    // Write to the vector until it complains. At that point,
    // we should have written up to the maximum buffer size.

    byte dummyValue[] = new byte[512];
    Arrays.fill(dummyValue, (byte) 'X');
    VarCharVector.Mutator mutator = vector.getMutator();
    int count = 0;
    for ( ; count < 2 * ValueVector.MAX_VALUE_COUNT; count++) {
      if (! mutator.setBounded(count, dummyValue, 0, dummyValue.length)) {
        break;
      }
    }

    // The vector should be at the allocation limit. If it wasn't, we
    // should have grown it to hold more data. The value count will
    // be below the maximum.

    mutator.setValueCount(count);
    assertEquals(ValueVector.MAX_BUFFER_SIZE, vector.getBuffer().getActualMemoryConsumed());
    assertTrue(count < ValueVector.MAX_VALUE_COUNT);
    vector.close();
  }

  /**
   * Test a vector directly using the vector mutator to ensure
   * that the <tt>setBounded</tt> method works for the maximum
   * value count.
   */

  @Test
  public void testNarrowVariableVector() {

    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField(MinorType.VARCHAR), fixture.allocator() );
    vector.allocateNew( );

    // Write small values that fit into 16 MB. We should stop writing
    // when we reach the value count limit.

    byte dummyValue[] = new byte[254];
    Arrays.fill(dummyValue, (byte) 'X');
    VarCharVector.Mutator mutator = vector.getMutator();
    int count = 0;
    for (; count < 2 * ValueVector.MAX_VALUE_COUNT; count++) {
      if (! mutator.setBounded(count, dummyValue, 0, dummyValue.length)) {
        break;
      }
    }

    // Buffer size should be at or below the maximum, with count
    // at the maximum.

    mutator.setValueCount(count);
    assertTrue(vector.getBuffer().getActualMemoryConsumed() <= ValueVector.MAX_BUFFER_SIZE);
    assertEquals(ValueVector.MAX_VALUE_COUNT, count);
    vector.close();
  }

  /**
   * Test a vector directly using the vector mutator to ensure
   * that the <tt>setBounded</tt> method works for the maximum
   * value count. Uses a DrillBuf as input.
   */

  @Test
  public void testDirectVariableVector() {

    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField(MinorType.VARCHAR), fixture.allocator() );
    vector.allocateNew( );

    // Repeat the big-value test, but with data coming from a DrillBuf
    // (direct memory) rather than a heap array.

    byte dummyValue[] = new byte[260];
    Arrays.fill(dummyValue, (byte) 'X');
    @SuppressWarnings("resource")
    DrillBuf drillBuf = fixture.allocator().buffer(dummyValue.length);
    drillBuf.setBytes(0, dummyValue);
    VarCharVector.Mutator mutator = vector.getMutator();
    int count = 0;
    for (; count < 2 * ValueVector.MAX_VALUE_COUNT; count++) {
      if (! mutator.setBounded(count, drillBuf, 0, dummyValue.length)) {
        break;
      }
    }
    drillBuf.close();

    // Again, vector should be at the size limit, count below the
    // value limit.

    mutator.setValueCount(count);
    assertEquals(ValueVector.MAX_BUFFER_SIZE, vector.getBuffer().getActualMemoryConsumed());
    assertTrue(count < ValueVector.MAX_VALUE_COUNT);
    vector.close();
  }
}
