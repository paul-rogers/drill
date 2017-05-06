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
package org.apache.drill.exec.physical.rowSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.impl.RowSetMutatorImpl;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class LoaderTest extends SubOperatorTest {

  @Test
  public void testBasics() {
    RowSetMutator rsMutator = new RowSetMutatorImpl(fixture.allocator());
    assertEquals(0, rsMutator.schemaVersion());
    assertEquals(ValueVector.MAX_ROW_COUNT, rsMutator.targetRowCount());
    assertEquals(ValueVector.MAX_BUFFER_SIZE, rsMutator.targetVectorSize());
    assertFalse(rsMutator.isFull());
    assertEquals(0, rsMutator.rowCount());
    assertEquals(0, rsMutator.batchCount());
    assertEquals(0, rsMutator.totalRowCount());

    // Failures due to wrong state (Start)

    try {
      rsMutator.writer();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.harvest();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.save();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    rsMutator.start();
    try {
      rsMutator.start();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    assertFalse(rsMutator.isFull());
    TupleLoader rootWriter = rsMutator.writer();
    TupleSchema schema = rootWriter.schema();
    assertEquals(0, schema.columnCount());

    MaterializedField fieldA = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    schema.addColumn(fieldA);

    assertEquals(1, rsMutator.schemaVersion());
    assertEquals(1, schema.columnCount());
    assertSame(fieldA, schema.column(0));
    assertSame(fieldA, schema.column("a"));

    rootWriter.column(0).setInt(100);
    assertEquals(0, rsMutator.rowCount());
    assertEquals(0, rsMutator.batchCount());
    rsMutator.save();
    assertEquals(1, rsMutator.rowCount());
    assertEquals(1, rsMutator.batchCount());
    assertEquals(1, rsMutator.totalRowCount());

    MaterializedField fieldB = SchemaBuilder.columnSchema("b", MinorType.INT, DataMode.OPTIONAL);
    schema.addColumn(fieldB);

    assertEquals(2, rsMutator.schemaVersion());
    assertEquals(2, schema.columnCount());
    assertSame(fieldB, schema.column(1));
    assertSame(fieldB, schema.column("b"));

    rootWriter.column(0).setInt(200);
    rootWriter.column(1).setInt(210);
    rsMutator.save();
    assertEquals(2, rsMutator.rowCount());
    assertEquals(1, rsMutator.batchCount());
    assertEquals(2, rsMutator.totalRowCount());

    assertFalse(rsMutator.isFull());
    RowSet result = fixture.wrap(rsMutator.harvest());
    assertEquals(0, rsMutator.rowCount());
    assertEquals(1, rsMutator.batchCount());
    assertEquals(2, rsMutator.totalRowCount());
    assertFalse(rsMutator.isFull());

    SingleRowSet expected = fixture.rowSetBuilder(result.batchSchema())
        .add(100, null)
        .add(200, 210)
        .build();
    new RowSetComparison(expected)
        .verifyAndClear(result);

    // Between batches: batch-based operations fail

    try {
      rsMutator.writer();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.harvest();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.save();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Create a second batch

    rsMutator.start();
    assertEquals(0, rsMutator.rowCount());
    assertEquals(1, rsMutator.batchCount());
    assertEquals(2, rsMutator.totalRowCount());
    rootWriter.column(0).setInt(300);
    rootWriter.column(1).setInt(310);
    rsMutator.save();
    assertEquals(1, rsMutator.rowCount());
    assertEquals(2, rsMutator.batchCount());
    assertEquals(3, rsMutator.totalRowCount());
    rootWriter.column(0).setInt(400);
    rootWriter.column(1).setInt(410);
    rsMutator.save();

    // Harvest. Schema has not changed.

    result = fixture.wrap(rsMutator.harvest());
    assertEquals(0, rsMutator.rowCount());
    assertEquals(2, rsMutator.batchCount());
    assertEquals(4, rsMutator.totalRowCount());

    expected = fixture.rowSetBuilder(result.batchSchema())
        .add(300, 310)
        .add(400, 410)
        .build();
    new RowSetComparison(expected)
        .verifyAndClear(result);

    // Next batch. Schema has changed.

    rsMutator.start();
    rootWriter.column(0).setInt(500);
    rootWriter.column(1).setInt(510);
    rootWriter.schema().addColumn(SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.OPTIONAL));
    rootWriter.column(2).setInt(520);
    rsMutator.save();
    rootWriter.column(0).setInt(600);
    rootWriter.column(1).setInt(610);
    rootWriter.column(2).setInt(620);
    rsMutator.save();

    result = fixture.wrap(rsMutator.harvest());
    assertEquals(3, rsMutator.schemaVersion());
    expected = fixture.rowSetBuilder(result.batchSchema())
        .add(500, 510, 520)
        .add(600, 610, 620)
        .build();
    new RowSetComparison(expected)
        .verifyAndClear(result);

    rsMutator.close();

    // Key operations fail after close.

    try {
      rsMutator.writer();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.start();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.harvest();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.save();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Benign to close twice

    rsMutator.close();
  }

  // Test case sensitive and case insensitive
  // Test name collisions: case insensitive
  // Test name collisions: case sensitive
  // Test name aliases: "a" and "A".
  // Test adding a non-nullable field to batch with more than 0 rows
  // Test adding non-nullable field to first row of second batch

}
