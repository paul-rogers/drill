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

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.TupleLoader.UndefinedColumnException;
import org.apache.drill.exec.physical.rowSet.impl.RowSetMutatorImpl;
import org.apache.drill.exec.physical.rowSet.impl.RowSetMutatorImpl.MutatorOptions;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

public class RowSetMutatorTest extends SubOperatorTest {

  public static boolean hasAssertions;

  @BeforeClass
  public static void initSetup() {
    hasAssertions = false;
    assert hasAssertions = true;
    if (! hasAssertions) {
      System.err.println("This test requires assertions, but they are not enabled - some tests skipped.");
    }
  }

  private ArrayLoader arrayWriter;

  @Test
  public void testBasics() {
    RowSetMutator rsMutator = new RowSetMutatorImpl(fixture.allocator());
    assertEquals(0, rsMutator.schemaVersion());
    assertEquals(ValueVector.MAX_ROW_COUNT, rsMutator.targetRowCount());
    assertEquals(ValueVector.MAX_BUFFER_SIZE, rsMutator.targetVectorSize());
    assertEquals(0, rsMutator.rowCount());
    assertEquals(0, rsMutator.batchCount());
    assertEquals(0, rsMutator.totalRowCount());

    // Failures due to wrong state (Start)

    try {
      rsMutator.harvest();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.saveRow();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Can define schema before starting the first batch.

    TupleLoader rootWriter = rsMutator.writer();
    TupleSchema schema = rootWriter.schema();
    assertEquals(0, schema.columnCount());

    MaterializedField fieldA = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    schema.addColumn(fieldA);

    assertEquals(1, rsMutator.schemaVersion());
    assertEquals(1, schema.columnCount());
    assertSame(fieldA, schema.column(0));
    assertSame(fieldA, schema.column("a"));

    // Error to write to a column before the first batch.

    try {
      rsMutator.startRow();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    if (hasAssertions) {
      try {
        rootWriter.column(0).setInt(50);
        fail();
      } catch (AssertionError e) {
        // Expected
      }
    }

    rsMutator.startBatch();
    try {
      rsMutator.startBatch();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    assertFalse(rsMutator.isFull());

    rootWriter.column(0).setInt(100);
    assertEquals(0, rsMutator.rowCount());
    assertEquals(0, rsMutator.batchCount());
    rsMutator.saveRow();
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
    rsMutator.saveRow();
    assertEquals(2, rsMutator.rowCount());
    assertEquals(1, rsMutator.batchCount());
    assertEquals(2, rsMutator.totalRowCount());

    assertFalse(rsMutator.isFull());
    RowSet result = fixture.wrap(rsMutator.harvest());
    assertEquals(0, rsMutator.rowCount());
    assertEquals(1, rsMutator.batchCount());
    assertEquals(2, rsMutator.totalRowCount());

    SingleRowSet expected = fixture.rowSetBuilder(result.batchSchema())
        .add(100, null)
        .add(200, 210)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(result);

    // Between batches: batch-based operations fail

    try {
      rsMutator.startRow();
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
      rsMutator.saveRow();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Create a second batch

    rsMutator.startBatch();
    assertEquals(0, rsMutator.rowCount());
    assertEquals(1, rsMutator.batchCount());
    assertEquals(2, rsMutator.totalRowCount());
    rootWriter.column(0).setInt(300);
    rootWriter.column(1).setInt(310);
    rsMutator.saveRow();
    assertEquals(1, rsMutator.rowCount());
    assertEquals(2, rsMutator.batchCount());
    assertEquals(3, rsMutator.totalRowCount());
    rootWriter.column(0).setInt(400);
    rootWriter.column(1).setInt(410);
    rsMutator.saveRow();

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
        .verifyAndClearAll(result);

    // Next batch. Schema has changed.

    rsMutator.startBatch();
    rootWriter.column(0).setInt(500);
    rootWriter.column(1).setInt(510);
    rootWriter.schema().addColumn(SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.OPTIONAL));
    rootWriter.column(2).setInt(520);
    rsMutator.saveRow();
    rootWriter.column(0).setInt(600);
    rootWriter.column(1).setInt(610);
    rootWriter.column(2).setInt(620);
    rsMutator.saveRow();

    result = fixture.wrap(rsMutator.harvest());
    assertEquals(3, rsMutator.schemaVersion());
    expected = fixture.rowSetBuilder(result.batchSchema())
        .add(500, 510, 520)
        .add(600, 610, 620)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(result);

    rsMutator.close();

    // Key operations fail after close.

    try {
      rsMutator.startRow();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.writer();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.startBatch();
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
      rsMutator.saveRow();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Benign to close twice

    rsMutator.close();
  }

  /**
   * Schemas are case insensitive by default. Verify that
   * the schema mechanism works, with emphasis on the
   * case insensitive case.
   */

  @Test
  public void testCaseInsensitiveSchema() {
    RowSetMutator rsMutator = new RowSetMutatorImpl(fixture.allocator());
    TupleLoader rootWriter = rsMutator.writer();
    TupleSchema schema = rootWriter.schema();

    // No columns defined in schema

    assertNull(schema.column("a"));
    try {
      schema.column(0);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }

    // No columns defined in writer

    try {
      rootWriter.column("a");
      fail();
    } catch (UndefinedColumnException e) {
      // Expected
    }
    try {
      rootWriter.column(0);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }

    // Define a column

    MaterializedField colSchema = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
    schema.addColumn(colSchema);

    // Can now be found, case insensitive

    assertSame(colSchema, schema.column(0));
    assertSame(colSchema, schema.column("a"));
    assertSame(colSchema, schema.column("A"));
    assertNotNull(rootWriter.column(0));
    assertNotNull(rootWriter.column("a"));
    assertNotNull(rootWriter.column("A"));
    assertEquals(1, schema.columnCount());
    assertEquals(0, schema.columnIndex("a"));
    assertEquals(0, schema.columnIndex("A"));

    // Reject a duplicate name, case insensitive

    try {
      schema.addColumn(colSchema);
      fail();
    } catch(IllegalArgumentException e) {
      // Expected
    }
    try {
      MaterializedField testCol = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
      schema.addColumn(testCol);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains("Duplicate"));
    }

    // Can still add required fields while writing the first row.

    rsMutator.startBatch();
    rsMutator.startRow();
    rootWriter.column(0).setString("foo");

    MaterializedField col2 = SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED);
    schema.addColumn(col2);
    assertSame(col2, schema.column(1));
    assertSame(col2, schema.column("b"));
    assertSame(col2, schema.column("B"));
    assertEquals(2, schema.columnCount());
    assertEquals(1, schema.columnIndex("b"));
    assertEquals(1, schema.columnIndex("B"));
    rootWriter.column(1).setString("second");

    // After first row, a required type is not allowed, must be optional or repeated.

    rsMutator.saveRow();
    rootWriter.column(0).setString("bar");
    rootWriter.column(1).setString("");

    try {
      MaterializedField testCol = SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.REQUIRED);
      schema.addColumn(testCol);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains("Cannot add a required field"));
    }

    MaterializedField col3 = SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.OPTIONAL);
    schema.addColumn(col3);
    assertSame(col3, schema.column(2));
    assertSame(col3, schema.column("c"));
    assertSame(col3, schema.column("C"));
    assertEquals(3, schema.columnCount());
    assertEquals(2, schema.columnIndex("c"));
    assertEquals(2, schema.columnIndex("C"));
    rootWriter.column("c").setString("c.2");

    MaterializedField col4 = SchemaBuilder.columnSchema("d", MinorType.VARCHAR, DataMode.REPEATED);
    schema.addColumn(col4);
    assertSame(col4, schema.column(3));
    assertSame(col4, schema.column("d"));
    assertSame(col4, schema.column("D"));
    assertEquals(4, schema.columnCount());
    assertEquals(3, schema.columnIndex("d"));
    assertEquals(3, schema.columnIndex("D"));

    // Handy way to test that close works to abort an in-flight batch
    // and clean up.

    rsMutator.close();
  }

  /**
   * Simplified schema test to verify the case-sensitive schema
   * option. Note that case-sensitivity is supported in this writer,
   * but nowhere else in Drill. Still, JSON is case-sensitive and
   * we have to start somewhere...
   */

  @Test
  public void testCaseSensitiveSchema() {
    MutatorOptions options = new RowSetMutatorImpl.OptionBuilder()
        .setCaseSensitive(true)
        .build();
    RowSetMutator rsMutator = new RowSetMutatorImpl(fixture.allocator(), options);
    TupleLoader rootWriter = rsMutator.writer();
    TupleSchema schema = rootWriter.schema();

    MaterializedField col1 = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
    schema.addColumn(col1);

    MaterializedField col2 = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
    schema.addColumn(col2);

    assertSame(col1, schema.column(0));
    assertSame(col1, schema.column("a"));
    assertSame(col2, schema.column(1));
    assertSame(col2, schema.column("A"));
    assertEquals(2, schema.columnCount());
    assertEquals(0, schema.columnIndex("a"));
    assertEquals(1, schema.columnIndex("A"));

    rsMutator.startBatch();
    rootWriter.column(0).setString("lower");
    rootWriter.column(1).setString("upper");

    // We'd like to verify the values, but even the row set
    // abstraction is case in-sensitive, so this is as far as
    // we can go.

    // TODO: Validate the values when the row set tools support
    // case-insensitivity.

    rsMutator.close();
  }

  /**
   * Verify that the writer stops when reaching the row limit.
   * In this case there is no look-ahead row.
   */

  @Test
  public void testRowLimit() {
    RowSetMutator rsMutator = new RowSetMutatorImpl(fixture.allocator());
    TupleLoader rootWriter = rsMutator.writer();
    TupleSchema schema = rootWriter.schema();
    schema.addColumn(SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REQUIRED));

    byte value[] = new byte[200];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsMutator.startBatch();
    for (; ! rsMutator.isFull(); count++) {
      rsMutator.startRow();
      rootWriter.column(0).setBytes(value);
      rsMutator.saveRow();
    }
    assertEquals(ValueVector.MAX_ROW_COUNT, count);
    assertEquals(count, rsMutator.rowCount());

    rsMutator.harvest().clear();
    rsMutator.startBatch();
    assertEquals(0, rsMutator.rowCount());

    rsMutator.close();
  }

  private static final int TEST_ROW_LIMIT = 1024;

  /**
   * Verify that the caller can set a row limit lower than the default.
   */

  @Test
  public void testCustomRowLimit() {

    // Try to set a default value larger than the hard limit. Value
    // is truncated to the limit.

    MutatorOptions options = new RowSetMutatorImpl.OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT + 1)
        .build();
    assertEquals(ValueVector.MAX_ROW_COUNT, options.rowCountLimit);

    // Just a bit of paranoia that we check against the vector limit,
    // not any previous value...

    options = new RowSetMutatorImpl.OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT + 1)
        .setRowCountLimit(TEST_ROW_LIMIT)
        .build();
    assertEquals(TEST_ROW_LIMIT, options.rowCountLimit);

    options = new RowSetMutatorImpl.OptionBuilder()
        .setRowCountLimit(TEST_ROW_LIMIT)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT + 1)
        .build();
    assertEquals(ValueVector.MAX_ROW_COUNT, options.rowCountLimit);

    // Do load with a (valid) limit lower than the default.

    options = new RowSetMutatorImpl.OptionBuilder()
        .setRowCountLimit(TEST_ROW_LIMIT)
        .build();
    RowSetMutator rsMutator = new RowSetMutatorImpl(fixture.allocator(), options);
    TupleLoader rootWriter = rsMutator.writer();
    TupleSchema schema = rootWriter.schema();
    schema.addColumn(SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REQUIRED));

    byte value[] = new byte[200];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsMutator.startBatch();
    for (; ! rsMutator.isFull(); count++) {
      rsMutator.startRow();
      rootWriter.column(0).setBytes(value);
      rsMutator.saveRow();
    }
    assertEquals(TEST_ROW_LIMIT, count);
    assertEquals(count, rsMutator.rowCount());

    // Should fail to write beyond the row limit

    try {
      rsMutator.startRow();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsMutator.saveRow();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    rsMutator.harvest().clear();
    rsMutator.startBatch();
    assertEquals(0, rsMutator.rowCount());

    rsMutator.close();
  }

  /**
   * Test that the writer detects a vector overflow. The offending column
   * value should be moved to the next batch.
   */

  @Test
  public void testSizeLimit() {
    RowSetMutator rsMutator = new RowSetMutatorImpl(fixture.allocator());
    TupleLoader rootWriter = rsMutator.writer();
    TupleSchema schema = rootWriter.schema();
    MaterializedField field = SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REQUIRED);
    schema.addColumn(field);

    rsMutator.startBatch();
    byte value[] = new byte[512];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    for (; ! rsMutator.isFull(); count++) {
      rsMutator.startRow();
      rootWriter.column(0).setBytes(value);
      rsMutator.saveRow();
    }

    // Row count should include the overflow row

    int expectedCount = ValueVector.MAX_BUFFER_SIZE / value.length;
    assertEquals(expectedCount + 1, count);
    assertEquals(expectedCount + 1, rsMutator.rowCount());
    assertEquals(expectedCount + 1, rsMutator.totalRowCount());

    // Result should exclude the overflow row

    RowSet result = fixture.wrap(rsMutator.harvest());
    assertEquals(expectedCount, result.rowCount());
    result.clear();

    // Next batch should start with the overflow row

    rsMutator.startBatch();
    assertEquals(1, rsMutator.rowCount());
    assertEquals(expectedCount + 1, rsMutator.totalRowCount());
    result = fixture.wrap(rsMutator.harvest());
    assertEquals(1, result.rowCount());
    result.clear();

    rsMutator.close();
  }

  /**
   * Test the case where the schema changes in the first batch.
   * Schema changes before the first record are trivial and tested
   * elsewhere. Here we write some records, then add new columns, as a
   * JSON reader might do.
   */

  @Test
  public void testCaseSchemaChangeFirstBatch() {
    RowSetMutator rsMutator = new RowSetMutatorImpl(fixture.allocator());
    TupleLoader rootWriter = rsMutator.writer();
    TupleSchema schema = rootWriter.schema();
    schema.addColumn(SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED));

    // Create initial rows

    rsMutator.startBatch();
    int rowCount = 0;
    for (int i = 0; i < 2;  i++) {
      rsMutator.startRow();
      rowCount++;
      rootWriter.column(0).setString("a_" + rowCount);
      rsMutator.saveRow();
    }

    // Add a second column. Must be nullable.

    schema.addColumn(SchemaBuilder.columnSchema("b", MinorType.INT, DataMode.OPTIONAL));
    for (int i = 0; i < 2;  i++) {
      rsMutator.startRow();
      rowCount++;
      rootWriter.column(0).setString("a_" + rowCount);
      rootWriter.column(1).setInt(rowCount);
      rsMutator.saveRow();
    }

    // Add a third column. Must be nullable. Use variable-width so that offset
    // vectors must be back-filled.

    schema.addColumn(SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.OPTIONAL));
    for (int i = 0; i < 2;  i++) {
      rsMutator.startRow();
      rowCount++;
      rootWriter.column(0).setString("a_" + rowCount);
      rootWriter.column(1).setInt(rowCount);
      rootWriter.column(2).setString("c_" + rowCount);
      rsMutator.saveRow();
    }

    // Add an array. Now two offset vectors must be back-filled.

    schema.addColumn(SchemaBuilder.columnSchema("d", MinorType.VARCHAR, DataMode.REPEATED));
    for (int i = 0; i < 2;  i++) {
      rsMutator.startRow();
      rowCount++;
      rootWriter.column(0).setString("a_" + rowCount);
      rootWriter.column(1).setInt(rowCount);
      rootWriter.column(2).setString("c_" + rowCount);
      arrayWriter = rootWriter.column(3).array();
      arrayWriter.setString("d_" + rowCount + "-1");
      arrayWriter.setString("d_" + rowCount + "-2");
      rsMutator.saveRow();
    }

    // Harvest the row and verify.

    RowSet actual = fixture.wrap(rsMutator.harvest());

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("b", MinorType.INT)
        .addNullable("c", MinorType.VARCHAR)
        .addArray("d", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add("a_1", null, null,  null)
        .add("a_2", null, null,  null)
        .add("a_3", 3,    null,  null)
        .add("a_4", 4,    null,  null)
        .add("a_5", 5,    "c_5", null)
        .add("a_6", 6,    "c_6", null)
        .add("a_7", 7,    "c_7", new String[] {"d_7-1", "d_7-2"})
        .add("a_8", 8,    "c_8", new String[] {"d_8-1", "d_8-2"})
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
    rsMutator.close();
  }

  // TODO: Test vector limits with repeated types

  // TODO: Test single array that exceeds vector limit

  // TODO: Test empty slots (unset values) at end of batch
  // (should fill in nulls)

  // TODO: Schema change during overflow row

  // TODO: Schema change after overflow batch created

  // TODO: Test initial vector allocation

  // TODO: Test schema change flag across batches

}
