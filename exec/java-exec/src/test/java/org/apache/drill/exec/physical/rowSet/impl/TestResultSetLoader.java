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
package org.apache.drill.exec.physical.rowSet.impl;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter.UndefinedColumnException;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class TestResultSetLoader extends SubOperatorTest {

  public static boolean hasAssertions;

  @BeforeClass
  public static void initSetup() {
    hasAssertions = false;
    assert hasAssertions = true;
    if (! hasAssertions) {
      System.err.println("This test requires assertions, but they are not enabled - some tests skipped.");
    }
  }

  @Test
  public void testBasics() {
    ResultSetLoaderImpl rsLoaderImpl = new ResultSetLoaderImpl(fixture.allocator());
    ResultSetLoader rsLoader = rsLoaderImpl;
    assertEquals(0, rsLoader.schemaVersion());
    assertEquals(ValueVector.MAX_ROW_COUNT, rsLoader.targetRowCount());
    assertEquals(ValueVector.MAX_BUFFER_SIZE, rsLoader.targetVectorSize());
    assertEquals(0, rsLoader.writer().rowCount());
    assertEquals(0, rsLoader.batchCount());
    assertEquals(0, rsLoader.totalRowCount());

    // Verify internal state

    SingleRowSetModel rootModel = rsLoaderImpl.rootModel();
    assertNotNull(rootModel);
    assertEquals(0, rootModel.size());
    assertSame(rsLoader.writer(), rootModel.writer());
    assertNotNull(rootModel.coordinator());
    assertSame(rootModel.schema(), rsLoader.writer().schema());

    // Failures due to wrong state (Start)

    try {
      rsLoader.harvest();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Can define schema before starting the first batch.

    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();
    assertEquals(0, schema.size());

    MaterializedField fieldA = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    rootWriter.addColumn(fieldA);

    assertEquals(1, schema.size());
    assertSame(fieldA, schema.column(0));
    assertSame(fieldA, schema.column("a"));

    // Error to start a row before the first batch.

    try {
      rootWriter.start();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Error to end a row before the first batch.

    try {
      rootWriter.save();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Because writing is an inner loop; no checks are
    // done to ensure that writing occurs only in the proper
    // state. So, can't test setInt() in the wrong state.

    rsLoader.startBatch();
    try {
      rsLoader.startBatch();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    assertFalse(rootWriter.isFull());

    rootWriter.start();
    rootWriter.scalar(0).setInt(100);
    assertEquals(0, rootWriter.rowCount());
    assertEquals(0, rsLoader.batchCount());
    rootWriter.save();
    assertEquals(1, rootWriter.rowCount());
    assertEquals(1, rsLoader.batchCount());
    assertEquals(1, rsLoader.totalRowCount());

    // Can add a field after first row, prior rows are
    // "back-filled".

    MaterializedField fieldB = SchemaBuilder.columnSchema("b", MinorType.INT, DataMode.OPTIONAL);
    rootWriter.addColumn(fieldB);

    assertEquals(2, schema.size());
    assertSame(fieldB, schema.column(1));
    assertSame(fieldB, schema.column("b"));

    rootWriter.start();
    rootWriter.scalar(0).setInt(200);
    rootWriter.scalar(1).setInt(210);
    rootWriter.save();
    assertEquals(2, rootWriter.rowCount());
    assertEquals(1, rsLoader.batchCount());
    assertEquals(2, rsLoader.totalRowCount());

    // Harvest the first batch. Version number is the number
    // of columns added.

    assertFalse(rootWriter.isFull());
    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(2, rsLoader.schemaVersion());
    assertEquals(0, rootWriter.rowCount());
    assertEquals(1, rsLoader.batchCount());
    assertEquals(2, rsLoader.totalRowCount());

    SingleRowSet expected = fixture.rowSetBuilder(result.batchSchema())
        .add(100, null)
        .add(200, 210)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(result);

    // Between batches: batch-based operations fail

    try {
      rootWriter.start();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsLoader.harvest();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rootWriter.save();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Create a second batch

    rsLoader.startBatch();
    assertEquals(0, rootWriter.rowCount());
    assertEquals(1, rsLoader.batchCount());
    assertEquals(2, rsLoader.totalRowCount());
    rootWriter.start();
    rootWriter.scalar(0).setInt(300);
    rootWriter.scalar(1).setInt(310);
    rootWriter.save();
    assertEquals(1, rootWriter.rowCount());
    assertEquals(2, rsLoader.batchCount());
    assertEquals(3, rsLoader.totalRowCount());
    rootWriter.start();
    rootWriter.scalar(0).setInt(400);
    rootWriter.scalar(1).setInt(410);
    rootWriter.save();

    // Harvest. Schema has not changed.

    result = fixture.wrap(rsLoader.harvest());
    assertEquals(2, rsLoader.schemaVersion());
    assertEquals(0, rootWriter.rowCount());
    assertEquals(2, rsLoader.batchCount());
    assertEquals(4, rsLoader.totalRowCount());

    expected = fixture.rowSetBuilder(result.batchSchema())
        .add(300, 310)
        .add(400, 410)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(result);

    // Next batch. Schema has changed.

    rsLoader.startBatch();
    rootWriter.start();
    rootWriter.scalar(0).setInt(500);
    rootWriter.scalar(1).setInt(510);
    rootWriter.addColumn(SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.OPTIONAL));
    rootWriter.scalar(2).setInt(520);
    rootWriter.save();
    rootWriter.start();
    rootWriter.scalar(0).setInt(600);
    rootWriter.scalar(1).setInt(610);
    rootWriter.scalar(2).setInt(620);
    rootWriter.save();

    result = fixture.wrap(rsLoader.harvest());
    assertEquals(3, rsLoader.schemaVersion());
    expected = fixture.rowSetBuilder(result.batchSchema())
        .add(500, 510, 520)
        .add(600, 610, 620)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(result);

    rsLoader.close();

    // Key operations fail after close.

    try {
      rootWriter.start();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsLoader.writer();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsLoader.startBatch();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsLoader.harvest();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rootWriter.save();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Benign to close twice

    rsLoader.close();
  }

  /**
   * Schemas are case insensitive by default. Verify that
   * the schema mechanism works, with emphasis on the
   * case insensitive case.
   */

  @Test
  public void testCaseInsensitiveSchema() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();

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
    rootWriter.addColumn(colSchema);

    // Can now be found, case insensitive

    assertSame(colSchema, schema.column(0));
    assertSame(colSchema, schema.column("a"));
    assertSame(colSchema, schema.column("A"));
    assertNotNull(rootWriter.column(0));
    assertNotNull(rootWriter.column("a"));
    assertNotNull(rootWriter.column("A"));
    assertEquals(1, schema.size());
    assertEquals(0, schema.index("a"));
    assertEquals(0, schema.index("A"));

    // Reject a duplicate name, case insensitive

    try {
      rootWriter.addColumn(colSchema);
      fail();
    } catch(IllegalArgumentException e) {
      // Expected
    }
    try {
      MaterializedField testCol = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
      rootWriter.addColumn(testCol);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains("Duplicate"));
    }

    // Can still add required fields while writing the first row.

    rsLoader.startBatch();
    rootWriter.start();
    rootWriter.scalar(0).setString("foo");

    MaterializedField col2 = SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED);
    rootWriter.addColumn(col2);
    assertSame(col2, schema.column(1));
    assertSame(col2, schema.column("b"));
    assertSame(col2, schema.column("B"));
    assertEquals(2, schema.size());
    assertEquals(1, schema.index("b"));
    assertEquals(1, schema.index("B"));
    rootWriter.scalar(1).setString("second");

    // After first row, can add an optional or repeated.
    // Also allows a required field: values will be back-filled.

    rootWriter.save();
    rootWriter.start();
    rootWriter.scalar(0).setString("bar");
    rootWriter.scalar(1).setString("");

    MaterializedField col3 = SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.REQUIRED);
    rootWriter.addColumn(col3);
    assertSame(col3, schema.column(2));
    assertSame(col3, schema.column("c"));
    assertSame(col3, schema.column("C"));
    assertEquals(3, schema.size());
    assertEquals(2, schema.index("c"));
    assertEquals(2, schema.index("C"));
    rootWriter.scalar("c").setString("c.2");

    MaterializedField col4 = SchemaBuilder.columnSchema("d", MinorType.VARCHAR, DataMode.OPTIONAL);
    rootWriter.addColumn(col4);
    assertSame(col4, schema.column(3));
    assertSame(col4, schema.column("d"));
    assertSame(col4, schema.column("D"));
    assertEquals(4, schema.size());
    assertEquals(3, schema.index("d"));
    assertEquals(3, schema.index("D"));
    rootWriter.scalar("d").setString("d.2");

    MaterializedField col5 = SchemaBuilder.columnSchema("e", MinorType.VARCHAR, DataMode.REPEATED);
    rootWriter.addColumn(col5);
    assertSame(col5, schema.column(4));
    assertSame(col5, schema.column("e"));
    assertSame(col5, schema.column("E"));
    assertEquals(5, schema.size());
    assertEquals(4, schema.index("e"));
    assertEquals(4, schema.index("E"));
    rootWriter.array(4).set("e1", "e2", "e3");
    rootWriter.save();

    // Verify. No reason to expect problems, but might as well check.

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(5, rsLoader.schemaVersion());
    SingleRowSet expected = fixture.rowSetBuilder(result.batchSchema())
        .add("foo", "second", "",    null,  new String[] { } )
        .add("bar", "",       "c.2", "d.2", new String[] {"e1", "e2", "e3"} )
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(result);

    // Handy way to test that close works to abort an in-flight batch
    // and clean up.

    rsLoader.close();
  }

  /**
   * Simplified schema test to verify the case-sensitive schema
   * option. Note that case-sensitivity is supported in this writer,
   * but nowhere else in Drill. Still, JSON is case-sensitive and
   * we have to start somewhere...
   */

  @Test
  public void testCaseSensitiveSchema() {
    ResultSetOptions options = new ResultSetLoaderImpl.OptionBuilder()
        .setCaseSensitive(true)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();

    MaterializedField col1 = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
    schema.add(col1);

    MaterializedField col2 = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
    schema.add(col2);

    assertSame(col1, schema.column(0));
    assertSame(col1, schema.column("a"));
    assertSame(col2, schema.column(1));
    assertSame(col2, schema.column("A"));
    assertEquals(2, schema.size());
    assertEquals(0, schema.index("a"));
    assertEquals(1, schema.index("A"));

    rsLoader.startBatch();
    rootWriter.scalar(0).setString("lower");
    rootWriter.scalar(1).setString("upper");

    // We'd like to verify the values, but even the row set
    // abstraction is case in-sensitive, so this is as far as
    // we can go.

    // TODO: Validate the values when the row set tools support
    // case-insensitivity.

    rsLoader.close();
  }

  /**
   * Verify that the writer stops when reaching the row limit.
   * In this case there is no look-ahead row.
   */

  @Test
  public void testRowLimit() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    rootWriter.addColumn(SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REQUIRED));

    byte value[] = new byte[200];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsLoader.startBatch();
    while (! rootWriter.isFull()) {
      rootWriter.start();
      rootWriter.scalar(0).setBytes(value, value.length);
      rootWriter.save();
      count++;
    }
    assertEquals(ValueVector.MAX_ROW_COUNT, count);
    assertEquals(count, rootWriter.rowCount());

    rsLoader.harvest().clear();

    // Do it again, a different way.

    count = 0;
    rsLoader.startBatch();
    assertEquals(0, rootWriter.rowCount());
    while (rootWriter.start()) {
      rootWriter.scalar(0).setBytes(value, value.length);
      rootWriter.save();
      count++;
    }
    assertEquals(ValueVector.MAX_ROW_COUNT, count);
    assertEquals(count, rootWriter.rowCount());

    rsLoader.harvest().clear();

    rsLoader.close();
  }

  private static final int TEST_ROW_LIMIT = 1024;

  /**
   * Verify that the caller can set a row limit lower than the default.
   */

  @Test
  public void testCustomRowLimit() {

    // Try to set a default value larger than the hard limit. Value
    // is truncated to the limit.

    ResultSetOptions options = new ResultSetLoaderImpl.OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT + 1)
        .build();
    assertEquals(ValueVector.MAX_ROW_COUNT, options.rowCountLimit);

    // Just a bit of paranoia that we check against the vector limit,
    // not any previous value...

    options = new ResultSetLoaderImpl.OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT + 1)
        .setRowCountLimit(TEST_ROW_LIMIT)
        .build();
    assertEquals(TEST_ROW_LIMIT, options.rowCountLimit);

    options = new ResultSetLoaderImpl.OptionBuilder()
        .setRowCountLimit(TEST_ROW_LIMIT)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT + 1)
        .build();
    assertEquals(ValueVector.MAX_ROW_COUNT, options.rowCountLimit);

    // Do load with a (valid) limit lower than the default.

    options = new ResultSetLoaderImpl.OptionBuilder()
        .setRowCountLimit(TEST_ROW_LIMIT)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();
    rootWriter.addColumn(SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REQUIRED));

    byte value[] = new byte[200];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsLoader.startBatch();
    while (! rootWriter.isFull()) {
      rootWriter.start();
      rootWriter.scalar(0).setBytes(value, value.length);
      rootWriter.save();
      count++;
    }
    assertEquals(TEST_ROW_LIMIT, count);
    assertEquals(count, rootWriter.rowCount());

    // Should fail to write beyond the row limit

    assertFalse(rootWriter.start());
    try {
      rootWriter.save();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    rsLoader.harvest().clear();
    rsLoader.startBatch();
    assertEquals(0, rootWriter.rowCount());

    rsLoader.close();
  }

  /**
   * Test that the writer detects a vector overflow. The offending column
   * value should be moved to the next batch.
   */

  @Test
  public void testSizeLimit() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    MaterializedField field = SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REQUIRED);
    rootWriter.addColumn(field);

    rsLoader.startBatch();
    byte value[] = new byte[512];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    while (! rootWriter.isFull()) {
      rootWriter.start();
      rootWriter.scalar(0).setBytes(value, value.length);
      rootWriter.save();
      count++;
    }

    // Our row count should include the overflow row

    int expectedCount = ValueVector.MAX_BUFFER_SIZE / value.length;
    assertEquals(expectedCount + 1, count);

    // Loader's row count should include only "visible" rows

    assertEquals(expectedCount, rootWriter.rowCount());

    // Total count should include invisible and look-ahead rows.

    assertEquals(expectedCount + 1, rsLoader.totalRowCount());

    // Result should exclude the overflow row

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(expectedCount, result.rowCount());
    result.clear();

    // Next batch should start with the overflow row

    rsLoader.startBatch();
    assertEquals(1, rootWriter.rowCount());
    assertEquals(expectedCount + 1, rsLoader.totalRowCount());
    result = fixture.wrap(rsLoader.harvest());
    assertEquals(1, result.rowCount());
    result.clear();

    rsLoader.close();
  }

  /**
   * Case where a single array fills up the vector to the maximum size
   * limit. Overflow won't work here; the attempt will fail with a user
   * exception.
   */

  @Test
  public void testOversizeArray() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();
    MaterializedField field = SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REPEATED);
    schema.add(field);

    // Create a single array as the column value in the first row. When
    // this overflows, an exception is thrown since overflow is not possible.

    rsLoader.startBatch();
    byte value[] = new byte[473];
    Arrays.fill(value, (byte) 'X');
    rsLoader.start();
    ScalarWriter array = rootWriter.array(0).scalar();
    try {
      for (int i = 0; i < ValueVector.MAX_ROW_COUNT; i++) {
        array.setBytes(value, value.length);
      }
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("column value is larger than the maximum"));
    }
    rsLoader.close();
  }

  @Test
  public void testSizeLimitOnArray() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();
    MaterializedField field = SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REPEATED);
    schema.add(field);

    // Fill batch with rows of with a single array, three values each. Tack on
    // a suffix to each so we can be sure the proper data is written and moved
    // to the overflow batch.

    rsLoader.startBatch();
    byte value[] = new byte[473];
    Arrays.fill(value, (byte) 'X');
    String strValue = new String(value, Charsets.UTF_8);
    int count = 0;
    while (! rsLoader.isFull()) {
      rsLoader.start();
      ScalarWriter array = rootWriter.array(0).scalar();
      for (int i = 0; i < 3; i++) {
        array.setString(strValue + (count + 1) + "." + i);
      }
      rsLoader.saveRow();
      count++;
    }

    // Row count should include the overflow row

    assertTrue(count <= ValueVector.MAX_BUFFER_SIZE / value.length / 3);
    int expectedCount = count - 1;

    // Result should exclude the overflow row. Last row
    // should hold the last full array.

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(expectedCount, result.rowCount());
    RowSetReader reader = result.reader();
    reader.set(expectedCount - 1);
    ScalarElementReader arrayReader = reader.column(0).elements();
    assertEquals(3, arrayReader.size());
    assertEquals(strValue + expectedCount + ".0", arrayReader.getString(0));
    assertEquals(strValue + expectedCount + ".1", arrayReader.getString(1));
    assertEquals(strValue + expectedCount + ".2", arrayReader.getString(2));
    result.clear();

    // Next batch should start with the overflow row.
    // Only row should be the whole array being written overflow.

    rsLoader.startBatch();
    assertEquals(1, rsLoader.rowCount());
    assertEquals(expectedCount + 1, rsLoader.totalRowCount());
    result = fixture.wrap(rsLoader.harvest());
    assertEquals(1, result.rowCount());
    reader = result.reader();
    reader.next();
    arrayReader = reader.column(0).elements();
    assertEquals(3, arrayReader.size());
    assertEquals(strValue + count + ".0", arrayReader.getString(0));
    assertEquals(strValue + count + ".1", arrayReader.getString(1));
    assertEquals(strValue + count + ".2", arrayReader.getString(2));
    result.clear();

    rsLoader.close();
  }

  /**
   * Create an array that contains more than 64K values. Drill has no numeric
   * limit on array lengths.
   */

  @Test
  public void testLargeArray() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();
    MaterializedField field = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REPEATED);
    schema.add(field);

    // Create a single array as the column value in the first row. When
    // this overflows, an exception is thrown since overflow is not possible.

    rsLoader.startBatch();
    rsLoader.start();
    ScalarWriter array = rootWriter.array(0).scalar();
    for (int i = 0; i < 2 * ValueVector.MAX_ROW_COUNT; i++) {
      array.setInt(i+1);
    }
    rsLoader.saveRow();
    rsLoader.harvest().zeroVectors();
    rsLoader.close();
  }

  /**
   * Test the case where the schema changes in the first batch.
   * Schema changes before the first record are trivial and tested
   * elsewhere. Here we write some records, then add new columns, as a
   * JSON reader might do.
   */

  @Test
  public void testSchemaChangeFirstBatch() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();
    schema.add(SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED));

    // Create initial rows

    rsLoader.startBatch();
    int rowCount = 0;
    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setString("a_" + rowCount);
      rsLoader.saveRow();
    }

    // Add a second column: nullable.

    schema.add(SchemaBuilder.columnSchema("b", MinorType.INT, DataMode.OPTIONAL));
    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setString("a_" + rowCount);
      rootWriter.scalar(1).setInt(rowCount);
      rsLoader.saveRow();
    }

    // Add a third column. Use variable-width so that offset
    // vectors must be back-filled.

    schema.add(SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.OPTIONAL));
    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setString("a_" + rowCount);
      rootWriter.scalar(1).setInt(rowCount);
      rootWriter.scalar(2).setString("c_" + rowCount);
      rsLoader.saveRow();
    }

    // Fourth: Required Varchar. Previous rows are back-filled with empty strings.
    // And a required int. Back-filled with zeros.
    // May occasionally be useful. But, does have to work to prevent
    // vector corruption if some reader decides to go this route.

    schema.add(SchemaBuilder.columnSchema("d", MinorType.VARCHAR, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("e", MinorType.INT,     DataMode.REQUIRED));
    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setString("a_" + rowCount);
      rootWriter.scalar(1).setInt(rowCount);
      rootWriter.scalar(2).setString("c_" + rowCount);
      rootWriter.scalar(3).setString("d_" + rowCount);
      rootWriter.scalar(4).setInt(rowCount * 10);
      rsLoader.saveRow();
    }

    // Add an array. Now two offset vectors must be back-filled.

    schema.add(SchemaBuilder.columnSchema("f", MinorType.VARCHAR, DataMode.REPEATED));
    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setString("a_" + rowCount);
      rootWriter.scalar(1).setInt(rowCount);
      rootWriter.scalar(2).setString("c_" + rowCount);
      rootWriter.scalar(3).setString("d_" + rowCount);
      rootWriter.scalar(4).setInt(rowCount * 10);
      ScalarWriter arrayWriter = rootWriter.column(5).array().scalar();
      arrayWriter.setString("f_" + rowCount + "-1");
      arrayWriter.setString("f_" + rowCount + "-2");
      rsLoader.saveRow();
    }

    // Harvest the batch and verify.

    RowSet actual = fixture.wrap(rsLoader.harvest());

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("b", MinorType.INT)
        .addNullable("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .add("e", MinorType.INT)
        .addArray("f", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add("a_1", null, null,   "",       0, new String[] {})
        .add("a_2", null, null,   "",       0, new String[] {})
        .add("a_3",    3, null,   "",       0, new String[] {})
        .add("a_4",    4, null,   "",       0, new String[] {})
        .add("a_5",    5, "c_5",  "",       0, new String[] {})
        .add("a_6",    6, "c_6",  "",       0, new String[] {})
        .add("a_7",    7, "c_7",  "d_7",   70, new String[] {})
        .add("a_8",    8, "c_8",  "d_8",   80, new String[] {})
        .add("a_9",    9, "c_9",  "d_9",   90, new String[] {"f_9-1",  "f_9-2"})
        .add("a_10",  10, "c_10", "d_10", 100, new String[] {"f_10-1", "f_10-2"})
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
    rsLoader.close();
  }

  /**
   * Test "holes" in the middle of a batch, and unset columns at
   * the end. Ending the batch should fill in missing values.
   */

  @Test
  public void testOmittedValuesAtEnd() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();

    // Create columns up front

    TupleMetadata schema = rootWriter.schema();
    schema.add(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.OPTIONAL));
    schema.add(SchemaBuilder.columnSchema("d", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("e", MinorType.INT, DataMode.OPTIONAL));
    schema.add(SchemaBuilder.columnSchema("f", MinorType.VARCHAR, DataMode.REPEATED));

    // Create initial rows

    rsLoader.startBatch();
    int rowCount = 0;
    ScalarWriter arrayWriter;
    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setInt(rowCount);
      rootWriter.scalar(1).setString("b_" + rowCount);
      rootWriter.scalar(2).setString("c_" + rowCount);
      rootWriter.scalar(3).setInt(rowCount * 10);
      rootWriter.scalar(4).setInt(rowCount * 100);
      arrayWriter = rootWriter.column(5).array().scalar();
      arrayWriter.setString("f_" + rowCount + "-1");
      arrayWriter.setString("f_" + rowCount + "-2");
      rsLoader.saveRow();
    }

    // Holes in half the columns

    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setInt(rowCount);
      rootWriter.scalar(1).setString("b_" + rowCount);
      rootWriter.scalar(3).setInt(rowCount * 10);
      arrayWriter = rootWriter.column(5).array().scalar();
      arrayWriter.setString("f_" + rowCount + "-1");
      arrayWriter.setString("f_" + rowCount + "-2");
      rsLoader.saveRow();
    }

    // Holes in the other half

    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setInt(rowCount);
      rootWriter.scalar(2).setString("c_" + rowCount);
      rootWriter.scalar(4).setInt(rowCount * 100);
      rsLoader.saveRow();
    }

    // All columns again.

    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setInt(rowCount);
      rootWriter.scalar(1).setString("b_" + rowCount);
      rootWriter.scalar(2).setString("c_" + rowCount);
      rootWriter.scalar(3).setInt(rowCount * 10);
      rootWriter.scalar(4).setInt(rowCount * 100);
      arrayWriter = rootWriter.column(5).array().scalar();
      arrayWriter.setString("f_" + rowCount + "-1");
      arrayWriter.setString("f_" + rowCount + "-2");
      rsLoader.saveRow();
    }

    // Omit all but key column at end

    for (int i = 0; i < 2;  i++) {
      rsLoader.start();
      rowCount++;
      rootWriter.scalar(0).setInt(rowCount);
      rsLoader.saveRow();
    }

    // Harvest the row and verify.

    RowSet actual = fixture.wrap(rsLoader.harvest());
//    actual.print();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addNullable("c", MinorType.VARCHAR)
        .add("3", MinorType.INT)
        .addNullable("e", MinorType.INT)
        .addArray("f", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(  1, "b_1", "c_1",  10,  100, new String[] {"f_1-1",  "f_1-2"})
        .add(  2, "b_2", "c_2",  20,  200, new String[] {"f_2-1",  "f_2-2"})
        .add(  3, "b_3", null,   30, null, new String[] {"f_3-1",  "f_3-2"})
        .add(  4, "b_4", null,   40, null, new String[] {"f_4-1",  "f_4-2"})
        .add(  5, "",    "c_5",   0,  500, new String[] {})
        .add(  6, "",    "c_6",   0,  600, new String[] {})
        .add(  7, "b_7", "c_7",  70,  700, new String[] {"f_7-1",  "f_7-2"})
        .add(  8, "b_8", "c_8",  80,  800, new String[] {"f_8-1",  "f_8-2"})
        .add(  9, "",    null,    0, null, new String[] {})
        .add( 10, "",    null,    0, null, new String[] {})
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
    rsLoader.close();
  }

  /**
   * Test "holes" at the end of a batch when batch overflows. Completed
   * batch must be finalized correctly, new batch initialized correct,
   * for the missing values.
   */

  @Test
  public void testOmittedValuesAtEndWithOverflow() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();
    // Row index
    schema.add(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    // Column that forces overflow
    schema.add(SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED));
    // Column with all holes
    schema.add(SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.OPTIONAL));
    // Column with some holes
    schema.add(SchemaBuilder.columnSchema("d", MinorType.VARCHAR, DataMode.OPTIONAL));

    // Fill the batch. Column d has some values. Column c is worst case: no values.

    rsLoader.startBatch();
    byte value[] = new byte[533];
    Arrays.fill(value, (byte) 'X');
    int rowNumber = 0;
    while (! rsLoader.isFull()) {
      rsLoader.start();
      rowNumber++;
      rootWriter.scalar(0).setInt(rowNumber);
      rootWriter.scalar(1).setBytes(value, value.length);
      if (rowNumber < 10_000) {
        rootWriter.scalar(3).setString("d-" + rowNumber);
      }
      rsLoader.saveRow();
      assertEquals(rowNumber, rsLoader.totalRowCount());
    }

    // Harvest and verify

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(rowNumber - 1, result.rowCount());
    RowSetReader reader = result.reader();
    int rowIndex = 0;
    while (reader.next()) {
      int expectedRowNumber = 1 + rowIndex;
      assertEquals(expectedRowNumber, reader.scalar(0).getInt());
      assertTrue(reader.scalar(2).isNull());
      if (expectedRowNumber < 10_000) {
        assertEquals("d-" + expectedRowNumber, reader.scalar(3).getString());
      } else {
        assertTrue(reader.scalar(3).isNull());
      }
      rowIndex++;
    }

    // Start count for this batch is one less than current
    // count, because of the overflow row.

    int startRowNumber = rowNumber;

    // Write a few more rows to the next batch

    rsLoader.startBatch();
    for (int i = 0; i < 10; i++) {
      rsLoader.start();
      rowNumber++;
      rootWriter.scalar(0).setInt(rowNumber);
      rootWriter.scalar(1).setBytes(value, value.length);
      if (i > 5) {
        rootWriter.scalar(3).setString("d-" + rowNumber);
      }
      rsLoader.saveRow();
      assertEquals(rowNumber, rsLoader.totalRowCount());
    }

    // Verify that holes were preserved.

    result = fixture.wrap(rsLoader.harvest());
    assertEquals(rowNumber, rsLoader.totalRowCount());
    assertEquals(rowNumber - startRowNumber + 1, result.rowCount());
//    result.print();
    reader = result.reader();
    rowIndex = 0;
    while (reader.next()) {
      int expectedRowNumber = startRowNumber + rowIndex;
      assertEquals(expectedRowNumber, reader.scalar(0).getInt());
      assertTrue(reader.scalar(2).isNull());
      if (rowIndex > 6) {
        assertEquals("d-" + expectedRowNumber, reader.scalar(3).getString());
      } else {
        assertTrue("Row " + rowIndex + " col d should be null", reader.scalar(3).isNull());
      }
      rowIndex++;
    }
    assertEquals(rowIndex, 11);

    rsLoader.close();
  }

  /**
   * Test a schema change on the row that overflows. If the
   * new column is added after overflow, it will appear as
   * a schema-change in the following batch. This is fine as
   * we are essentially time-shifting: pretending that the
   * overflow row was written in the next batch (which, in
   * fact, it is: that's what overflow means.)
   */

  @Test
  public void testSchemaChangeWithOverflow() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();
    schema.add(SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED));

    rsLoader.startBatch();
    byte value[] = new byte[512];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    while (! rsLoader.isFull()) {
      rsLoader.start();
      rootWriter.scalar(0).setBytes(value, value.length);

      // Relies on fact that isFull becomes true right after
      // a vector overflows; don't have to wait for saveRow().

      if (rsLoader.isFull()) {
        schema.add(SchemaBuilder.columnSchema("b", MinorType.INT, DataMode.OPTIONAL));
        rootWriter.scalar(1).setInt(count);

        // Add a Varchar to ensure its offset fiddling is done properly

        schema.add(SchemaBuilder.columnSchema("c", MinorType.VARCHAR, DataMode.OPTIONAL));
        rootWriter.scalar(2).setString("c-" + count);

        // Do not allow adding a required column at this point.

        try {
          schema.add(SchemaBuilder.columnSchema("d", MinorType.INT, DataMode.REQUIRED));
          fail();
        } catch (IllegalArgumentException e) {
          // Expected.
        }
      }
      rsLoader.saveRow();
      count++;
    }

    // Result should include only the first column.

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .build();
    RowSet result = fixture.wrap(rsLoader.harvest());
    assertTrue(result.batchSchema().isEquivalent(expectedSchema));
    assertEquals(count - 1, result.rowCount());
    result.clear();
    assertEquals(1, rsLoader.schemaVersion());

    // Double check: still can't add a required column after
    // starting the next batch. (No longer in overflow state.)

    rsLoader.startBatch();
    try {
      schema.add(SchemaBuilder.columnSchema("e", MinorType.INT, DataMode.REQUIRED));
      fail();
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    // Next batch should start with the overflow row, including
    // the column added at the end of the previous batch, after
    // overflow.

    result = fixture.wrap(rsLoader.harvest());
    assertEquals(3, rsLoader.schemaVersion());
    assertEquals(1, result.rowCount());
    expectedSchema = new SchemaBuilder(expectedSchema)
        .addNullable("b", MinorType.INT)
        .addNullable("c", MinorType.VARCHAR)
        .build();
    assertTrue(result.batchSchema().isEquivalent(expectedSchema));
    RowSetReader reader = result.reader();
    reader.next();
    assertEquals(count - 1, reader.scalar(1).getInt());
    assertEquals("c-" + (count - 1), reader.scalar(2).getString());
    result.clear();

    rsLoader.close();
  }

  /**
   * Test that omitting the call to saveRow() effectively discards
   * the row. Note that the vectors still contain values in the
   * discarded position; the client must overwrite all column values
   * on the next row to get the correct result. Leaving a column unset
   * will leave the value at the prior, discarded value.
   */

  @Test
  public void testSkipRows() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();
    schema.add(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.OPTIONAL));

    rsLoader.startBatch();
    int rowNumber = 0;
    for (int i = 0; i < 14; i++) {
      rsLoader.start();
      rowNumber++;
      rootWriter.scalar(0).setInt(rowNumber);
      if (i % 3 == 0) {
        rootWriter.scalar(1).setNull();
      } else {
        rootWriter.scalar(1).setString("b-" + rowNumber);
      }
      if (i % 2 == 0) {
        rsLoader.saveRow();
      }
    }

    RowSet result = fixture.wrap(rsLoader.harvest());
//    result.print();
    SingleRowSet expected = fixture.rowSetBuilder(result.batchSchema())
        .add( 1, null)
        .add( 3, "b-3")
        .add( 5, "b-5")
        .add( 7, null)
        .add( 9, "b-9")
        .add(11, "b-11")
        .add(13, null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(result);

    rsLoader.close();
  }

  /**
   * Test that discarding a row works even if that row happens to be an
   * overflow row.
   */

  @Test
  public void testSkipOverflowRow() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata schema = rootWriter.schema();
    schema.add(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.OPTIONAL));

    rsLoader.startBatch();
    byte value[] = new byte[512];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    while (! rsLoader.isFull()) {
      rsLoader.start();
      rootWriter.scalar(0).setInt(count);
      rootWriter.scalar(1).setBytes(value, value.length);

      // Relies on fact that isFull becomes true right after
      // a vector overflows; don't have to wait for saveRow().
      // Keep all rows, but discard the overflow row.

      if (! rsLoader.isFull()) {
        rsLoader.saveRow();
      }
      count++;
    }

    // Discard the results.

    rsLoader.harvest().zeroVectors();

    // Harvest the next batch. Will be empty (because overflow row
    // was discarded.)

    rsLoader.startBatch();
    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(0, result.rowCount());
    result.clear();

    rsLoader.close();
  }

  /**
   * Test imposing a selection mask between the client and the underlying
   * vector container.
   */

  @Test
  public void testSelection() {
    List<String> selection = Lists.newArrayList("c", "b");
    ResultSetOptions options = new ResultSetLoaderImpl.OptionBuilder()
        .setProjection(selection)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();
    assertTrue(rootWriter instanceof LogicalRowSetLoader);
    TupleMetadata schema = rootWriter.schema();
    schema.add(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("b", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("d", MinorType.INT, DataMode.REQUIRED));

    assertEquals(4, schema.size());
    assertEquals("a", schema.column(0).getName());
    assertEquals("b", schema.column(1).getName());
    assertEquals("c", schema.column(2).getName());
    assertEquals("d", schema.column(3).getName());
    assertEquals(0, schema.index("A"));
    assertEquals(3, schema.index("d"));
    assertEquals(-1, schema.index("e"));

    rsLoader.startBatch();
    for (int i = 1; i < 3; i++) {
      rsLoader.start();
      assertNull(rootWriter.column(0));
      rootWriter.scalar(1).setInt(i);
      rootWriter.scalar(2).setInt(i * 10);
      assertNull(rootWriter.column(3));
      rsLoader.saveRow();
    }

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("b", MinorType.INT)
        .add("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(1, 10)
        .add(2, 20)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(rsLoader.harvest()));
    rsLoader.close();
  }

  // TODO: Add a method that resets current row to default values

  // TODO: Test initial vector allocation
}
