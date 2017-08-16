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
