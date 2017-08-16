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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.TupleWriter.UndefinedColumnException;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

/**
 * Tests of the overall result set loader protocol focusing on which operations
 * are valid in each state, basics of column lookup, basics of adding columns
 * and so on. Uses the simplest possible type: a required int.
 * <p>
 * Run this test first to do a sanity check of the result set loader after making
 * changes.
 */

public class TestResultSetLoaderProtocol extends SubOperatorTest {

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
   * Provide a schema up front to the loader; schema is built before
   * the first row.
   * <p>
   * Also verifies the test-time method to set a row of values using
   * a single method.
   */

  @Test
  public void testInitialSchema() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.INT)
        .add("c", MinorType.VARCHAR)
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new ResultSetLoaderImpl.OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();
    assertSame(schema, rootWriter.schema());

    rsLoader.startBatch();
    rootWriter
        .setRow(10, 100, "fred")
        .setRow(20, null, "barney")
        .setRow(30, 300, "wilma");
    RowSet actual = fixture.wrap(rsLoader.harvest());

    RowSet expected = fixture.rowSetBuilder(schema)
        .add(10, 100, "fred")
        .add(20, null, "barney")
        .add(30, 300, "wilma")
        .build();

    new RowSetComparison(expected).verifyAndClearAll(actual);
    rsLoader.close();
  }
}
