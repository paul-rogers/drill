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
package org.apache.drill.exec.store.easy.text.compliant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Sanity test of CSV files with headers. Tests both the original
 * "compliant" version and the V3 version based on the row set
 * framework.
 *
 * @see {@link TestHeaderBuilder}
 */

// CSV reader now hosted on the row set framework
@Category(RowSetTests.class)
public class TestCsvWithHeaders extends BaseCsvTest {

  private static final String TEST_FILE_NAME = "case2.csv";

  private static String invalidHeaders[] = {
      "$,,9b,c,c,c_2",
      "10,foo,bar,fourth,fifth,sixth"
  };

  private static String emptyHeaders[] = {
      "",
      "10,foo,bar"
  };

  private static String raggedRows[] = {
      "a,b,c",
      "10,dino",
      "20,foo,bar",
      "30"
  };

  @BeforeClass
  public static void setup() throws Exception {
    BaseCsvTest.setup(false,  true);
    buildFile(TEST_FILE_NAME, validHeaders);
    buildNestedTable();
  }

  private static final String EMPTY_FILE = "empty.csv";

  @Test
  public void testEmptyFile() throws IOException {
    buildFile(EMPTY_FILE, new String[] {});
    try {
      enableV3(false);
      doTestEmptyFile();
      enableV3(true);
      doTestEmptyFile();
    } finally {
      resetV3();
    }
  }

  private void doTestEmptyFile() throws IOException {
    RowSet rowSet = client.queryBuilder().sql(makeStatement(EMPTY_FILE)).rowSet();
    assertNull(rowSet);
    //assertEquals(0, rowSet.rowCount());
//    rowSet.clear();
  }

  private static final String EMPTY_HEADERS_FILE = "noheaders.csv";

  /**
   * Trivial case: empty header. This case should fail.
   */

  @Test
  public void testEmptyCsvHeaders() throws IOException {
    buildFile(EMPTY_HEADERS_FILE, emptyHeaders);
    try {
      enableV3(false);
      doTestEmptyCsvHeaders();
      enableV3(true);
      doTestEmptyCsvHeaders();
    } finally {
      resetV3();
    }
  }

  private void doTestEmptyCsvHeaders() throws IOException {
    try {
      client.queryBuilder().sql(makeStatement(EMPTY_HEADERS_FILE)).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("must define at least one header"));
    }
  }

  @Test
  public void testValidCsvHeaders() throws IOException {
    try {
      enableV3(false);
      doTestValidCsvHeaders();
      enableV3(true);
      doTestValidCsvHeaders();
    } finally {
      resetV3();
    }
  }

  private void doTestValidCsvHeaders() throws IOException {
    RowSet actual = client.queryBuilder().sql(makeStatement(TEST_FILE_NAME)).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testInvalidCsvHeaders() throws IOException {
    try {
      enableV3(false);
      doTestInvalidCsvHeaders();
      enableV3(true);
      doTestInvalidCsvHeaders();
    } finally {
      resetV3();
    }
  }

  private void doTestInvalidCsvHeaders() throws IOException {
    String fileName = "case3.csv";
    buildFile(fileName, invalidHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName)).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("column_1", MinorType.VARCHAR)
        .add("column_2", MinorType.VARCHAR)
        .add("col_9b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("c_2", MinorType.VARCHAR)
        .add("c_2_2", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar", "fourth", "fifth", "sixth")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testCsvHeadersCaseInsensitive() throws IOException {
    try {
      enableV3(false);
      doTestCsvHeadersCaseInsensitive();
      enableV3(true);
      doTestCsvHeadersCaseInsensitive();
    } finally {
      resetV3();
    }
  }

  // Test fix for DRILL-5590
  private void doTestCsvHeadersCaseInsensitive() throws IOException {
    String sql = "SELECT A, b, C FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  private String makeStatement(String fileName) {
    return "SELECT * FROM `dfs.data`.`" + fileName + "`";
  }

  @Test
  public void testWildcard() throws IOException {
    try {
      enableV3(false);
      doTestWildcard();
      enableV3(true);
      doTestWildcard();
    } finally {
      resetV3();
    }
  }

  /**
   * Verify that the wildcard expands columns to the header names, including
   * case
   */
  private void doTestWildcard() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column.
   */

  @Test
  public void testImplicitColsExplicitSelectV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT A, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("A", MinorType.VARCHAR)
          .addNullable("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testImplicitColsExplicitSelectV3() throws IOException {
    try {
      enableV3(true);
      String sql = "SELECT A, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("A", MinorType.VARCHAR)
          .add("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column.
   */

  @Test
  public void testImplicitColsWildcardV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", "foo", "bar", TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testImplicitColsWildcardV3() throws IOException {
    try {
      enableV3(true);
      String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .add("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", "foo", "bar", TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testColsWithWildcard() throws IOException {
    try {
      enableV3(false);
      doTestColsWithWildcard();
      enableV3(true);
      doTestColsWithWildcard();
    } finally {
      resetV3();
    }
  }

  private void doTestColsWithWildcard() throws IOException {
    String sql = "SELECT *, a as d FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar", "10")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testPartitionColsWildcardV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT *, dir0 FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.INT)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", "foo", "bar", null)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testPartitionColsWildcardV3() throws IOException {
    try {
      enableV3(true);
      String sql = "SELECT *, dir0 FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", "foo", "bar", null)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testImplicitColWildcardV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", "foo", "bar", TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testImplicitColWildcardV3() throws IOException {
    try {
      enableV3(true);
      String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .add("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", "foo", "bar", TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  /**
   * CSV does not allow explicit use of dir0, dir1, etc. columns. Treated
   * as undefined nullable int columns.
   */

  @Test
  public void testPartitionColsExplicitV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT a, dir0, dir5 FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.INT)
          .addNullable("dir5", MinorType.INT)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", null, null)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testPartitionColsExplicitV3() throws IOException {
    try {
      enableV3(true);
      String sql = "SELECT a, dir0, dir5 FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.VARCHAR)
          .addNullable("dir5", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", null, null)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testDupColumn() throws IOException {
    try {
      enableV3(false);
      doTestDupColumn();
      enableV3(true);
      doTestDupColumn();
    } finally {
      resetV3();
    }
  }

  private void doTestDupColumn() throws IOException {
    String sql = "SELECT a, b, a FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("a0", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "10")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  // This test cannot be run for V2. The data gets corrupted and we get
  // internal errors.

  @Test
  public void testRaggedRowsV3() throws IOException {
    try {
      enableV3(true);
      String fileName = "case4.csv";
      buildFile(fileName, raggedRows);
      RowSet actual = client.queryBuilder().sql(makeStatement(fileName)).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .buildSchema();
      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", "dino", "")
          .addRow("20", "foo", "bar")
          .addRow("30", "", "")
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  /**
   * Test partition expansion. Because the two files are read in the
   * same scan operator, the schema is consistent. See
   * {@link TestPartitionRace} for the multi-threaded race where all
   * hell breaks loose.
   */
  @Test
  public void testPartitionExpansionV2() throws IOException {
    try {
      enableV3(false);

      String sql = "SELECT * FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addNullable("dir0", MinorType.VARCHAR)
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .buildSchema();

      // Read the two batches.

      for (int i = 0; i < 2; i++) {
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();

        // Figure out which record this is and test accordingly.

        RowSetReader reader = rowSet.reader();
        assertTrue(reader.next());
        String col2 = reader.scalar(1).getString();
        if (col2.equals("10")) {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(null, "10", "foo", "bar")
              .build();
          RowSetUtilities.verify(expected, rowSet);
        } else {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(NESTED_DIR, "20", "fred", "wilma")
              .build();
          RowSetUtilities.verify(expected, rowSet);
        }
      }
      assertFalse(iter.hasNext());
    } finally {
      resetV3();
    }
  }

  /**
   * Test partition expansion.
   * <p>
   * This test is tricky because it will return two data batches
   * (preceded by an empty schema batch.) File read order is random
   * so we have to expect the files in either order.
   */
  @Test
  public void testPartitionExpansionV3() throws IOException {
    try {
      enableV3(true);

      String sql = "SELECT * FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.VARCHAR)
          .buildSchema();

      // First batch is empty; just carries the schema.

      assertTrue(iter.hasNext());
      RowSet rowSet = iter.next();
      assertEquals(0, rowSet.rowCount());
      rowSet.clear();

      // Read the other two batches.

      for (int i = 0; i < 2; i++) {
        assertTrue(iter.hasNext());
        rowSet = iter.next();

        // Figure out which record this is and test accordingly.

        RowSetReader reader = rowSet.reader();
        assertTrue(reader.next());
        String col1 = reader.scalar(0).getString();
        if (col1.equals("10")) {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow("10", "foo", "bar", null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        } else {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow("20", "fred", "wilma", NESTED_DIR)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        }
      }
      assertFalse(iter.hasNext());
    } finally {
      resetV3();
    }
  }

  @Test
  public void testWilcardAndPartitionsMultiFilesV2() throws IOException {
    try {
      enableV3(false);

      String sql = "SELECT *, dir0, dir1 FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addNullable("dir0", MinorType.VARCHAR)
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir00", MinorType.VARCHAR)
          .addNullable("dir1", MinorType.INT)
          .buildSchema();

      // Read the two batches.

      for (int i = 0; i < 2; i++) {
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();
        rowSet.print();

        // Figure out which record this is and test accordingly.

        RowSetReader reader = rowSet.reader();
        assertTrue(reader.next());
        String aCol = reader.scalar("a").getString();
        if (aCol.equals("10")) {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(null, "10", "foo", "bar", null, null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        } else {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(NESTED_DIR, "20", "fred", "wilma", null, null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        }
      }
      assertFalse(iter.hasNext());
    } finally {
      resetV3();
    }
  }

  @Test
  public void doTestExplicitPartitionsMultiFilesV2() throws IOException {
    try {
      enableV3(false);

      String sql = "SELECT a, b, c, dir0, dir1 FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.VARCHAR)
          .addNullable("dir1", MinorType.INT)
          .buildSchema();

      // Read the two batches.

      for (int i = 0; i < 2; i++) {
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();
        rowSet.print();

        // Figure out which record this is and test accordingly.

        RowSetReader reader = rowSet.reader();
        assertTrue(reader.next());
        String aCol = reader.scalar("a").getString();
        if (aCol.equals("10")) {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow("10", "foo", "bar", null, null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        } else {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow("20", "fred", "wilma", NESTED_DIR, null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        }
      }
      assertFalse(iter.hasNext());
    } finally {
      resetV3();
    }
  }

  @Test
  public void doTestExplicitPartitionsMultiFilesV3() throws IOException {
    try {
      enableV3(true);

      String sql = "SELECT a, b, c, dir0, dir1 FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.VARCHAR)
          .addNullable("dir1", MinorType.VARCHAR)
          .buildSchema();

      // First batch is empty; just carries the schema.

      assertTrue(iter.hasNext());
      RowSet rowSet = iter.next();
      assertEquals(0, rowSet.rowCount());
      rowSet.clear();

      // Read the two batches.

      for (int i = 0; i < 2; i++) {
        assertTrue(iter.hasNext());
        rowSet = iter.next();
        rowSet.print();

        // Figure out which record this is and test accordingly.

        RowSetReader reader = rowSet.reader();
        assertTrue(reader.next());
        String aCol = reader.scalar("a").getString();
        if (aCol.equals("10")) {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow("10", "foo", "bar", null, null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        } else {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow("20", "fred", "wilma", NESTED_DIR, null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        }
      }
      assertFalse(iter.hasNext());
    }
    finally {
      resetV3();
    }
  }
}
