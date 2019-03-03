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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * SQL-level tests for CSV headers. See
 * {@link TestHeaderBuilder} for detailed unit tests.
 * This test does not attempt to duplicate all the cases
 * from the unit tests; instead it just does a sanity check.
 */

// CSV reader now hosted on the row set framework
@Category(RowSetTests.class)
public class TestCsvWithHeaders extends ClusterTest {

  private static final String TEST_FILE_NAME = "case2.csv";
  private static final String PART_DIR = "root";

  private static String invalidHeaders[] = {
      "$,,9b,c,c,c_2",
      "10,foo,bar,fourth,fifth,sixth"
  };

  private static String emptyHeaders[] = {
      "",
      "10,foo,bar"
  };

  private static String validHeaders[] = {
      "a,b,c",
      "10,foo,bar"
  };

  private static String raggedRows[] = {
      "a,b,c",
      "10,dino",
      "20,foo,bar",
      "30"
  };

  private static String secondFile[] = {
      "a,b,c",
      "20,fred,wilma"
  };

  private static File testDir;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));

    // Set up CSV storage plugin using headers.

    TextFormatConfig csvFormat = new TextFormatConfig();
    csvFormat.fieldDelimiter = ',';
    csvFormat.skipFirstLine = false;
    csvFormat.extractHeader = true;

    testDir = cluster.makeDataDir("data", "csv", csvFormat);
    buildFile(TEST_FILE_NAME, validHeaders);

    // Two-level partitioned table

    File rootDir = new File(testDir, PART_DIR);
    rootDir.mkdir();
    buildFile(new File(rootDir, "first.csv"), validHeaders);
    File nestedDir = new File(rootDir, "nested");
    nestedDir.mkdir();
    buildFile(new File(nestedDir, "second.csv"), secondFile);
  }

  @Test
  public void testEmptyCsvHeaders() throws IOException {
    String fileName = "case1.csv";
    buildFile(fileName, emptyHeaders);
    try {
      client.queryBuilder().sql(makeStatement(fileName)).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("must define at least one header"));
    }
  }

  @Test
  public void testValidCsvHeaders() throws IOException {
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

  // Test fix for DRILL-5590
  @Test
  public void testCsvHeadersCaseInsensitive() throws IOException {
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

  static void buildFile(String fileName, String[] data) throws IOException {
    buildFile(new File(testDir, fileName), data);
  }

  static void buildFile(File file, String[] data) throws IOException {
    try(PrintWriter out = new PrintWriter(new FileWriter(file))) {
      for (String line : data) {
        out.println(line);
      }
    }
  }

  /**
   * Verify that the wildcard expands columns to the header names, including
   * case
   */
  @Test
  public void testWildcard() throws IOException {
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
  public void testImplicitColsExplicitSelect() throws IOException {
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
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column.
   */
  @Test
  public void testImplicitColsWildcard() throws IOException {
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
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column.
   */
  @Test
  public void testColsWithWildcard() throws IOException {
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
  public void testPartitionColsWildcard() throws IOException {
    String sql = "SELECT *, dir0 FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();
    actual.print();

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
  }

  @Test
  public void testImplicitColWildcard() throws IOException {
    String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();
    actual.print();

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
  }

  /**
   * CSV does not allow explicit use of dir0, dir1, etc. columns. Treated
   * as undefined nullable int columns.
   */
  @Test
  public void testPartitionColsExplicit() throws IOException {
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
  }

  @Test
  public void testDupColumn() throws IOException {
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

  @Test
  public void testRaggedRows() throws IOException {
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
  }

  /**
   * Test partition expansion. The reader expands partitions, but projection
   * removes them since they are not referenced.
   * <p>
   * This test is tricky because it will return two data batches
   * (preceded by an empty schema batch.) File read order is random
   * so we have to expect the files in either order. If we read the
   * root file first, it will contain no dir0 column. That column
   * will appear in the second batch once the reader decends down
   * one level. (Or, the order will be reversed with the deeper file
   * read first, with the shallow file second. In this case the
   * dir0 won't disappear.)
   */
  @Test
  public void testPartitionExpansionRemoval() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";
    Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

    assertTrue(iter.hasNext());
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .buildSchema();

    // First batch is empty; just carries the schema.

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
            .addRow("10", "foo", "bar")
            .build();
        RowSetUtilities.verify(expected, rowSet);
      } else {
        RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
            .addRow("20", "fred", "wilma")
            .build();
        RowSetUtilities.verify(expected, rowSet);
      }
    }
    assertFalse(iter.hasNext());
  }
}
