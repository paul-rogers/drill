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
 * limitations under the License.‰
 */
package org.apache.drill.exec.store.easy.text.compliant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * SQL-level tests for CSV headers. See
 * {@link TestHeaderBuilder} for detailed unit tests.
 * This test does not attempt to duplicate all the cases
 * from the unit tests; instead it just does a sanity check.
 */

public class TestCsvHeaders extends ClusterTest {

  public static final String WS = "data";
  public static final String STAR = "*";
  public static final String COLUMNS = "columns";

  private static File testDir;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder()
        .maxParallelization(1)
        );

    // Set up CSV storage plugin using headers.

    testDir = cluster.makeTempDir("csv");
    TextFormatConfig csvFormat = new TextFormatConfig();
    csvFormat.fieldDelimiter = ',';
    csvFormat.skipFirstLine = false;
    csvFormat.extractHeader = true;
    cluster.defineWorkspace("dfs", WS, testDir.getAbsolutePath(), "csv", csvFormat);
  }

  String emptyFile[] = { };

  /**
   * Test an empty CSV file without headers with SELECT *. Should return an
   * empty result set with schema as the columns array.
   *
   * @throws Exception if the test fails
   */

  @Test
  @Ignore("DRILL-5548")
  public void testEmptyFileStar() throws Exception {
    String fileName = "empty.csv";
    buildFile(fileName, emptyFile);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName, STAR)).rowSet();

    // See DRILL-5548. Empty schema? Return `columns` with no data?

    SingleRowSet expected = client.rowSetBuilder(columnsSchema())
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  String emptyHeaderNoData[] = {
      ""
  };

  @Test
  @Ignore("DRILL-5549")
  public void testEmptyHeaderStar() throws Exception {
    String fileName = "emptyHeader.csv";
    buildFile(fileName, emptyHeaderNoData);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName, STAR)).rowSet();

    // See DRILL-5548. Empty schema? Return `columns` with no data?

    SingleRowSet expected = client.rowSetBuilder(columnsSchema())
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  String emptyHeaders[] = {
      "",
      "10,foo,bar"
  };

  /**
   * Test an empty CSV file, with headers enabled. Should get
   * an error.
   * @throws IOException if test itself fails
   */

  @Test
  public void testEmptyHeaderWithData() throws IOException {
    String fileName = "emptyh.csv";
    buildFile(fileName, emptyHeaders);

    // Note: DRILL-5549 suggests that this change to returning the
    // data in a `columns` array, as if the file did not have headers.

    try {
      client.queryBuilder().sql(makeStatement(fileName, STAR)).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("must define at least one header"));
    }
  }

  /**
   * Test an empty CSV file without headers with SELECT columns. Should return an
   * empty result set with schema as the columns array.
   *
   * @throws Exception if the test fails
   */

  @Test
  public void testEmptyCsvFileColumns() throws Exception {
    String fileName = "empty.csv";
    buildFile(fileName, emptyFile);
    RowSet results = client.queryBuilder().sql(makeStatement(fileName, COLUMNS)).rowSet();
    SingleRowSet expected = client.rowSetBuilder(columnsSchema())
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(results);
  }

  String validHeaders[] = {
      "a,b,c",
      "10,foo,bar"
  };

  @Test
  public void testValidHeadersStar() throws IOException {
    String fileName = "validHeads.csv";
    buildFile(fileName, validHeaders);
    String sql = makeStatement(fileName, STAR);
    System.out.println(sql);
    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .build();
    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add("10", "foo", "bar")
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  @Test
  public void testValidHeadersColumns() throws IOException {
    String fileName = "validHeads.csv";
    buildFile(fileName, validHeaders);
    String sql = makeStatement(fileName, COLUMNS);
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    RowSet expected = client.rowSetBuilder(columnsSchema())
        .add(new Object[] {new String[] {"10", "foo", "bar"}})
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  @Test
  public void testValidHeadersSelectAll() throws IOException {
    String fileName = "validHeads.csv";
    buildFile(fileName, validHeaders);
    String sql = makeStatement(fileName, "a,b,c");
    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .build();
    assertEquals(expectedSchema, actual.batchSchema());

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add("10", "foo", "bar")
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  @Test
  public void testValidHeadersSelection() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName, "c, a")).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add("bar", "10")
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  @Test
  public void testValidHeadersMissingColumn() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    String sql = makeStatement(fileName, "a, c, d");
    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR) // See DRILL-5550
        // .addNullable(d, MinorType.VARCHAR) // Should be this
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add("10", "bar", "") // should be "10, "bar", null
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  String invalidHeaders[] = {
      "$,,9b,c,c,c_2",
      "10,foo,bar,fourth,fifth,sixth"
  };

  @Test
  public void testInvalidHeaders() throws IOException {
    String fileName = "invalidHeaders.csv";
    buildFile(fileName, invalidHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName, "*")).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("column_1", MinorType.VARCHAR)
        .add("column_2", MinorType.VARCHAR)
        .add("col_9b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("c_2", MinorType.VARCHAR)
        .add("c_2_2", MinorType.VARCHAR)
        .build();
    assertEquals(expectedSchema, actual.batchSchema());

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .add("10", "foo", "bar", "fourth", "fifth", "sixth")
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  @Test
  public void testColumnsAndSelect() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    String sql = makeStatement(fileName, COLUMNS + ", a");
//    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add(COLUMNS, MinorType.VARCHAR) // DRILL-5551
        //.addArray(COLUMNS, MinorType.VARCHAR) // Expected
        .add("a", MinorType.VARCHAR)
        .build();
    RowSet expected = client.rowSetBuilder(expectedSchema)
        //.add(new String[] {"10", "foo", "bar"}, "10") // Expected
        .add("", "10")
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  /**
   * Test <tt>SELECT *, a ... </tt>
   * Should fail but actually succeeds.
   */
  @Test
  public void testStarAndSelect() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    String sql = makeStatement(fileName, STAR + ", a");
//    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    // DRILL-5552: this should fail.
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .add("a0", MinorType.VARCHAR) // Bogus
        .build();
    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add("10", "foo", "bar", "10")
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  /**
   * Test <tt>SELECT *, columns ... </tt>
   * Should fail but actually succeeds.
   */
  @Test
  public void testStarAndColumns() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    String sql = makeStatement(fileName, STAR + ", " + COLUMNS);
//    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    // DRILL-5553: this should fail.
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .addNullable("columns", MinorType.INT) // Bogus
        .build();
    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add("10", "foo", "bar", null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  @Test
  public void testStarAndStar() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    String sql = makeStatement(fileName, STAR + ", " + STAR);
    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    // DRILL-5556: this should fail.
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .add("a0", MinorType.VARCHAR)
        .add("b0", MinorType.VARCHAR)
        .add("c0", MinorType.VARCHAR)
        .build();
    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add("10", "foo", "bar", "10", "foo", "bar")
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  private String makeStatement(String fileName, String cols) {
    return "SELECT " + cols + " FROM `dfs." + WS + "`.`" + fileName + "`";
  }

  private void buildFile(String fileName, String[] data) throws IOException {
    try(PrintWriter out = new PrintWriter(new FileWriter(new File(testDir, fileName)))) {
      for (String line : data) {
        out.println(line);
      }
    }
  }

  private BatchSchema columnsSchema() {
    return new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .build();
  }
}
