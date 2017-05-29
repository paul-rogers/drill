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
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SQL-level tests for CSV headers. See
 * {@link TestHeaderBuilder} for detailed unit tests.
 * This test does not attempt to duplicate all the cases
 * from the unit tests; instead it just does a sanity check.
 */

public class TestCsv extends ClusterTest {

  public static final String HEADERS = "headers";
  public static final String NO_HEADERS = "plain";
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
    cluster.defineWorkspace("dfs", HEADERS, testDir.getAbsolutePath(), "csv", csvFormat);

    // And one without headers.

//    csvFormat = new TextFormatConfig();
//    csvFormat.fieldDelimiter = ',';
//    csvFormat.skipFirstLine = true;
//    csvFormat.extractHeader = false;
//    cluster.defineWorkspace("dfs", NO_HEADERS, testDir.getAbsolutePath(), "csv", csvFormat);
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
  public void testEmptyCsvHeaders() throws IOException {
    String fileName = "emptyh.csv";
    buildFile(fileName, emptyHeaders);
    try {
      client.queryBuilder().sql(makeStatement(HEADERS, fileName, STAR)).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("must define at least one header"));
    }
  }

  String emptyFile[] = { };

  /**
   * Test an empty CSV file without headers. Should return an
   * empty result set.
   *
   * @throws Exception
   */

  @Test
  public void testEmptyCsvFileStar() throws Exception {
    String fileName = "emptyp.csv";
    buildFile(fileName, emptyFile);
    RowSet actual = client.queryBuilder().sql(makeStatement(NO_HEADERS, fileName, STAR)).rowSet();
    SingleRowSet expected = client.rowSetBuilder(columnsSchema())
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  @Test
  public void testEmptyCsvFileColumns() throws Exception {
    String fileName = "emptyp.csv";
    buildFile(fileName, emptyFile);
    RowSet results = client.queryBuilder().sql(makeStatement(NO_HEADERS, fileName, COLUMNS)).rowSet();
    SingleRowSet expected = client.rowSetBuilder(columnsSchema())
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
  }

  private BatchSchema columnsSchema() {
    return new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .build();
  }

  String validHeaders[] = {
      "a,b,c",
      "10,foo,bar"
  };

  @Test
  public void testValidCsvHeadersStar() throws IOException {
    String fileName = "validHeads.csv";
    buildFile(fileName, validHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(HEADERS, fileName, STAR)).rowSet();

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
      .verifyAndClearAll(actual);
  }

  @Test
  public void testValidCsvHeadersSelection() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(HEADERS, fileName, "c, a")).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add("bar", "10")
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }


  @Test
  public void testValidCsvHeadersMissing() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(HEADERS, fileName, "a, c, d")).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add("10", "bar", null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  String invalidHeaders[] = {
      "$,,9b,c,c,c_2",
      "10,foo,bar,fourth,fifth,sixth"
  };

  @Test
  public void testInvalidCsvHeaders() throws IOException {
    String fileName = "case3h.csv";
    buildFile(fileName, invalidHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(HEADERS, fileName, "*")).rowSet();

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
      .verifyAndClearAll(actual);
  }

  @Test
  public void testCsvHeadersWithColumns() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(HEADERS, fileName, COLUMNS)).rowSet();

    RowSet expected = client.rowSetBuilder(columnsSchema())
        .addSingleCol(new String[] {"10", "foo", "bar"})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  @Test
  public void testCsvHeadersWithColumnsAndSelect() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(HEADERS, fileName, COLUMNS + ", a")).rowSet();

    RowSet expected = client.rowSetBuilder(columnsSchema())
        .addSingleCol(new String[] {"10", "foo", "bar"})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  @Test
  public void testCsvPlainWithColumnsAndSelect() throws IOException {
    String fileName = "headers.csv";
    buildFile(fileName, validHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(HEADERS, fileName, COLUMNS + ", a")).rowSet();

    RowSet expected = client.rowSetBuilder(columnsSchema())
        .addSingleCol(new String[] {"10", "foo", "bar"})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  private String makeStatement(String workSpace, String fileName, String cols) {
    return "SELECT " + cols + " FROM `dfs." + workSpace + "`.`" + fileName + "`";
  }

  private void buildFile(String fileName, String[] data) throws IOException {
    try(PrintWriter out = new PrintWriter(new FileWriter(new File(testDir, fileName)))) {
      for (String line : data) {
        out.println(line);
      }
    }
  }
}
