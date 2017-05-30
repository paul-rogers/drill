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

import static org.junit.Assert.*;

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
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestCsvNoHeaders extends ClusterTest {

  public static final String WS = "data";
  public static final String STAR = "*";
  public static final String COLUMNS = "columns";

  private static File testDir;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder()
        .maxParallelization(1)
        );

    // Set up CSV storage plugin without headers.

    testDir = cluster.makeTempDir("csv");
    TextFormatConfig csvFormat = new TextFormatConfig();
    csvFormat.fieldDelimiter = ',';
    csvFormat.skipFirstLine = false;
    csvFormat.extractHeader = false;
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
  public void testEmptyFileStar() throws Exception {
    String fileName = "empty.csv";
    buildFile(fileName, emptyFile);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName, STAR)).rowSet();
    SingleRowSet expected = client.rowSetBuilder(columnsSchema())
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  /**
   * Test an empty CSV file without headers with SELECT coluns. Should return an
   * empty result set with schema as the columns array.
   *
   * @throws Exception if the test fails
   */

  @Test
  public void testEmptyFileColumns() throws Exception {
    String fileName = "empty.csv";
    buildFile(fileName, emptyFile);
    RowSet results = client.queryBuilder().sql(makeStatement(fileName, COLUMNS)).rowSet();
    SingleRowSet expected = client.rowSetBuilder(columnsSchema())
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(results);
  }

  String validFile[] = {
      "10,foo,bar"
  };

  @Test
  public void testValidFileStar() throws Exception {
    String fileName = "valid.csv";
    buildFile(fileName, validFile);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName, STAR)).rowSet();
    SingleRowSet expected = client.rowSetBuilder(columnsSchema())
        .add(new Object[] {new String[] {"10", "foo", "bar"}})
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  /**
   * Test an empty CSV file without headers with SELECT columns. Should return an
   * empty result set with schema as the columns array.
   *
   * @throws Exception if the test fails
   */

  @Test
  public void testValidFileColumns() throws Exception {
    String fileName = "valid.csv";
    buildFile(fileName, validFile);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName, COLUMNS)).rowSet();
    SingleRowSet expected = client.rowSetBuilder(columnsSchema())
        .add(new Object[] {new String[] {"10", "foo", "bar"}})
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  /**
   * Test <tt>SELECT a ... </tt>
   * Fails as expected.
   */
  @Test
  public void testValidFileSelection() throws IOException {
    String fileName = "valid.csv";
    buildFile(fileName, validFile);
    try {
      client.queryBuilder().sql(makeStatement(fileName, "a")).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("must have name 'columns' or must be plain '*'"));
    }
  }

  /**
   * Test <tt>SELECT columns, a ... </tt>
   * Should fail but actually succeeds.
   * @throws Exception
   */
  @Test
  public void testColumnsAndSelect() throws Exception {
    String fileName = "valid.csv";
    buildFile(fileName, validFile);
    String sql = makeStatement(fileName, COLUMNS + ", a");
//    System.out.println(sql);
//    System.out.println(client.queryBuilder().sql(sql).explainJson());
//    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    // DRILL-5555: Should fail with unsupported operation error
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .addNullable("a", MinorType.INT)
        .build();
    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add(new String[] {"10", "foo", "bar"}, null)
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
    String fileName = "valid.csv";
    buildFile(fileName, validFile);
    String sql = makeStatement(fileName, STAR + ", a");
//    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    // DRILL-5555: Should fail with unsupported operation error
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .addNullable("a", MinorType.INT)
        .build();
    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add(new String[] {"10", "foo", "bar"}, null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  /**
   * Test <tt>SELECT *, columns ... </tt>
   * Should fail but actually succeeds.
   * @throws Exception
   */
  @Test
  public void testStarAndColumns() throws Exception {
    String fileName = "valid.csv";
    buildFile(fileName, validFile);
    String sql = makeStatement(fileName, STAR + ", " + COLUMNS);
//    System.out.println(client.queryBuilder().sql(sql).explainJson());
//    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    // DRILL-5553: this should fail.
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .addArray("columns0", MinorType.VARCHAR)
        .build();
    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add(new String[] {"10", "foo", "bar"}, new String[] {"10", "foo", "bar"})
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  /**
   * Test <tt>SELECT *, * ... </tt>
   * Should fail but actually succeeds.
   * @throws Exception
   */
  @Test
  public void testStarAndStar() throws Exception {
    String fileName = "valid.csv";
    buildFile(fileName, validFile);
    String sql = makeStatement(fileName, STAR + ", " + STAR);
//    System.out.println(client.queryBuilder().sql(sql).explainJson());
//    client.queryBuilder().sql(sql).printCsv();
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    // DRILL-5556: this should fail.
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .addArray("columns0", MinorType.VARCHAR)
        .build();
    RowSet expected = client.rowSetBuilder(expectedSchema)
        .add(new String[] {"10", "foo", "bar"}, new String[] {"10", "foo", "bar"})
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  /**
   * Test <tt>SELECT columns AS cols ...</tt>
   */
  @Test
  public void testColumnsAs() throws Exception {
    String fileName = "valid.csv";
    buildFile(fileName, validFile);
    String sql = makeStatement(fileName, COLUMNS + " AS cols");
//    System.out.println(client.queryBuilder().sql(sql).explainJson());
    RowSet actual = client.queryBuilder().sql(sql).rowSet();
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("cols", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = client.rowSetBuilder(expectedSchema)
        .add(new Object[] {new String[] {"10", "foo", "bar"}})
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(actual);
  }

  private BatchSchema columnsSchema() {
    return new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .build();
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
}
