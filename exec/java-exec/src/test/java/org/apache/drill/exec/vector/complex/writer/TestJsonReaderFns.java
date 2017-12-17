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
package org.apache.drill.exec.vector.complex.writer;

import static org.apache.drill.test.TestBuilder.listOf;
import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests of Drill selected Drill functions using JSON as an input source.
 * (Split from the original <tt>TestJsonReader</tt>.) Relative to the Drill 1.12
 * version, the tests here:
 * <ul>
 * <li>Are rewritten to use the {@link ClusterFixture} framework.</li>
 * <li>Add data verification where missing.</li>
 * <li>Clean up handling of session options.</li>
 * </ul>
 * When running tests, consider these to be secondary. First verify the core
 * JSON reader itself (using {@link TestJsonReader}), then run these tests to
 * ensure vectors populated by JSON work with downstream functions.
 */

public class TestJsonReaderFns extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("store", "json"));
    dirTestWatcher.copyResourceToRoot(Paths.get("vector","complex", "writer"));
  }

  @Test
  public void hack() throws Exception {
    testSplitAndTransferFailure();
  }

  private RowSet runTest(String sql) {
    try {
      return client.queryBuilder().sql(sql).rowSet();
    } catch (RpcException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  public void testEmptyList() throws Exception {

    String sql = "select count(a[0]) as ct from dfs.`store/json/emptyLists`";

    RowSet results = runTest(sql);
    BatchSchema schema = new SchemaBuilder()
        .add("ct", MinorType.BIGINT)
        .build();

    RowSet expected = client.rowSetBuilder(schema)
        .addRow(6L)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  // Expansion of former testRepeatedCount()

  @Test
  public void testRepeatedCountStr() throws Exception {
    RowSet results = runTest("select repeated_count(str_list) from cp.`store/json/json_basic_repeated_varchar.json`");
    RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(5)
        .addSingleCol(1)
        .addSingleCol(3)
        .addSingleCol(1)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedCountInt() throws Exception {
    RowSet results = runTest("select repeated_count(INT_col) from cp.`parquet/alltypes_repeated.json`");
    RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(12)
        .addSingleCol(4)
        .addSingleCol(4)
        .addSingleCol(4)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedCountFloat4() throws Exception {
    RowSet results = runTest("select repeated_count(FLOAT4_col) from cp.`parquet/alltypes_repeated.json`");
    RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(7)
        .addSingleCol(4)
        .addSingleCol(4)
        .addSingleCol(4)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedCountVarchar() throws Exception {
    RowSet results = runTest("select repeated_count(VARCHAR_col) from cp.`parquet/alltypes_repeated.json`");
    RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(4)
        .addSingleCol(3)
        .addSingleCol(3)
        .addSingleCol(3)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedCountBit() throws Exception {
    RowSet results = runTest("select repeated_count(BIT_col) from cp.`parquet/alltypes_repeated.json`");
    RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(7)
        .addSingleCol(7)
        .addSingleCol(5)
        .addSingleCol(3)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  private BatchSchema countSchema() {
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("EXPR$0", MinorType.INT)
        .build();
    return expectedSchema;
  }

  @Test
  public void testSplitAndTransferFailure() throws Exception {
    final String testVal = "a string";
    testBuilder()
        .sqlQuery("select flatten(config) as flat from cp.`store/json/null_list.json`")
        .ordered()
        .baselineColumns("flat")
        .baselineValues(listOf())
        .baselineValues(listOf(testVal))
        .go();
  }


  // Reimplementation of testRepeatedContains()

  @Test
  public void testRepeatedContainsStr() throws Exception {
    RowSet results = runTest("select repeated_contains(str_list, 'asdf') from cp.`store/json/json_basic_repeated_varchar.json`");
    RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(5) // WRONG! Should be 1. See DRILL-6034
        .addSingleCol(0)
        .addSingleCol(0)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsInt() throws Exception {
    RowSet results = runTest("select repeated_contains(INT_col, -2147483648) from cp.`parquet/alltypes_repeated.json`");
    RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(1)
        .addSingleCol(0)
        .addSingleCol(0)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsFloat4() throws Exception {
    RowSet results = runTest("select repeated_contains(FLOAT4_col, -1000000000000.0) from cp.`parquet/alltypes_repeated.json`");
    RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(1)
        .addSingleCol(0)
        .addSingleCol(0)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsVarchar() throws Exception {
    RowSet results = runTest("select repeated_contains(VARCHAR_col, 'qwerty' ) from cp.`parquet/alltypes_repeated.json`");
    RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(1)
        .addSingleCol(0)
        .addSingleCol(0)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsBitTrue() throws Exception {
    RowSet results = runTest("select repeated_contains(BIT_col, true) from cp.`parquet/alltypes_repeated.json`");
    RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(11) // WRONG! Should be 1. See DRILL-6034
        .addSingleCol(0)
        .addSingleCol(0)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsBitFalse() throws Exception {
    RowSet results = runTest("select repeated_contains(BIT_col, false) from cp.`parquet/alltypes_repeated.json`");
    RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(7) // WRONG! Should be 1. See DRILL-6034
        .addSingleCol(0)
        .addSingleCol(0)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  private BatchSchema bitCountSchema() {
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("EXPR$0", MinorType.BIT)
        .build();
    return expectedSchema;
  }

  @Test
  public void drill_4479() throws Exception {
    File table_dir = dirTestWatcher.makeTestTmpSubDir(Paths.get("drill_4479"));
    table_dir.mkdir();
    BufferedWriter os = new BufferedWriter(new FileWriter(new File(table_dir, "mostlynulls.json")));
    // Create an entire batch of null values for 3 columns
    for (int i = 0 ; i < JSONRecordReader.DEFAULT_ROWS_PER_BATCH; i++) {
      os.write("{\"a\": null, \"b\": null, \"c\": null}");
    }
    // Add a row with {bigint,  float, string} values
    os.write("{\"a\": 123456789123, \"b\": 99.999, \"c\": \"Hello World\"}");
    os.close();

    try {
      client.alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);
      testBuilder()
        .sqlQuery("select c, count(*) as cnt from dfs.tmp.drill_4479 t group by c")
        .ordered()
        .baselineColumns("c", "cnt")
        .baselineValues(null, 4096L)
        .baselineValues("Hello World", 1L)
        .go();

      testBuilder()
        .sqlQuery("select a, b, c, count(*) as cnt from dfs.tmp.drill_4479 t group by a, b, c")
        .ordered()
        .baselineColumns("a", "b", "c", "cnt")
        .baselineValues(null, null, null, 4096L)
        .baselineValues("123456789123", "99.999", "Hello World", 1L)
        .go();

      testBuilder()
        .sqlQuery("select max(a) as x, max(b) as y, max(c) as z from dfs.tmp.drill_4479 t")
        .ordered()
        .baselineColumns("x", "y", "z")
        .baselineValues("123456789123", "99.999", "Hello World")
        .go();

    } finally {
      client.resetSession(ExecConstants.JSON_ALL_TEXT_MODE);
    }
  }

  @Test
  public void testFlattenEmptyArrayWithAllTextMode() throws Exception {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dirTestWatcher.getRootDir(), "empty_array_all_text_mode.json")))) {
      writer.write("{ \"a\": { \"b\": { \"c\": [] }, \"c\": [] } }");
    }

    try {
      String query = "select flatten(t.a.b.c) as c from dfs.`empty_array_all_text_mode.json` t";

      client.alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .expectsEmptyResultSet()
        .go();

      client.alterSession(ExecConstants.JSON_ALL_TEXT_MODE, false);
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .expectsEmptyResultSet()
        .go();

    } finally {
      client.resetSession(ExecConstants.JSON_ALL_TEXT_MODE);
    }
  }

  @Test // DRILL-5521
  public void testKvgenWithUnionAll() throws Exception {
    String fileName = "map.json";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dirTestWatcher.getRootDir(), fileName)))) {
      writer.write("{\"rk\": \"a\", \"m\": {\"a\":\"1\"}}");
    }

    String query = String.format("select kvgen(m) as res from (select m from dfs.`%s` union all " +
        "select convert_from('{\"a\" : null}' ,'json') as m from (values(1)))", fileName);
    QuerySummary result = client.queryBuilder().sql(query).run();
    assertEquals("Row count should match", 2, result.recordCount());
  }

  @Test // DRILL-4264
  public void testFieldWithDots() throws Exception {
    String fileName = "table.json";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dirTestWatcher.getRootDir(), fileName)))) {
      writer.write("{\"rk.q\": \"a\", \"m\": {\"a.b\":\"1\", \"a\":{\"b\":\"2\"}, \"c\":\"3\"}}");
    }

    testBuilder()
      .sqlQuery("select t.m.`a.b` as a,\n" +
        "t.m.a.b as b,\n" +
        "t.m['a.b'] as c,\n" +
        "t.rk.q as d,\n" +
        "t.`rk.q` as e\n" +
        "from dfs.`%s` t", fileName)
      .unOrdered()
      .baselineColumns("a", "b", "c", "d", "e")
      .baselineValues("1", "2", "1", null, "a")
      .go();
  }
}
