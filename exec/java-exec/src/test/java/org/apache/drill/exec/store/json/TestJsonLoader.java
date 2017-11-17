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
package org.apache.drill.exec.store.json;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.JsonLoaderImpl.JsonOptions;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

/**
 * Tests the JSON loader itself: the various syntax combinations that the loader understands
 * or rejects. Assumes that the JSON parser itself works, and that the underlying result set
 * loader properly handles column types, projections and so on.
 */

public class TestJsonLoader extends SubOperatorTest {

  private class JsonTester {
    private final JsonOptions options;
    private ResultSetLoader tableLoader;

    public JsonTester(JsonOptions options) {
      this.options = options;
    }

    public JsonTester() {
      this(new JsonOptions());
    }

    public RowSet parse(String json) {
      tableLoader = new ResultSetLoaderImpl(fixture.allocator());
      InputStream inStream = new
          ReaderInputStream(new StringReader(json));
      options.context = "test Json";
      JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);
      tableLoader.startBatch();
      while (loader.next()) {
        // No op
      }
      try {
        inStream.close();
      } catch (IOException e) {
        fail();
      }
      loader.close();
      return fixture.wrap(tableLoader.harvest());
    }

    public void close() {
      tableLoader.close();
    }
  }

  @Test
  public void testEmpty() {
    JsonTester tester = new JsonTester();
    RowSet results = tester.parse("");
    assertEquals(0, results.rowCount());
    results.clear();
    tester.close();
  }

  @Test
  public void testEmptyTuple() {
    JsonTester tester = new JsonTester();
    String json = "{} {} {}";
    RowSet results = tester.parse(json);
    assertEquals(3, results.rowCount());
    results.clear();
    tester.close();
  }

  @Test
  public void testBoolean() {
    JsonTester tester = new JsonTester();
    String json = "{a: true} {a: false} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.TINYINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1)
        .addRow(0)
        .addSingleCol(null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testInteger() {
    JsonTester tester = new JsonTester();
    String json = "{a: 0} {a: 100} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0L)
        .addRow(100L)
        .addSingleCol(null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testFloat() {
    JsonTester tester = new JsonTester();

    // Note: integers allowed after first float.

    String json = "{a: 0.0} {a: 100.5} {a: 5} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0D)
        .addRow(100.5D)
        .addRow(5D)
        .addSingleCol(null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testExtendedFloat() {
    JsonOptions options = new JsonOptions();
    options.allowNanInf = true;
    JsonTester tester = new JsonTester(options);
    String json =
        "{a: 0.0} {a: 100.5} {a: -200.5} {a: NaN}\n" +
        "{a: Infinity} {a: -Infinity} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0D)
        .addRow(100.5D)
        .addRow(-200.5D)
        .addRow(Double.NaN)
        .addRow(Double.POSITIVE_INFINITY)
        .addRow(Double.NEGATIVE_INFINITY)
        .addSingleCol(null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testIntegerAsFloat() {
    JsonOptions options = new JsonOptions();
    options.readNumbersAsDouble = true;
    JsonTester tester = new JsonTester(options);
    String json = "{a: 0} {a: 100} {a: null} {a: 123.45}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0D)
        .addRow(100D)
        .addSingleCol(null)
        .addRow(123.45D)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testString() {
    JsonTester tester = new JsonTester();
    String json = "{a: \"\"} {a: \"hi\"} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow("")
        .addRow("hi")
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testRootTuple() {
    JsonTester tester = new JsonTester();
    String json =
      "{id: 1, name: \"Fred\", balance: 100.0}\n" +
      "{id: 2, name: \"Barney\"}\n" +
      "{id: 3, name: \"Wilma\", balance: 500.00}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addNullable("name", MinorType.VARCHAR)
        .addNullable("balance", MinorType.FLOAT8)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, "Fred", 100.00D)
        .addRow(2L, "Barney", null)
        .addRow(3L, "Wilma", 500.0D)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testRootTupleAllText() {
    JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    JsonTester tester = new JsonTester(options);
    String json =
      "{id: 1, name: \"Fred\", balance: 100.0, extra: [\"a\",   \"\\\"b,\\\", said I\" ]}\n" +
      "{id: 2, name: \"Barney\", extra: {a:  10 , b:20}}\n" +
      "{id: 3, name: \"Wilma\", balance: 500.00, extra: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.VARCHAR)
        .addNullable("name", MinorType.VARCHAR)
        .addNullable("balance", MinorType.VARCHAR)
        .addNullable("extra", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow("1", "Fred", "100.0", "[\"a\", \"\\\"b,\\\", said I\"]")
        .addRow("2", "Barney", null, "{\"a\": 10, \"b\": 20}")
        .addRow("3", "Wilma", "500.00", "null")
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testBooleanArray() {
    JsonTester tester = new JsonTester();
    String json =
        "{a: [true, false]}\n" +
        "{a: []} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.TINYINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(new int[] {1, 0})
        .addRow(new int[] {})
        .addRow(new int[] {})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testIntegerArray() {
    JsonTester tester = new JsonTester();
    String json =
        "{a: [1, 100]}\n" +
        "{a: []} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(new long[] {1, 100})
        .addRow(new long[] {})
        .addRow(new long[] {})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testFloatArray() {
    JsonTester tester = new JsonTester();
    String json =
        "{a: [1.0, 100]}\n" +
        "{a: []} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.FLOAT8)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(new double[] {1, 100})
        .addRow(new double[] {})
        .addRow(new double[] {})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testIntegerArrayAsFloat() {
    JsonOptions options = new JsonOptions();
    options.readNumbersAsDouble = true;
    JsonTester tester = new JsonTester(options);
    String json =
        "{a: [1, 100]}\n" +
        "{a: []}\n" +
        "{a: null}\n" +
        "{a: [12.5, 123.45]}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.FLOAT8)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(new double[] {1, 100})
        .addRow(new double[] {})
        .addRow(new double[] {})
        .addRow(new double[] {12.5, 123.45})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testStringArray() {
    JsonTester tester = new JsonTester();
    String json =
        "{a: [\"\", \"foo\"]}\n" +
        "{a: []} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(new String[] {"", "foo"})
        .addSingleCol(new String[] {})
        .addSingleCol(new String[] {})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testAllTextArray() {
    JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    JsonTester tester = new JsonTester(options);
    String json =
        "{a: [\"foo\", true, false, 10, 20.0] }";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(new String[] {"foo", "true", "false", "10", "20.0"})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testRootArray() {
    JsonTester tester = new JsonTester();
    String json = "[{a: 0}, {a: 100}, {a: null}]";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0L)
        .addRow(100L)
        .addSingleCol(null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testRootArrayDisallowed() {
    JsonOptions options = new JsonOptions();
    options.skipOuterList = false;
    JsonTester tester = new JsonTester(options);
    String json = "[{a: 0}, {a: 100}, {a: null}]";
    expectError(tester, json);
  }

  private void expectError(String json) {
    JsonTester tester = new JsonTester();
    expectError(tester, json);
  }

  private void expectError(JsonTester tester, String json) {
    try {
      tester.parse(json);
      fail();
    } catch (UserException e) {
//      System.out.println(e.getMessage());
      // expected
      assertTrue(e.getMessage().contains("test Json"));
    }
    tester.close();
  }

  @Test
  public void testMissingEndObject() {
    expectError("{a: 0} {a: 100");
  }

  @Test
  public void testMissingValue() {
    expectError("{a: 0} {a: ");
  }

  @Test
  public void testMissingEndOuterArray() {
    expectError("[{a: 0}, {a: 100}");
  }

  @Test
  public void testNullInArray() {
    expectError("{a: [10, 20, null]}");
  }

  @Test
  public void testDeferredScalarNull() {
    JsonTester tester = new JsonTester();
    String json = "{a: null} {a: null} {a: 10}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .addRow(10L)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testDeferredScalarNullRealized() {
    String json = "{a: null} {a: null} {a: null} {a: 10} {a: \"foo\"}";
    ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    JsonOptions options = new JsonOptions();
    options.context = "test Json";
    JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    tableLoader.startBatch();
    for (int i = 0; i < 2; i++) {
      assertTrue(loader.next());
    }
    loader.endBatch();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(tableLoader.harvest()));

    // Second batch, read remaining records as text mode.

    tableLoader.startBatch();
    while (loader.next()) {
      // No op
    }
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol("10")
        .addSingleCol("foo")
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(tableLoader.harvest()));

    try {
      inStream.close();
    } catch (IOException e) {
      fail();
    }
    loader.close();
    tableLoader.close();
  }

  @Test
  public void testDeferredScalarArray() {
    JsonTester tester = new JsonTester();
    String json = "{a: []} {a: null} {a: [10, 20]}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(new long[] {})
        .addSingleCol(new long[] {})
        .addSingleCol(new long[] {10, 20})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testDeferredArrayRealized() {
    String json = "{a: []} {a: null} {a: []} {a: [10, 20]} {a: [\"foo\", \"bar\"]}";
    ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    JsonOptions options = new JsonOptions();
    options.context = "test Json";
    JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    tableLoader.startBatch();
    for (int i = 0; i < 2; i++) {
      assertTrue(loader.next());
    }
    loader.endBatch();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(new String[] {})
        .addSingleCol(new String[] {})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(tableLoader.harvest()));

    // Second batch, read remaining records as text mode.

    tableLoader.startBatch();
    while (loader.next()) {
      // No op
    }
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(new String[] {})
        .addSingleCol(new String[] {"10", "20"})
        .addSingleCol(new String[] {"foo", "bar"})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(tableLoader.harvest()));

    try {
      inStream.close();
    } catch (IOException e) {
      fail();
    }
    loader.close();
    tableLoader.close();
  }

  /**
   * Double deferral. First null causes a deferred null. Then,
   * the empty array causes a deferred array. Fianlly, the third
   * array announces the type.
   */

  @Test
  public void testDeferredNullToArray() {
    JsonTester tester = new JsonTester();
    String json = "{a: null} {a: []} {a: [10, 20]}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(new long[] {})
        .addSingleCol(new long[] {})
        .addSingleCol(new long[] {10, 20})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testNestedTuple() {
    JsonTester tester = new JsonTester();
    String json =
        "{id: 1, customer: { name: \"fred\" }}\n" +
        "{id: 2, customer: { name: \"barney\" }}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addMap("customer")
          .addNullable("name", MinorType.VARCHAR)
          .buildMap()
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, new Object[] {"fred"})
        .addRow(2L, new Object[] {"barney"})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testNullTuple() {
    JsonTester tester = new JsonTester();
    String json =
        "{id: 1, customer: {name: \"fred\"}}\n" +
        "{id: 2, customer: {name: \"barney\"}}\n" +
        "{id: 3, customer: null}\n" +
        "{id: 4}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addMap("customer")
          .addNullable("name", MinorType.VARCHAR)
          .buildMap()
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, new Object[] {"fred"})
        .addRow(2L, new Object[] {"barney"})
        .addRow(3L, new Object[] {null})
        .addRow(4L, new Object[] {null})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testTupleArray() {
    JsonTester tester = new JsonTester();
    String json =
        "{id: 1, customer: {name: \"fred\"}, orders: [\n" +
        "  {id: 1001, status: \"closed\"},\n" +
        "  {id: 1002, status: \"open\"}]}\n" +
        "{id: 2, customer: {name: \"barney\"}, orders: []}\n" +
        "{id: 3, customer: {name: \"wilma\"}, orders: null}\n" +
        "{id: 4, customer: {name: \"betty\"}}\n" +
        "{id: 5, customer: {name: \"pebbles\"}, orders: [\n" +
        "  {id: 1003, status: \"canceled\"}]}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addMap("customer")
          .addNullable("name", MinorType.VARCHAR)
          .buildMap()
        .addMapArray("orders")
          .addNullable("id", MinorType.BIGINT)
          .addNullable("status", MinorType.VARCHAR)
          .buildMap()
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, new Object[] {"fred"}, new Object[] {
            new Object[] {1001L, "closed"},
            new Object[] {1002L, "open"}})
        .addRow(2L, new Object[] {"barney"}, new Object[] {})
        .addRow(3L, new Object[] {"wilma"}, new Object[] {})
        .addRow(4L, new Object[] {"betty"}, new Object[] {})
        .addRow(5L, new Object[] {"pebbles"}, new Object[] {
            new Object[] {1003L, "canceled"}})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testDeferredTupleArray() {
    JsonTester tester = new JsonTester();
    String json = "{a: []} {a: null} {a: [{name: \"fred\"}, {name: \"barney\"}]}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addMapArray("a")
          .addNullable("name", MinorType.VARCHAR)
          .buildMap()
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(new Object[] {})
        .addSingleCol(new Object[] {})
        .addSingleCol(new Object[] {
            new Object[] {"fred"}, new Object[] {"barney"}})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  /**
   * Verify that non-projected maps are just "free-wheeled", the JSON loader
   * just seeks the matching end brace, ignoring content semantics.
   */

  @Test
  public void testNonProjected() {
    String json =
        "{a: 10, b: {c: 100, c: 200, d: {x: null, y: 30}}}\n" +
        "{a: 20, b: { }}\n" +
        "{a: 30, b: null}\n" +
        "{a: 40, b: {c: \"foo\", d: [{}, {x: {}}]}}\n" +
        "{a: 50, b: [{c: 100, c: 200, d: {x: null, y: 30}}, 10]}\n" +
        "{a: 60, b: []}\n" +
        "{a: 70, b: null}\n" +
        "{a: 80, b: [\"foo\", [{}, {x: {}}]]}\n" +
        "{a: 90, b: 55.5} {a: 100, b: 10} {a: 110, b: \"foo\"}";
    ResultSetOptions rsOptions = new OptionBuilder()
        .setProjection(ScanTestUtils.projectList("a"))
        .build();
    ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator(), rsOptions);
    InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    JsonOptions options = new JsonOptions();
    options.context = "test Json";
    JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    tableLoader.startBatch();
    while (loader.next()) {
      // No op
    }
    loader.endBatch();
    RowSet result = fixture.wrap(tableLoader.harvest());

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    assertTrue(expectedSchema.isEquivalent(result.batchSchema()));
    RowSetReader reader = result.reader();
    for (int i = 10; i <= 110; i += 10) {
      assertTrue(reader.next());
      assertEquals(i, reader.scalar(0).getLong());
    }
    assertFalse(reader.next());
    result.clear();

    try {
      inStream.close();
    } catch (IOException e) {
      fail();
    }
    loader.close();
    tableLoader.close();
  }

  /**
   * Drill supports 1-D arrays using repeated types. Drill does not
   * support 2-D or higher arrays. Instead, Drill reverts to "text
   * mode" for such arrays, capturing them as JSON text, allowing the
   * client to interpret them.
   */

  @Test
  public void testArrays() {
    String oneDArray = "[[1, 2], [3, 4]]";
    String twoDArray = "[[[1, 2], [3, 4]], [[5, 6], [7, 8]]]";
    String json =
        "{a: [10, 11]," +
        " b: " + oneDArray + "," +
        " c: " + twoDArray + "}\n" +

        // 2- and 3-D arrays are all text. So, allow changes
        // to cardinality.

        "{a: [20, 21]," +
        " b: " + twoDArray + "," +
        " c: " + oneDArray + "}";
    ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    JsonOptions options = new JsonOptions();
    options.context = "test Json";
    JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    tableLoader.startBatch();
    while (loader.next()) {
      // No op
    }
    loader.endBatch();
    RowSet result = fixture.wrap(tableLoader.harvest());

    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR)
        .addNullable("c", MinorType.VARCHAR)
        .build();

    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(new long[] {10, 11}, oneDArray, twoDArray)
        .addRow(new long[] {20, 21}, twoDArray, oneDArray)
        .build();

    new RowSetComparison(expected)
      .verifyAndClearAll(result);

    try {
      inStream.close();
    } catch (IOException e) {
      fail();
    }
    loader.close();
    tableLoader.close();
  }

  /**
   * Test the JSON parser's limited recover abilities.
   *
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-4653">DRILL-4653</a>
   */

  @Test
  public void testErrorRecovery() {
    JsonOptions options = new JsonOptions();
    options.skipMalformedRecords = true;
    JsonTester tester = new JsonTester(options);
    String json = "{\"a: 10}\n{a: 20}\n{a: 30}";
    RowSet results = tester.parse(json);

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
//        .addRow(20L) // Recovery will eat the second record.
        .addRow(30L)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  /**
   * Test handling of unrecoverable parse errors. This test must change if
   * we resolve DRILL-5953 and allow better recovery.
   *
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-5953">DRILL-5953</a>
   */

  @Test
  public void testUnrecoverableError() {
    JsonOptions options = new JsonOptions();
    options.skipMalformedRecords = true;
    JsonTester tester = new JsonTester(options);
    expectError(tester, "{a: }\n{a: 20}\n{a: 30}");
  }

  // TODO: Lists
  // TODO: Union support
}
