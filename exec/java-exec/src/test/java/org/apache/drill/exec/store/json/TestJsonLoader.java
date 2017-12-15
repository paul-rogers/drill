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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.List;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.drill.exec.physical.impl.scan.TestScanOperatorExec.BasicScanOpFixture;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.JsonLoaderImpl.JsonOptions;
import org.apache.drill.exec.store.easy.json.JsonLoaderImpl.TypeNegotiator;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.drill.test.rowSet.RowSetUtilities.intArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.doubleArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleMap;

import com.google.common.base.Joiner;

/**
 * Tests the JSON loader itself: the various syntax combinations that the loader understands
 * or rejects. Assumes that the JSON parser itself works, and that the underlying result set
 * loader properly handles column types, projections and so on.
 */

public class TestJsonLoader extends SubOperatorTest {

  private class JsonTester {
    public OptionBuilder loaderOptions = new OptionBuilder();
    private final JsonOptions options;
    private ResultSetLoader tableLoader;

    public JsonTester(JsonOptions options) {
      this.options = options;
    }

    public JsonTester() {
      this(new JsonOptions());
    }

    public RowSet parseFile(String resourcePath) {
      try {
        return parse(ClusterFixture.getResource(resourcePath));
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public RowSet parse(String json) {
      tableLoader = new ResultSetLoaderImpl(fixture.allocator(),
          loaderOptions.build());
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
  @Ignore("Turns out all text can't handle structures")
  public void testRootTupleAllTextComplex() {
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
        .addRow("3", "Wilma", "500.00", null)
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
      "{id: 1, name: \"Fred\", balance: 100.0, extra: true}\n" +
      "{id: 2, name: \"Barney\", extra: 10}\n" +
      "{id: 3, name: \"Wilma\", balance: 500.00, extra: null}\n" +
      "{id: 4, name: \"Betty\", balance: 12.00, extra: \"Hello\"}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.VARCHAR)
        .addNullable("name", MinorType.VARCHAR)
        .addNullable("balance", MinorType.VARCHAR)
        .addNullable("extra", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow("1", "Fred", "100.0", "true")
        .addRow("2", "Barney", null, "10")
        .addRow("3", "Wilma", "500.00", null)
        .addRow("4", "Betty", "12.00", "Hello")
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
        .addRow(intArray(1, 0))
        .addRow(intArray())
        .addRow(intArray())
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
        .addRow(longArray(1L, 100L))
        .addRow(longArray())
        .addRow(longArray())
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
        .addRow(doubleArray(1D, 100D))
        .addRow(doubleArray())
        .addRow(doubleArray())
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
        .addRow(doubleArray(1.0, 100.0))
        .addRow(doubleArray())
        .addRow(doubleArray())
        .addRow(doubleArray(12.5, 123.45))
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
        .addSingleCol(strArray("", "foo"))
        .addSingleCol(strArray())
        .addSingleCol(strArray())
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
        "{a: [\"foo\", true, false, 10, null, 20.0] }";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray("foo", "true", "false", "10", "", "20.0"))
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
  public void testDeferredScalarNullAsText() {
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
  public void testDeferredScalarNullAsType() {
    String json = "{a: null} {a: null} {a: null} {a: 10} {a: 20}";
    ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    JsonOptions options = new JsonOptions();
    options.context = "test Json";
    options.typeNegotiator = new TypeNegotiator() {

      // Only one field.

      @Override
      public MajorType typeOf(List<String> path) {
        assertEquals(1, path.size());
        assertEquals("a", path.get(0));
        return Types.optional(MinorType.BIGINT);
      }
    };
    JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    tableLoader.startBatch();
    for (int i = 0; i < 2; i++) {
      assertTrue(loader.next());
    }
    loader.endBatch();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(tableLoader.harvest()));

    // Second batch, read remaining records as given type.

    tableLoader.startBatch();
    while (loader.next()) {
      // No op
    }
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(10L)
        .addSingleCol(20L)
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
  public void testNestedDeferredScalarNullAsType() {
    String json =
        "{a: {b: null, c: null, d: null}, e: null}\n" +
        "{a: {b: null, c: null, d: null}, e: null}\n" +
        "{a: {b: null, c: null, d: null}, e: null}\n" +
//        "{a: {b: 10, c: \"fred\", d: [1.5, 2.5]}, e: {f: 30}}\n";
        "{a: {b: 10, c: \"fred\", d: [1.5, 2.5]}, e: 10.25}\n";
    ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    JsonOptions options = new JsonOptions();
    options.context = "test Json";
    options.typeNegotiator = new TypeNegotiator() {

      // Find types for three fields.

      @Override
      public MajorType typeOf(List<String> path) {
        String name = Joiner.on(".").join(path);
        switch (name) {
        case "a.b": return Types.optional(MinorType.BIGINT);
        case "a.c": return Types.optional(MinorType.VARCHAR);
        case "a.d": return Types.repeated(MinorType.FLOAT8);
        case "e": return null;
        default:
          fail();
          return null;
        }
      }
    };
    JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    tableLoader.startBatch();
    for (int i = 0; i < 2; i++) {
      assertTrue(loader.next());
    }
    loader.endBatch();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addMap("a")
          .addNullable("b", MinorType.BIGINT)
          .addNullable("c", MinorType.VARCHAR)
          .addArray("d", MinorType.FLOAT8)
          .buildMap()
        .addNullable("e", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(mapValue(null, null, doubleArray()), null)
        .addRow(mapValue(null, null, doubleArray()), null)
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(tableLoader.harvest()));

    // Second batch, read remaining records as given type.

    tableLoader.startBatch();
    while (loader.next()) {
      // No op
    }
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(mapValue(null, null, doubleArray()), null)
        .addRow(mapValue(10L, "fred", doubleArray(1.5, 2.5)), "10.25")
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
        .addSingleCol(longArray())
        .addSingleCol(longArray())
        .addSingleCol(longArray(10L, 20L))
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  @Test
  public void testDeferredArrayAsText() {
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
        .addSingleCol(strArray())
        .addSingleCol(strArray())
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(tableLoader.harvest()));

    // Second batch, read remaining records as text mode.

    tableLoader.startBatch();
    while (loader.next()) {
      // No op
    }
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray())
        .addSingleCol(strArray("10", "20"))
        .addSingleCol(strArray("foo", "bar"))
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
  public void testDeferredArrayAsType() {
    String json =
        "{a: [], b: []}\n" +
        "{a: null, b: null}\n" +
        "{a: [], b: []}\n" +
        "{a: [10, 20], b: [10.5, \"fred\"]}\n" +
        "{a: [30, 40], b: [null, false]}";
    ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    JsonOptions options = new JsonOptions();
    options.context = "test Json";
    options.typeNegotiator = new TypeNegotiator() {
      @Override
      public MajorType typeOf(List<String> path) {
        assertEquals(1, path.size());
        switch (path.get(0)) {
        case "a": return Types.repeated(MinorType.BIGINT);

        // Note: type for b is optional, not compatible with
        // an array, so is ignored and text model will be used.

        case "b": return Types.optional(MinorType.BIGINT);
        default:
          fail();
          return null;
        }
      }
    };
    JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    tableLoader.startBatch();
    for (int i = 0; i < 2; i++) {
      assertTrue(loader.next());
    }
    loader.endBatch();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .addArray("b", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(longArray(), strArray())
        .addRow(longArray(), strArray())
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(tableLoader.harvest()));

    // Second batch, read remaining records as long.

    tableLoader.startBatch();
    while (loader.next()) {
      // No op
    }
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(longArray(), strArray())
        .addRow(longArray(10L, 20L), strArray("10.5", "fred"))
        .addRow(longArray(30L, 40L), strArray("", "false"))
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
        .addSingleCol(longArray())
        .addSingleCol(longArray())
        .addSingleCol(longArray(10L, 20L))
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
        .addRow(1L, mapValue("fred"))
        .addRow(2L, mapValue("barney"))
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
        .addRow(1L, mapValue("fred"))
        .addRow(2L, mapValue("barney"))
        .addRow(3L, mapValue((String) null))
        .addRow(4L, mapValue((String) null))
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
        .addRow(1L, mapValue("fred"), mapArray(
            mapValue(1001L, "closed"),
            mapValue(1002L, "open")))
        .addRow(2L, mapValue("barney"), mapArray())
        .addRow(3L, mapValue("wilma"), mapArray())
        .addRow(4L, mapValue("betty"), mapArray())
        .addRow(5L, mapValue("pebbles"), mapArray(
            mapValue(1003L, "canceled")))
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
        .addSingleCol(mapArray())
        .addSingleCol(mapArray())
        .addSingleCol(mapArray(
            mapValue("fred"), mapValue("barney")))
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
        .setProjection(RowSetTestUtils.projectList("a"))
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

  @Test
  public void testArrays() {
    String json =
        "{a: null, b: []}\n" +
        "{a: [true, false], b: [10, 20], c: [10.5, 12.25], d: [\"foo\", \"bar\"]}";
    ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    JsonOptions options = new JsonOptions();
    options.context = "test Json";
    JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    tableLoader.startBatch();
    while (loader.next()) {
      // No op
    }
    loader.endBatch();
    RowSet result = fixture.wrap(tableLoader.harvest());

    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.TINYINT)
        .addArray("b", MinorType.BIGINT)
        .addArray("c", MinorType.FLOAT8)
        .addArray("d", MinorType.VARCHAR)
        .build();

    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(intArray(), longArray(), doubleArray(), strArray())
        .addRow(intArray(1, 0), longArray(10L, 20L), doubleArray(10.5, 12.25), strArray("foo", "bar"))
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
   * Drill supports 1-D arrays using repeated types. Drill does not
   * support 2-D or higher arrays. Instead, Drill reverts to "text
   * mode" for such arrays, capturing them as JSON text, allowing the
   * client to interpret them.
   */

  @Test
  @Ignore("All text mode does not handle structures")
  public void testArraysOld() {
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
   * Test the JSON parser's limited recovery abilities.
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

  /**
   * Test based on TestJsonReader.testAllTextMode
   * Verifies the complex case of an array of maps that contains an array of
   * strings (from all text mode). Also verifies projection.
   */

  @Test
  public void testMapSelect() {
    JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    JsonTester tester = new JsonTester(options);
    tester.loaderOptions.setProjection(RowSetTestUtils.projectList("field_5"));
    RowSet results = tester.parseFile("store/json/schema_change_int_to_string.json");
//    results.print();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addMapArray("field_5")
          .addArray("inner_list", MinorType.VARCHAR)
          .addArray("inner_list_2", MinorType.VARCHAR)
          .buildMap()
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(mapArray())
        .addSingleCol(mapArray(
            mapValue(strArray("1", "", "6"), strArray()),
            mapValue(strArray("3", "8"), strArray()),
            mapValue(strArray("12", "", "4", "null", "5"), strArray())))
        .addSingleCol(mapArray(
            mapValue(strArray("5", "", "6.0", "1234"), strArray()),
            mapValue(strArray("7", "8.0", "12341324"), strArray("1", "2", "2323.443e10", "hello there")),
            mapValue(strArray("3", "4", "5"), strArray("10", "11", "12"))))
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
    tester.close();
  }

  private static class TestJsonReader implements ManagedReader<SchemaNegotiator> {

    private ResultSetLoader tableLoader;
    private String filePath;
    private InputStream stream;
    private JsonLoader jsonLoader;
    private JsonOptions options;

    public TestJsonReader(String filePath, JsonOptions options) {
      this.filePath = filePath;
      this.options = options;
    }

    @Override
    public boolean open(SchemaNegotiator negotiator) {
      stream = new BufferedInputStream(getClass().getResourceAsStream(filePath));
      tableLoader = negotiator.build();
      jsonLoader = new JsonLoaderImpl(stream, tableLoader.writer(), options);
      return true;
    }

    @Override
    public boolean next() {
      boolean more = true;
      while (! tableLoader.writer().isFull()) {
        if (! jsonLoader.next()) {
          more = false;
          break;
        }
      }
      jsonLoader.endBatch();
      return more;
    }

    @Override
    public void close() {
      try {
        stream.close();
      } catch (IOException e) {
        // Ignore;
      }
    }

  }

  /**
   * Test the case where the reader does not play the "first batch contains
   * only schema" game, and instead returns data. The Scan operator will
   * split the first batch into two: one with schema only, another with
   * data.
   */

  @Test
  public void testScanOperator() {

    BasicScanOpFixture opFixture = new BasicScanOpFixture();
    JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    opFixture.addReader(new TestJsonReader("/store/json/schema_change_int_to_string.json", options));
    opFixture.setProjection("field_3", "field_5");
    ScanOperatorExec scanOp = opFixture.build();

    assertTrue(scanOp.buildSchema());
    RowSet result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
//  result.print();
    result.clear();
    assertTrue(scanOp.next());
    result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
//    result.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("field_3")
          .addNullable("inner_1", MinorType.VARCHAR)
          .addNullable("inner_2", MinorType.VARCHAR)
          .addMapArray("inner_3")
            .addNullable("inner_object_field_1", MinorType.VARCHAR)
            .buildMap()
          .buildMap()
        .addMapArray("field_5")
          .addArray("inner_list", MinorType.VARCHAR)
          .addArray("inner_list_2", MinorType.VARCHAR)
          .buildMap()
        .buildSchema();

    RowSetUtilities.strArray();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(mapValue(null, null, mapArray()),
                mapArray())
        .addRow(mapValue("2", null, mapArray()),
                mapArray(
                  mapValue(strArray("1", "", "6"), strArray()),
                  mapValue(strArray("3", "8"), strArray()),
                  mapValue(strArray("12", "", "4", "null", "5"), strArray())))
        .addRow(mapValue("5", "3", mapArray(singleMap(null), singleMap("10"))),
            mapArray(
                mapValue(strArray("5", "", "6.0", "1234"), strArray()),
                mapValue(strArray("7", "8.0", "12341324"),
                         strArray("1", "2", "2323.443e10", "hello there")),
                mapValue(strArray("3", "4", "5"), strArray("10", "11", "12"))))
        .build();

    new RowSetComparison(expected)
      .verifyAndClearAll(result);
    opFixture.close();
  }

  @Test
  public void testScanProject1() {

    BasicScanOpFixture opFixture = new BasicScanOpFixture();
    JsonOptions options = new JsonOptions();
    opFixture.addReader(new TestJsonReader("/store/json/schema_change_int_to_string.json", options));
    opFixture.setProjection("field_3.inner_1", "field_3.inner_2");
    ScanOperatorExec scanOp = opFixture.build();

    assertTrue(scanOp.buildSchema());
    RowSet result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
//    result.print();
    assertTrue(scanOp.next());
    result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
    result.print();
    result.clear();
    opFixture.close();
  }

  @Test
  public void testScanProjectMapSubset() {

    BasicScanOpFixture opFixture = new BasicScanOpFixture();
    JsonOptions options = new JsonOptions();
    opFixture.addReader(new TestJsonReader("/store/json/schema_change_int_to_string.json", options));
    opFixture.setProjection("field_3.inner_1", "field_3.inner_2");
    ScanOperatorExec scanOp = opFixture.build();

    assertTrue(scanOp.buildSchema());
    RowSet result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
//    result.print();
    assertTrue(scanOp.next());
    result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
//    result.print();

    BatchSchema schema = new SchemaBuilder()
        .addMap("field_3")
          .addNullable("inner_1", MinorType.BIGINT)
          .addNullable("inner_2", MinorType.BIGINT)
          .buildMap()
        .build();

    RowSet expected = fixture.rowSetBuilder(schema)
        .addSingleCol(mapValue(null, null))
        .addSingleCol(mapValue(2L, null))
        .addSingleCol(mapValue(5L, 3L))
        .build();
    new RowSetComparison(expected).verifyAndClearAll(result);
    scanOp.close();
  }

  @Test
  public void testScanProjectMapArraySubsetAndNull() {

    BasicScanOpFixture opFixture = new BasicScanOpFixture();
    JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    opFixture.addReader(new TestJsonReader("/store/json/schema_change_int_to_string.json", options));
    opFixture.setProjection("field_5.inner_list", "field_5.dummy");
    opFixture.setNullType(Types.optional(MinorType.VARCHAR));
    ScanOperatorExec scanOp = opFixture.build();

    assertTrue(scanOp.buildSchema());
    RowSet result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
//    result.print();
    assertTrue(scanOp.next());
    result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
//    result.print();

    BatchSchema schema = new SchemaBuilder()
        .addMapArray("field_5")
          .addArray("inner_list", MinorType.VARCHAR)
          .addNullable("dummy", MinorType.VARCHAR)
          .buildMap()
        .build();

    RowSet expected = fixture.rowSetBuilder(schema)
        .addSingleCol(mapArray())
        .addSingleCol(mapArray(
            mapValue(strArray("1", "", "6"), null),
            mapValue(strArray("3", "8"), null),
            mapValue(strArray("12", "", "4", "null", "5"), null)))
        .addSingleCol(mapArray(
            mapValue(strArray("5", "", "6.0", "1234"), null),
            mapValue(strArray("7" ,"8.0", "12341324"), null),
            mapValue(strArray("3", "4", "5"), null)))
        .build();
    new RowSetComparison(expected).verifyAndClearAll(result);
    scanOp.close();
  }

  @Test
  public void testScanProject() {

    BasicScanOpFixture opFixture = new BasicScanOpFixture();
    JsonOptions options = new JsonOptions();
    opFixture.addReader(new TestJsonReader("/store/json/schema_change_int_to_string.json", options));
    opFixture.setProjection("field_1", "field_3.inner_1", "field_3.inner_2", "field_4.inner_1",
        "non_existent_at_root", "non_existent.nested.field");
    opFixture.setNullType(Types.optional(MinorType.VARCHAR));
    ScanOperatorExec scanOp = opFixture.build();

    assertTrue(scanOp.buildSchema());
    RowSet result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
//    result.print();
    assertTrue(scanOp.next());
    result = fixture.wrap(scanOp.batchAccessor().getOutgoingContainer());
//    result.print();

    // Projects all columns (since the revised scan operator handles missing-column
    // projection.) Note that the result includes two batches, including the first empty
    // batch.

    BatchSchema schema = new SchemaBuilder()
        .addArray("field_1", MinorType.BIGINT)
        .addMap("field_3")
          .addNullable("inner_1", MinorType.BIGINT)
          .addNullable("inner_2", MinorType.BIGINT)
          .buildMap()
        .addMap("field_4")
          .addArray("inner_1", MinorType.BIGINT)
          .buildMap()
        .addNullable("non_existent_at_root", MinorType.VARCHAR)
        .addMap("non_existent")
          .addMap("nested")
            .addNullable("field", MinorType.VARCHAR)
            .buildMap()
          .buildMap()
        .build();

    Object nullMap = singleMap(singleMap(null));
    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow(longArray(1L), mapValue(null, null), mapValue(longArray()), null, nullMap )
        .addRow(longArray(5L), mapValue(2L, null), mapValue(longArray(1L, 2L, 3L)), null, nullMap)
        .addRow(longArray(5L, 10L, 15L), mapValue(5L, 3L), mapValue(longArray(4L, 5L, 6L)), null, nullMap)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(result);
    scanOp.close();
  }

  // TODO: Lists
  // TODO: Union support
}
