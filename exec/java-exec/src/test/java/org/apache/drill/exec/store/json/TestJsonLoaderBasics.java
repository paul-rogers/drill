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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.Test;

import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;

/**
 * Tests the JSON loader itself: the various syntax combinations that the loader understands
 * or rejects. Assumes that the JSON parser itself works, and that the underlying result set
 * loader properly handles column types, projections and so on.
 */

public class TestJsonLoaderBasics extends BaseTestJsonLoader {

  @Test
  public void testEmpty() {
    JsonTester tester = jsonTester();
    RowSet results = tester.parse("");
    assertEquals(0, results.rowCount());
    results.clear();
    tester.close();
  }

  @Test
  public void testEmptyTuple() {
    JsonTester tester = jsonTester();
    String json = "{} {} {}";
    RowSet results = tester.parse(json);
    assertEquals(3, results.rowCount());
    results.clear();
    tester.close();
  }

  @Test
  public void testBoolean() {
    JsonTester tester = jsonTester();
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testInteger() {
    JsonTester tester = jsonTester();
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

    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testFloat() {
    JsonTester tester = jsonTester();

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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testExtendedFloat() {
    JsonOptions options = new JsonOptions();
    options.allowNanInf = true;
    JsonTester tester = jsonTester(options);
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testIntegerAsFloat() {
    JsonOptions options = new JsonOptions();
    options.readNumbersAsDouble = true;
    JsonTester tester = jsonTester(options);
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testString() {
    JsonTester tester = jsonTester();
    String json = "{a: \"\"} {a: \"hi\"} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow("")
        .addRow("hi")
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testRootTuple() {
    JsonTester tester = jsonTester();
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
    RowSetUtilities.verify(expected, results);
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
  public void testEmptyKey() {
    expectError("{\"\": 10}");
  }

  @Test
  public void testBlankKey() {
    expectError("{\"  \": 10}");
  }

  @Test
  public void testLeadingTrailingWhitespace() {
    JsonTester tester = jsonTester();
    String json = "{\" a\": 10, \" b\": 20, \" c \": 30}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("b", MinorType.BIGINT)
        .addNullable("c", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(10L, 20L, 30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Verify that names are case insensitive, first name determine's
   * Drill's column name.
   */

  @Test
  public void testCaseInsensitive() {
    JsonTester tester = jsonTester();
    String json = "{a: 10} {A: 20} {\" a \": 30}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(10L)
        .addRow(20L)
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Verify that the first name wins when determining case.
   */

  @Test
  public void testCaseInsensitive2() {
    JsonTester tester = jsonTester();
    String json = "{Bob: 10} {bOb: 20} {BoB: 30}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("Bob", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(10L)
        .addRow(20L)
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Verify that, when names are duplicated, the last value wins.
   */

  @Test
  public void testDuplicateNames() {
    JsonTester tester = jsonTester();
    String json = "{a: 10, A: 20, \" a \": 30}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testNestedTuple() {
    JsonTester tester = jsonTester();
    String json =
        "{id: 1, customer: { name: \"fred\" }}\n" +
        "{id: 2, customer: { name: \"barney\" }}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addMap("customer")
          .addNullable("name", MinorType.VARCHAR)
          .resumeSchema()
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, mapValue("fred"))
        .addRow(2L, mapValue("barney"))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testNullTuple() {
    JsonTester tester = jsonTester();
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
          .resumeSchema()
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, mapValue("fred"))
        .addRow(2L, mapValue("barney"))
        .addRow(3L, mapValue((String) null))
        .addRow(4L, mapValue((String) null))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testRootArray() {
    JsonTester tester = jsonTester();
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testRootArrayDisallowed() {
    JsonOptions options = new JsonOptions();
    options.skipOuterList = false;
    JsonTester tester = jsonTester(options);
    String json = "[{a: 0}, {a: 100}, {a: null}]";
    expectError(tester, json);
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

    readBatch(tableLoader, loader);
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
   * Test the JSON parser's limited recovery abilities.
   *
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-4653">DRILL-4653</a>
   */

  @Test
  public void testErrorRecovery() {
    JsonOptions options = new JsonOptions();
    options.skipMalformedRecords = true;
    JsonTester tester = jsonTester(options);
    String json = "{\"a: 10}\n{a: 20}\n{a: 30}";
    RowSet results = tester.parse(json);

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
//        .addRow(20L) // Recovery will eat the second record.
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
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
    JsonTester tester = jsonTester(options);
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
    JsonTester tester = jsonTester(options);
    tester.loaderOptions.setProjection(RowSetTestUtils.projectList("field_5"));
    RowSet results = tester.parseFile("store/json/schema_change_int_to_string.json");
//    results.print();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addMapArray("field_5")
          .addArray("inner_list", MinorType.VARCHAR)
          .addArray("inner_list_2", MinorType.VARCHAR)
          .resumeSchema()
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  // TODO: Union support
}
