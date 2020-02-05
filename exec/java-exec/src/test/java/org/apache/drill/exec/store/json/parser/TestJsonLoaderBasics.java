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
package org.apache.drill.exec.store.json.parser;

import static org.junit.Assert.assertEquals;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the JSON loader itself: the various syntax combinations that the loader
 * understands or rejects. Assumes that the JSON parser itself works, and that
 * the underlying result set loader properly handles column types, projections
 * and so on.
 */
@Category(RowSetTests.class)
public class TestJsonLoaderBasics extends BaseTestJsonLoader {

  private JsonFixture jsonFixture(String json) {
    JsonFixture fixture = new JsonFixture();
    fixture.open(json);
    return fixture;
  }

  @Test
  public void testEmpty() {
    String json = "";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    assertEquals(0, results.rowCount());
    results.clear();
    tester.close();
  }

  @Test
  public void testEmptyTuple() {
    final String json = "{} {} {}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    assertEquals(3, results.rowCount());
    results.clear();
    tester.close();
  }

  @Test
  public void testBoolean() {
    final String json = "{a: true} {a: false} {a: null}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1)
        .addRow(0)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testInteger() {
    final String json = "{a: 0} {a: 100} {a: null}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0L)
        .addRow(100L)
        .addSingleCol(null)
        .build();

    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testFloat() {

    // Note: integers allowed after first float.

    final String json = "{a: 0.0} {a: 100.5} {a: 5} {a: null}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
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
    final String json =
        "{a: 0.0} {a: 100.5} {a: -200.5} {a: NaN}\n" +
        "{a: Infinity} {a: -Infinity} {a: null}";
    JsonFixture tester = new JsonFixture();
    tester.options.allowNanInf = true;
    tester.open(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
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
    final String json = "{a: 0} {a: 100} {a: null} {a: 123.45}";
    JsonFixture tester = new JsonFixture();
    tester.options.readNumbersAsDouble = true;
    tester.open(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
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
    final String json = "{a: \"\"} {a: \"hi\"} {a: null}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow("")
        .addRow("hi")
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testRootTuple() {
    final String json =
      "{id: 1, name: \"Fred\", balance: 100.0}\n" +
      "{id: 2, name: \"Barney\"}\n" +
      "{id: 3, name: \"Wilma\", balance: 500.00}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addNullable("name", MinorType.VARCHAR)
        .addNullable("balance", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
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
    final String json = "{\" a\": 10, \" b\": 20, \" c \": 30}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("b", MinorType.BIGINT)
        .addNullable("c", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
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
    final String json = "{a: 10} {A: 20} {\" a \": 30}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
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
  public void testMixedCase() {
    final String json = "{Bob: 10} {bOb: 20} {BoB: 30}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("Bob", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
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
    final String json = "{a: 10, A: 20, \" a \": 30}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testRootArray() {
    final String json = "[{a: 0}, {a: 100}, {a: null}]";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0L)
        .addRow(100L)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testRootArrayDisallowed() {
    final String json = "[{a: 0}, {a: 100}, {a: null}]";
    JsonFixture tester = new JsonFixture();
    tester.options.skipOuterList = false;
    tester.open(json);
    expectError(tester);
    tester.close();
  }
}
