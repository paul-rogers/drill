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

import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleMap;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;

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
 * Drill requires types to be known on the first row. JSON files can be quite
 * lazy about revealing the type: there may be many missing values, null values,
 * or empty arrays before the parser finally sees a token that suggests the
 * column type. The JSON loader has "null deferral" logic to postpone picking a
 * column type until a type token finally appears (or until the end of the
 * batch, when the pick is forced.)
 * <p>
 * Empty arrays for multi-dimensional lists are tested in
 * {@link TestJsonLoaderRepeatedLists}.
 */
@Category(RowSetTests.class)
public class TestJsonLoaderNulls extends BaseTestJsonLoader {

  private JsonFixture jsonFixture(String json) {
    JsonFixture fixture = new JsonFixture();
    fixture.open(json);
    return fixture;
  }

  @Test
  public void testAllNulls() {
    final String json = "{a: null} {a: null} {a: null}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testAllNullsInNested() {
    final String json = "{a: {b: null}} {a: {b: null}} {a: {b: null}}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .addNullable("b", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(singleMap(null))
        .addSingleCol(singleMap(null))
        .addSingleCol(singleMap(null))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredScalarNull() {
    final String json = "{a: null} {a: null} {a: 10}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .addRow(10L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredScalarNullInNested() {
    final String json = "{a: {b: null}} {a: {b: null}} {a: {b: 10}}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .addNullable("b", MinorType.BIGINT)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(singleMap(null))
        .addSingleCol(singleMap(null))
        .addSingleCol(singleMap(10L))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredScalarNullAsText() {
    final String json = "{a: null} {a: null} {a: null} {a: 10} {a: \"foo\"}";
    final JsonFixture tester = jsonFixture(json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.read(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as text mode.

    results = tester.read();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol("10")
        .addSingleCol("foo")
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  @Test
  public void testDeferredScalarArray() {
    final String json = "{a: []} {a: null} {a: [10, 20]}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(longArray())
        .addSingleCol(longArray())
        .addSingleCol(longArray(10L, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredArrayAsText() {
    final String json = "{a: []} {a: null} {a: []} {a: [10, 20]} {a: [\"foo\", \"bar\"]}";
    final JsonFixture tester = jsonFixture(json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.read(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray())
        .addSingleCol(strArray())
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as text mode.

    results = tester.read();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray())
        .addSingleCol(strArray("10", "20"))
        .addSingleCol(strArray("foo", "bar"))
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  /**
   * Double deferral. First null causes a deferred null. Then,
   * the empty array causes a deferred array. Finally, the third
   * array announces the type.
   */
  @Test
  public void testDeferredNullToArray() {
    final String json = "{a: null} {a: []} {a: [10, 20]}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(longArray())
        .addSingleCol(longArray())
        .addSingleCol(longArray(10L, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }
}
