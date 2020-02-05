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

import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.impl.RowSetTestUtils;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

/**
 * Test projection. For things within Drill's control, anything goes in
 * non-projected columns. However, some things are not in Drill's control.
 * For example, the JSON parser, not Drill, determines if NaN and
 * Infinity are valid float values, so they will throw an error, even in
 * non-projected columns, unless the extended float option is set.
 */
public class TestJsonLoaderProjection extends BaseTestJsonLoader {

  @Test
  public void testSimpleProjection() {
    final String json =
        "{a: 10, b: \"foo\"}\n" +
        "{a: 20, b: \"bar\"}\n";

    final JsonFixture reader = new JsonFixture();
    reader.project(RowSetTestUtils.projectList("a"));
    reader.open(json);
    RowSet results = reader.read();

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(10L)
        .addSingleCol(20L)
        .build();

    RowSetUtilities.verify(expected, results);
    reader.close();
  }

  /**
   * The non-projected column is ignored. So, even though the
   * types conflict (first BIGINT, then DOUBLE), we just ignore
   * the data.
   */
  @Test
  public void testTypeConflict() {
    final String json =
        "{a: 10, b: 10}\n" +
        "{a: 20, b: 10.1}\n";

    final JsonFixture reader = new JsonFixture();
    reader.project(RowSetTestUtils.projectList("a"));
    reader.open(json);
    RowSet results = reader.read();

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(10L)
        .addSingleCol(20L)
        .build();

    RowSetUtilities.verify(expected, results);
    reader.close();
  }

  /**
   * Test that guessing a null type incorrectly has no effect
   * for non-projected columns.
   */
  @Test
  public void testNullCol() {
    final String json =
        "{a: 10, b: null}\n" +
        "{a: 20, b: null}\n" +
        "{a: 30, b: 10}\n" +
        "{a: 40, b: \"foo\"}\n";

    final JsonFixture reader = new JsonFixture();
    reader.project(RowSetTestUtils.projectList("a"));
    reader.open(json);
    RowSet results = reader.read(2);

    // Saw only nulls, Normally would have guessed a type.
    // But, no need here, since b is not projected.
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(10L)
        .addSingleCol(20L)
        .build();
    RowSetUtilities.verify(expected, results);

    // Next batch is OK, though it would fail if b were projected.
    results = reader.read();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(30L)
        .addSingleCol(40L)
        .build();
    RowSetUtilities.verify(expected, results);
    reader.close();
  }

  /**
   * Verify that non-projected maps are just "free-wheeled", the JSON loader
   * just seeks the matching end brace, ignoring content semantics.
   */
  @Test
  public void testMap() {
    final String json =
        "{a: 10, b: {c: 100, c: 200, d: {x: null, y: 30}}}\n" +
        "{a: 20, b: { }}\n" +
        "{a: 30, b: null}\n" +
        "{a: 40, b: {c: \"foo\", d: [{}, {x: {}}]}}\n" +
        "{a: 50, b: [{c: 100, c: 200, d: {x: null, y: 30}}, 10]}\n" +
        "{a: 60, b: []}\n" +
        "{a: 70, b: null}\n" +
        "{a: 80, b: [\"foo\", [{}, {x: {}}]]}\n" +
        "{a: 90, b: 55.5} {a: 100, b: 10} {a: 110, b: \"foo\"}";

    final JsonFixture jsonReader = new JsonFixture();
    jsonReader.project(RowSetTestUtils.projectList("a"));
    jsonReader.open(json);
    RowSet result = jsonReader.read();

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    assertTrue(expectedSchema.isEquivalent(result.schema()));
    final RowSetReader reader = result.reader();
    int id = 10;
    while (reader.hasNext()) {
      reader.next();
      assertEquals(id, reader.scalar(0).getLong());
      id += 10;
    }
    result.clear();

    jsonReader.close();
  }

  /**
   * Test based on TestJsonReader.testAllTextMode
   * Verifies the complex case of an array of maps that contains an array of
   * strings (from all text mode). Also verifies projection.
   */
  @Test
  public void testMapProject() {
    JsonFixture tester = new JsonFixture();
    tester.options.allTextMode = true;
    tester.project(RowSetTestUtils.projectList("field_5"));
    tester.openResource("store/json/schema_change_int_to_string.json");
    final RowSet results = tester.read();
//    results.print();

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMapArray("field_5")
          .addArray("inner_list", MinorType.VARCHAR)
          .addArray("inner_list_2", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
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

  /**
   * Test that the JSON reader uses a projection hint to
   * determine that a null type is an array.
   */
  @Test
  public void testArrayProjectionHint() {

    // Read the one and only record into a batch. When we saw the
    // null value for b, we should have used the knowledge that b must
    // be an array (based on the projection of b[0]), to make it an array.
    // Then, at the end of the batch, we guess that the array is of
    // type VARCHAR.

    final String json =
        "{a: 1, b: null}";
    JsonFixture tester = new JsonFixture();
    tester.options.detectTypeEarly = true;
    tester.project(RowSetTestUtils.projectList("a", "b[0]"));
    tester.open(json);
    final RowSet results = tester.read();

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addArray("b", MinorType.VARCHAR)
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, strArray())
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Test that the JSON reader uses a projection hint to
   * determine that a null type is a map.
   */
  @Test
  public void testMapProjectionHint() {

    // Read the one and only record into a batch. When we saw the
    // null value for b, we should have used the knowledge that b must
    // be a map (based on the projection of a.b), to make it a map
    // (which contains no columns.)

    final String json =
        "{a: 1, b: null}";
    JsonFixture tester = new JsonFixture();
    tester.options.detectTypeEarly = true;
    tester.project(RowSetTestUtils.projectList("a", "b.c"));
    tester.open(json);
    final RowSet results = tester.read();

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addMap("b")
          .resumeSchema()
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, mapValue())
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Test that the JSON reader uses a projection hint to
   * determine that a null type is a map.
   */
  @Test
  public void testMapArrayProjectionHint() {

    // Read the one and only record into a batch. When we saw the
    // null value for b, we should have used the knowledge that b must
    // be a map (based on the projection of a.b), to make it a map
    // (which contains no columns.)

    final String json =
        "{a: 1, b: null}";
    JsonFixture tester = new JsonFixture();
    tester.options.detectTypeEarly = true;
    tester.project(RowSetTestUtils.projectList("a", "b[0].c"));
    tester.open(json);
    final RowSet results = tester.read();

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addMapArray("b")
          .resumeSchema()
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, mapValue())
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }
}
