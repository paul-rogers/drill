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

import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.listValue;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.exec.store.json.JsonLoaderTestUtils.JsonTester;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestJsonLoaderLists extends SubOperatorTest {

  private JsonTester jsonTester() {
    return new JsonTester(fixture.allocator());
  }

  private JsonTester jsonTester(JsonOptions options) {
    return new JsonTester(fixture.allocator(), options);
  }

  /**
   * Test scalar list support.
   */

  @Test
  public void testScalarList() {

    // Read the one and only record into a batch. When we saw the
    // null value for b, we should have used the knowledge that b must
    // be a map (based on the projection of a.b), to make it an map
    // (which contains no columns.)

    String json =
        "{a: 1, b: [10, null, 20]}";
    JsonOptions options = new JsonOptions();
    JsonTester tester = jsonTester(options);
    options.useArrayTypes = false;
    RowSet results = tester.parse(json);

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addList("b")
          .addType(MinorType.BIGINT)
          .build()
        .build();

    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, longArray(10L, null, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testScalarListLeadingNull() {
    String json =
        "{a: 1, b: [null, 10, 20]}";
    JsonOptions options = new JsonOptions();
    JsonTester tester = jsonTester(options);
    options.useArrayTypes = false;
    RowSet results = tester.parse(json);

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addList("b")
          .addType(MinorType.BIGINT)
          .build()
        .build();

    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, longArray(null, 10L, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testScalarListAllNull() {
    String json =
        "{a: 1, b: [null, null, null]}";
    JsonOptions options = new JsonOptions();
    JsonTester tester = jsonTester(options);
    options.useArrayTypes = false;
    RowSet results = tester.parse(json);

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addList("b")
          .addType(MinorType.VARCHAR)
          .build()
        .build();

    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, strArray(null, null, null))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testDeferredListAsText() {
    String json = "{a: []} {a: null} {a: []} {a: [10, 20]} {a: [\"foo\", \"bar\"]}";
    ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    JsonOptions options = new JsonOptions();
    options.useArrayTypes = false;
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
        .addList("a")
          .addType(MinorType.VARCHAR)
          .build()
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
  public void testListPromotionFromNull() {
    String json = "{a: null} {a: []} {a: null} {a: [10, 20]}";
    JsonOptions options = new JsonOptions();
    JsonTester tester = jsonTester(options);
    options.useArrayTypes = false;
    RowSet results = tester.parse(json);

    BatchSchema expectedSchema = new SchemaBuilder()
        .addList("a")
          .addType(MinorType.BIGINT)
          .build()
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(longArray())
        .addSingleCol(null)
        .addSingleCol(longArray(10L, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testConflictingListTypes() {
    JsonOptions options = new JsonOptions();
    options.skipOuterList = false;
    JsonTester tester = jsonTester(options);
    String json = "{a: [10]} {a: [\"oops\"]}";
    JsonLoaderTestUtils.expectError(tester, json);
  }

  @Test
  public void testObjectList() {
    String json =
        "{a: [{b: \"fred\", c: 10}, null, {b: \"barney\", c: 20}]}\n" +
        "{a: []} {a: null} {a: [{b: \"wilma\", c: 30}]}";
    JsonOptions options = new JsonOptions();
    JsonTester tester = jsonTester(options);
    options.useArrayTypes = false;
    RowSet results = tester.parse(json);

    // Note use of TupleMetadata: BatchSchema can't hold the
    // structure of a list.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addMap()
            .addNullable("b", MinorType.VARCHAR)
            .addNullable("c", MinorType.BIGINT)
            .buildNested()
          .build()
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(mapArray(mapValue("fred", 10L), null, mapValue("barney", 20L)))
        .addSingleCol(mapArray())
        .addSingleCol(null)
        .addSingleCol(mapArray(mapValue("wilma", 30L)))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testDeferredObjectList() {
    String json =
        "{a: null} {a: []} {a: [null]}\n" +
        "{a: [{b: \"fred\", c: 10}, null, {b: \"barney\", c: 20}]}\n" +
        "{a: []} {a: null} {a: [{b: \"wilma\", c: 30}]}";
    JsonOptions options = new JsonOptions();
    JsonTester tester = jsonTester(options);
    options.useArrayTypes = false;
    RowSet results = tester.parse(json);

    // Note use of TupleMetadata: BatchSchema can't hold the
    // structure of a list.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addMap()
            .addNullable("b", MinorType.VARCHAR)
            .addNullable("c", MinorType.BIGINT)
            .buildNested()
          .build()
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(mapArray())
        .addSingleCol(mapArray((Object[]) null))
        .addSingleCol(mapArray(mapValue("fred", 10L), null, mapValue("barney", 20L)))
        .addSingleCol(mapArray())
        .addSingleCol(null)
        .addSingleCol(mapArray(mapValue("wilma", 30L)))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testListofListofScalar() {
    String json =
        "{a: [[1, 2], [3, 4]]}";
    JsonOptions options = new JsonOptions();
    JsonTester tester = jsonTester(options);
    options.useArrayTypes = false;
    RowSet results = tester.parse(json);

    // Note use of TupleMetadata: BatchSchema can't hold the
    // structure of a list.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addList()
            .addType(MinorType.BIGINT)
            .buildNested()
          .build()
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(listValue(listValue(1L, 2L), listValue(3L, 4L)))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  // TODO: Nested list
  // TODO: List of objects

}
