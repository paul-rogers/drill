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

import static org.apache.drill.test.rowSet.RowSetUtilities.doubleArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.intArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Ignore;
import org.junit.Test;

public class TestJsonLoaderArray extends BaseTestJsonLoader {

  @Test
  public void testBooleanArray() {
    JsonTester tester = jsonTester();
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testIntegerArray() {
    JsonTester tester = jsonTester();
    String json =
        "{a: [1, 100]}\n" +
        "{a: []} {a: null}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(longArray(1L, 100L))
        .addSingleCol(longArray())
        .addSingleCol(longArray())
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testFloatArray() {
    JsonTester tester = jsonTester();
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testIntegerArrayAsFloat() {
    JsonOptions options = new JsonOptions();
    options.readNumbersAsDouble = true;
    JsonTester tester = jsonTester(options);
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testIntererFloatConflictInArray() {
    expectError("{a: [10, 12.5]");
  }

  @Test
  public void testStringArray() {
    JsonTester tester = jsonTester();
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testAllTextArray() {
    JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    JsonTester tester = jsonTester(options);
    String json =
        "{a: [\"foo\", true, false, 10, null, 20.0] }";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray("foo", "true", "false", "10", "", "20.0"))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testTupleArray() {
    JsonTester tester = jsonTester();
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
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredTupleArray() {
    JsonTester tester = jsonTester();
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
    RowSetUtilities.verify(expected, results);
    tester.close();
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
    readBatch(tableLoader, loader);
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

    RowSetUtilities.verify(expected, result);

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

    readBatch(tableLoader, loader);
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

    RowSetUtilities.verify(expected, result);

    try {
      inStream.close();
    } catch (IOException e) {
      fail();
    }
    loader.close();
    tableLoader.close();
  }

}
