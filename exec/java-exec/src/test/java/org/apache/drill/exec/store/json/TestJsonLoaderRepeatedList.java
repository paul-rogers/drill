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
import static org.apache.drill.test.rowSet.RowSetUtilities.objArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleObjArray;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestJsonLoaderRepeatedList extends BaseTestJsonLoader {

  @Test
  public void testBoolean2D() {
    JsonTester tester = jsonTester();
    String json =
        "{a: [[true, false], [false, true]]}\n" +
        "{a: [[true], [false]]}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.TINYINT, 2)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(intArray(1, 0), intArray(0, 1)))
        .addSingleCol(objArray(intArray(1), intArray(0)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testInt2D() {
    JsonTester tester = jsonTester();
    String json =
        "{a: [[1, 10], [2, 20]]}\n" +
        "{a: [[1], [-1]]}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT, 2)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(longArray(1L, 10L), longArray(2L, 20L)))
        .addSingleCol(objArray(longArray(1L), longArray(-1L)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testFloat2D() {
    JsonTester tester = jsonTester();
    String json =
        "{a: [[1.25, 10.5], [2.25, 20.5]]}\n" +
        "{a: [[1], [-1]]}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.FLOAT8, 2)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(doubleArray(1.25D, 10.5D), doubleArray(2.25D, 20.5D)))
        .addSingleCol(objArray(doubleArray(1D), doubleArray(-1D)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testString2D() {
    JsonTester tester = jsonTester();
    String json =
        "{a: [[\"first\", \"second\"], [\"third\", \"fourth\"]]}\n" +
        "{a: [[\"fifth\"], [\"sixth\"]]}";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR, 2)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(strArray("first", "second"), strArray("third", "fourth")))
        .addSingleCol(objArray(strArray("fifth"), strArray("sixth")))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testLeadingEmpties() {
    JsonTester tester = jsonTester();
    String json =
        "{a: [[]]}\n" +
        "{a: [[], null]}\n" +
        "{a: [[1, 10], [2, 20]]}\n";
    RowSet results = tester.parse(json);
    BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT, 2)
        .build();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(singleObjArray(longArray()))
        .addSingleCol(objArray(longArray(), longArray()))
        .addSingleCol(objArray(longArray(1L, 10L), longArray(2L, 20L)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  // TODO: nulls, empties after type

  // TODO: [] then [[]], then type

  // TODO: Resolve at end of batch

  // TODO: List of objects

  // TODO: Multi-dimensional lists

}
