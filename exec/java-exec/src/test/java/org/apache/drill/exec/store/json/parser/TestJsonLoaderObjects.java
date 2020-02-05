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

import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

public class TestJsonLoaderObjects extends BaseTestJsonLoader {

  private JsonFixture jsonFixture(String json) {
    JsonFixture fixture = new JsonFixture();
    fixture.open(json);
    return fixture;
  }

  @Test
  public void testNestedTuple() {
    final String json =
        "{id: 1, customer: { name: \"fred\" }}\n" +
        "{id: 2, customer: { name: \"barney\" }}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addMap("customer")
          .addNullable("name", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, mapValue("fred"))
        .addRow(2L, mapValue("barney"))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testNullTuple() {
    final String json =
        "{id: 1, customer: {name: \"fred\"}}\n" +
        "{id: 2, customer: {name: \"barney\"}}\n" +
        "{id: 3, customer: null}\n" +
        "{id: 4}";
    final JsonFixture tester = jsonFixture(json);
    final RowSet results = tester.read();
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addMap("customer")
          .addNullable("name", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, mapValue("fred"))
        .addRow(2L, mapValue("barney"))
        .addRow(3L, mapValue((String) null))
        .addRow(4L, mapValue((String) null))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }
}
