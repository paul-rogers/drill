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
package org.apache.drill.exec.store.json.loader;

import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

public class TestObjects extends BaseJsonLoaderTest {

  @Test
  public void testMap() {
    String json =
        "{a: 1, m: {b: 10, c: 20}}\n" +
        "{a: 2, m: {b: 100}}\n" +
        "{a: 3, m: {c: 220}}\n" +
        "{a: 4, m: {}}\n" +
        "{a: 5, m: null}\n" +
        "{a: 6}\n" +
        "{a: 7, m: {b: 700, c: 720}}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addMap("m")
          .addNullable("b", MinorType.BIGINT)
          .addNullable("c", MinorType.BIGINT)
          .resumeSchema()
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, mapValue(10L, 20L))
        .addRow(2L, mapValue(100L, null))
        .addRow(3L, mapValue(null, 220L))
        .addRow(4L, mapValue(null, null))
        .addRow(5L, mapValue(null, null))
        .addRow(6L, mapValue(null, null))
        .addRow(7L, mapValue(700L, 720L))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testMapWithSchema() {
    String json =
        "{a: 6}\n" +
        "{a: 5, m: null}\n" +
        "{a: 4, m: {}}\n" +
        "{a: 2, m: {b: 100}}\n" +
        "{a: 3, m: {c: 220}}\n" +
        "{a: 1, m: {b: 10, c: 20}}\n" +
        "{a: 7, m: {b: 700, c: 720}}";

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addMap("m")
          .addNullable("b", MinorType.BIGINT)
          .addNullable("c", MinorType.BIGINT)
          .resumeSchema()
        .build();

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.providedSchema = schema;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow(6L, mapValue(null, null))
        .addRow(5L, mapValue(null, null))
        .addRow(4L, mapValue(null, null))
        .addRow(2L, mapValue(100L, null))
        .addRow(3L, mapValue(null, 220L))
        .addRow(1L, mapValue(10L, 20L))
        .addRow(7L, mapValue(700L, 720L))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testMapAsJson() {
    String json =
        "{a: 6}\n" +
        "{a: 5, m: null}\n" +
        "{a: 4, m: {}}\n" +
        "{a: 2, m: {b: 100}}\n" +
        "{a: 3, m: {c: 220}}\n" +
        "{a: 1, m: {b: 10, c: 20}}\n" +
        "{a: 7, m: {b: 700, c: 720}}";

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("m", MinorType.VARCHAR)
        .build();
    ColumnMetadata m = schema.metadata("m");
    m.setProperty(ColumnMetadata.JSON_MODE, ColumnMetadata.JSON_LITERAL_MODE);

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.providedSchema = schema;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow(6L, null)
        .addRow(5L, "null")
        .addRow(4L, "{}")
        .addRow(2L, "{\"b\": 100}")
        .addRow(3L, "{\"c\": 220}")
        .addRow(1L, "{\"b\": 10, \"c\": 20}")
        .addRow(7L, "{\"b\": 700, \"c\": 720}")
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }
}
