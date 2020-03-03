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

import static org.apache.drill.test.rowSet.RowSetUtilities.boolArray;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

public class TestUnknowns extends BaseJsonLoaderTest {

  @Test
  public void testNullToBoolean() {
    String json =
        "{a: null} {a: true} {a: false} {a: true}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIT)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow((Boolean) null)
        .addRow(true)
        .addRow(false)
        .addRow(true)
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testNullToString() {
    String json =
        "{a: null} {a: \"foo\"} {a: \"bar\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow((String) null)
        .addRow("foo")
        .addRow("bar")
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  /**
   * Input contains all nulls. The loader will force resolve to a
   * type, and will choose VARCHAR as all scalar types which
   * may later appear can be converted to VARCHAR.
   */
  @Test
  public void testForcedResolve() {
    String json =
        "{a: null} {a: null}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow((String) null)
        .addRow((String) null)
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testArrayToBoolean() {
    String json =
        "{a: []} {a: [true, false]} {a: true}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIT)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(boolArray())
        .addSingleCol(boolArray(true, false))
        .addSingleCol(boolArray(false))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  // null to array to boolean

  // array to null

  // array to object

}
