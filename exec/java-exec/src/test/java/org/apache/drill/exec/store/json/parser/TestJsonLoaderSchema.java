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

import static org.apache.drill.test.rowSet.RowSetUtilities.doubleArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.parser.TupleProjection;
import org.apache.drill.exec.store.easy.json.parser.TupleProjectionImpl;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

public class TestJsonLoaderSchema extends BaseTestJsonLoader {

  private static class ProjectionFixture implements TupleProjection {

    public Map<String, MajorType> cols = new HashMap<>();
    public Map<String, TupleProjection> maps = new HashMap<>();

    @Override
    public boolean isProjected(String key) { return true; }

    @Override
    public MajorType typeOf(String key) {
      return cols.get(key);
    }

    @Override
    public TupleProjection map(String key) {
      TupleProjection child = maps.get(key);
      return child == null ? TupleProjectionImpl.projectAll() : child;
    }

    public void add(String name, MajorType type) {
      cols.put(name, type);
    }

    public void add(String name, TupleProjection map) {
      maps.put(name, map);
      cols.put(name, Types.required(MinorType.MAP));
    }

    @Override
    public Hint typeHint(String key) { return null; }
  }

  @Test
  public void testDeferredScalarNullAsType() {
    final String json = "{a: null} {a: null} {a: null} {a: 10} {a: 20}";

    ProjectionFixture proj = new ProjectionFixture();
    proj.add("a", Types.optional(MinorType.BIGINT));

    JsonFixture tester = new JsonFixture();
    tester.options.rootProjection = proj;
    tester.open(json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.read(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as given type.

    results = tester.read();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(10L)
        .addSingleCol(20L)
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  @Test
  public void testNestedDeferredScalarNullAsType() {
    final String json =
        "{a: {b: null, c: null, d: null}, e: null}\n" +
        "{a: {b: null, c: null, d: null}, e: null}\n" +
        "{a: {b: null, c: null, d: null}, e: null}\n" +
        "{a: {b: 10, c: \"fred\", d: [1.5, 2.5]}, e: 10.25}\n";

    // Find types for three fields.

    ProjectionFixture childProj = new ProjectionFixture();
    childProj.add("b", Types.optional(MinorType.BIGINT));
    childProj.add("c", Types.optional(MinorType.VARCHAR));
    childProj.add("d", Types.repeated(MinorType.FLOAT8));
    ProjectionFixture proj = new ProjectionFixture();
    proj.add("a", childProj);

    JsonFixture tester = new JsonFixture();
    tester.options.rootProjection = proj;
    tester.options.detectTypeEarly = true;
    tester.open(json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.read(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .addNullable("b", MinorType.BIGINT)
          .addNullable("c", MinorType.VARCHAR)
          .addArray("d", MinorType.FLOAT8)
          .resumeSchema()
        .addNullable("e", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(mapValue(null, null, doubleArray()), null)
        .addRow(mapValue(null, null, doubleArray()), null)
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as given type.

    results = tester.read();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(mapValue(null, null, doubleArray()), null)
        .addRow(mapValue(10L, "fred", doubleArray(1.5, 2.5)), "10.25")
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  @Test
  public void testDeferredArrayAsType() {
    final String json =
        "{a: [], b: []}\n" +
        "{a: null, b: null}\n" +
        "{a: [], b: []}\n" +
        "{a: [10, 20], b: [10.5, \"fred\"]}\n" +
        "{a: [30, 40], b: [null, false]}";

    ProjectionFixture proj = new ProjectionFixture();
    proj.add("a", Types.repeated(MinorType.BIGINT));

    JsonFixture tester = new JsonFixture();
    tester.options.rootProjection = proj;
    tester.open(json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.read(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .addArray("b", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(longArray(), strArray())
        .addRow(longArray(), strArray())
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as long.

    results = tester.read();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(longArray(), strArray())
        .addRow(longArray(10L, 20L), strArray("10.5", "fred"))
        .addRow(longArray(30L, 40L), strArray("", "false"))
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }
}
