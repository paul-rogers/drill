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
package org.apache.drill.exec.physical.impl.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.ExplicitSchemaProjection;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.WildcardSchemaProjection;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestSchemaLevelProjection extends SubOperatorTest {

  @Test
  public void testWildcard() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectAll(), Lists.newArrayList());
    assertEquals(1, scanProj.columns().size());

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    ScanTestUtils.DummySource fileSource = new ScanTestUtils.DummySource();
    SchemaLevelProjection schemaProj = new WildcardSchemaProjection(
        scanProj, tableSchema, fileSource, Lists.newArrayList());
    assertEquals(3, schemaProj.columns().size());

    List<ResolvedColumn> columns = schemaProj.columns();
    assertEquals("a", columns.get(0).name());
    assertEquals(0, columns.get(0).projection().sourceIndex());
    assertEquals(0, columns.get(0).projection().destIndex());
    assertSame(fileSource, columns.get(0).projection().source());
    assertEquals("c", columns.get(1).name());
    assertEquals(1, columns.get(1).projection().sourceIndex());
    assertEquals(1, columns.get(1).projection().destIndex());
    assertSame(fileSource, columns.get(1).projection().source());
    assertEquals("d", columns.get(2).name());
    assertEquals(2, columns.get(2).projection().sourceIndex());
    assertEquals(2, columns.get(2).projection().destIndex());
    assertSame(fileSource, columns.get(2).projection().source());
  }
  /**
   * Test SELECT list with columns defined in a order and with
   * name case different than the early-schema table.
   */

  @Test
  public void testFullList() {

    // Simulate SELECT c, b, a ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList("c", "b", "a"),
        Lists.newArrayList());
    assertEquals(3, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    ScanTestUtils.DummySource fileSource = new ScanTestUtils.DummySource();
    ScanTestUtils.DummySource nullSource = new ScanTestUtils.DummySource();
    SchemaLevelProjection schemaProj = new ExplicitSchemaProjection(
        scanProj, tableSchema, fileSource, nullSource, Lists.newArrayList());
    assertEquals(3, schemaProj.columns().size());

    List<ResolvedColumn> columns = schemaProj.columns();
    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).projection().sourceIndex());
    assertEquals(0, columns.get(0).projection().destIndex());
    assertSame(fileSource, columns.get(0).projection().source());

    assertEquals("b", columns.get(1).name());
    assertEquals(1, columns.get(1).projection().sourceIndex());
    assertEquals(1, columns.get(1).projection().destIndex());
    assertSame(fileSource, columns.get(1).projection().source());

    assertEquals("a", columns.get(2).name());
    assertEquals(0, columns.get(2).projection().sourceIndex());
    assertEquals(2, columns.get(2).projection().destIndex());
    assertSame(fileSource, columns.get(2).projection().source());
  }

  /**
   * Test SELECT list with columns missing from the table schema.
   */

  @Test
  public void testMissing() {

    // Simulate SELECT c, v, b, w ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList("c", "v", "b", "w"),
        Lists.newArrayList());
    assertEquals(4, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    ScanTestUtils.DummySource fileSource = new ScanTestUtils.DummySource();
    ScanTestUtils.DummySource nullSource = new ScanTestUtils.DummySource();
    SchemaLevelProjection schemaProj = new ExplicitSchemaProjection(
        scanProj, tableSchema, fileSource, nullSource, Lists.newArrayList());
    assertEquals(4, schemaProj.columns().size());

    @SuppressWarnings("unused")
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .addNullable("v", MinorType.NULL)
        .add("b", MinorType.VARCHAR)
        .addNullable("w", MinorType.NULL)
        .buildSchema();

    List<ResolvedColumn> columns = schemaProj.columns();
    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).projection().sourceIndex());
    assertEquals(0, columns.get(0).projection().destIndex());
    assertSame(fileSource, columns.get(0).projection().source());

    assertEquals("v", columns.get(1).name());
    assertEquals(0, columns.get(1).projection().sourceIndex());
    assertEquals(1, columns.get(1).projection().destIndex());
    assertSame(nullSource, columns.get(1).projection().source());

    assertEquals("b", columns.get(2).name());
    assertEquals(1, columns.get(2).projection().sourceIndex());
    assertEquals(2, columns.get(2).projection().destIndex());
    assertSame(fileSource, columns.get(2).projection().source());

    assertEquals("w", columns.get(3).name());
    assertEquals(1, columns.get(3).projection().sourceIndex());
    assertEquals(3, columns.get(3).projection().destIndex());
    assertSame(nullSource, columns.get(3).projection().source());
  }

  @Test
  public void testSubset() {

    // Simulate SELECT c, a ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList("c", "a"),
        Lists.newArrayList());
    assertEquals(2, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    ScanTestUtils.DummySource fileSource = new ScanTestUtils.DummySource();
    ScanTestUtils.DummySource nullSource = new ScanTestUtils.DummySource();
    SchemaLevelProjection schemaProj = new ExplicitSchemaProjection(
        scanProj, tableSchema, fileSource, nullSource, Lists.newArrayList());
    assertEquals(2, schemaProj.columns().size());

    @SuppressWarnings("unused")
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    List<ResolvedColumn> columns = schemaProj.columns();
    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).projection().sourceIndex());
    assertEquals(0, columns.get(0).projection().destIndex());
    assertSame(fileSource, columns.get(0).projection().source());

    assertEquals("a", columns.get(1).name());
    assertEquals(0, columns.get(1).projection().sourceIndex());
    assertEquals(1, columns.get(1).projection().destIndex());
    assertSame(fileSource, columns.get(1).projection().source());
  }

  // TODO: Custom columns array type
}
