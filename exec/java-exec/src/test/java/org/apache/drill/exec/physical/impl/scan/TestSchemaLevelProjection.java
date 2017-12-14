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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.NullMapColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple.ResolvedRowTuple;
import org.apache.drill.exec.physical.impl.scan.project.NullColumnBuilder;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedNullColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTableColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.ExplicitSchemaProjection;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.WildcardSchemaProjection;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.VectorSource;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestSchemaLevelProjection extends SubOperatorTest {

  @Test
  public void testWildcard() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers());
    assertEquals(1, scanProj.columns().size());

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRowTuple rootTuple = new ResolvedRowTuple(builder);
    new WildcardSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(3, columns.size());

    assertEquals("a", columns.get(0).name());
    assertEquals(0, columns.get(0).sourceIndex());
    assertSame(rootTuple, columns.get(0).source());
    assertEquals("c", columns.get(1).name());
    assertEquals(1, columns.get(1).sourceIndex());
    assertSame(rootTuple, columns.get(1).source());
    assertEquals("d", columns.get(2).name());
    assertEquals(2, columns.get(2).sourceIndex());
    assertSame(rootTuple, columns.get(2).source());
  }

  /**
   * Test SELECT list with columns defined in a order and with
   * name case different than the early-schema table.
   */

  @Test
  public void testFullList() {

    // Simulate SELECT c, b, a ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("c", "b", "a"),
        ScanTestUtils.parsers());
    assertEquals(3, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRowTuple rootTuple = new ResolvedRowTuple(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(3, columns.size());

    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).sourceIndex());
    assertSame(rootTuple, columns.get(0).source());

    assertEquals("b", columns.get(1).name());
    assertEquals(1, columns.get(1).sourceIndex());
    assertSame(rootTuple, columns.get(1).source());

    assertEquals("a", columns.get(2).name());
    assertEquals(0, columns.get(2).sourceIndex());
    assertSame(rootTuple, columns.get(2).source());
  }

  /**
   * Test SELECT list with columns missing from the table schema.
   */

  @Test
  public void testMissing() {

    // Simulate SELECT c, v, b, w ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("c", "v", "b", "w"),
        ScanTestUtils.parsers());
    assertEquals(4, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRowTuple rootTuple = new ResolvedRowTuple(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(4, columns.size());
    VectorSource nullBuilder = rootTuple.nullBuilder();

    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).sourceIndex());
    assertSame(rootTuple, columns.get(0).source());

    assertEquals("v", columns.get(1).name());
    assertEquals(0, columns.get(1).sourceIndex());
    assertSame(nullBuilder, columns.get(1).source());

    assertEquals("b", columns.get(2).name());
    assertEquals(1, columns.get(2).sourceIndex());
    assertSame(rootTuple, columns.get(2).source());

    assertEquals("w", columns.get(3).name());
    assertEquals(1, columns.get(3).sourceIndex());
    assertSame(nullBuilder, columns.get(3).source());
  }

  @Test
  public void testSubset() {

    // Simulate SELECT c, a ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("c", "a"),
        ScanTestUtils.parsers());
    assertEquals(2, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRowTuple rootTuple = new ResolvedRowTuple(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(2, columns.size());

    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).sourceIndex());
    assertSame(rootTuple, columns.get(0).source());

    assertEquals("a", columns.get(1).name());
    assertEquals(0, columns.get(1).sourceIndex());
    assertSame(rootTuple, columns.get(1).source());
  }

  @Test
  public void testDisjoint() {

    // Simulate SELECT c, a ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("b"),
        ScanTestUtils.parsers());
    assertEquals(1, scanProj.columns().size());

    // Simulate a data source, with early schema, of (a)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRowTuple rootTuple = new ResolvedRowTuple(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(1, columns.size());
    VectorSource nullBuilder = rootTuple.nullBuilder();

    assertEquals("b", columns.get(0).name());
    assertEquals(0, columns.get(0).sourceIndex());
    assertSame(nullBuilder, columns.get(0).source());
  }

  @Test
  public void testOmittedMap() {

    // Simulate SELECT a, b.c.d ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a", "b.c.d"),
        ScanTestUtils.parsers());
    assertEquals(2, scanProj.columns().size());
    {
      assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(1).nodeType());
      UnresolvedColumn bCol = (UnresolvedColumn) (scanProj.columns().get(1));
      assertTrue(bCol.element().isTuple());
    }

    // Simulate a data source, with early schema, of (a)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    NullColumnBuilder builder = new NullColumnBuilder(null, false);
    ResolvedRowTuple rootTuple = new ResolvedRowTuple(builder);
    new ExplicitSchemaProjection(
        scanProj, tableSchema, rootTuple,
        ScanTestUtils.resolvers());

    List<ResolvedColumn> columns = rootTuple.columns();
    assertEquals(2, columns.size());

    // Should have resolved a to a table column, b to a missing map.

    // A is projected

    ResolvedColumn aCol = columns.get(0);
    assertEquals("a", aCol.name());
    assertEquals(ResolvedTableColumn.ID, aCol.nodeType());

    // B is not projected, is implicitly a map

    ResolvedColumn bCol = columns.get(1);
    assertEquals("b", bCol.name());
    assertEquals(NullMapColumn.ID, bCol.nodeType());

    NullMapColumn bMap = (NullMapColumn) bCol;
    ResolvedTuple bMembers = bMap.members();
    assertNotNull(bMembers);
    assertEquals(1, bMembers.columns().size());

    // C is a map within b

    ResolvedColumn cCol = bMembers.columns().get(0);
    assertEquals(NullMapColumn.ID, cCol.nodeType());

    NullMapColumn cMap = (NullMapColumn) cCol;
    ResolvedTuple cMembers = cMap.members();
    assertNotNull(cMembers);
    assertEquals(1, cMembers.columns().size());

    // D is an unknown column type (not a map)

    ResolvedColumn dCol = cMembers.columns().get(0);
    assertEquals(ResolvedNullColumn.ID, dCol.nodeType());
  }

  // Test table of (a{b, c}), project a.b, a.d, a.e.f
}
