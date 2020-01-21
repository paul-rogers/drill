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
package org.apache.drill.exec.physical.resultSet.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.resultSet.impl.RowSetTestUtils;
import org.apache.drill.exec.physical.resultSet.project.Qualifier.MapQualifier;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple.TupleProjectionType;
import org.apache.drill.test.BaseTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the projection list parser: parses a list of SchemaPath
 * items into a detailed structure, handling duplicate or overlapping
 * items. Special cases the select-all (SELECT *) and select none
 * (SELECT COUNT(*)) cases.
 * <p>
 * These tests should verify everything about (runtime) projection
 * parsing; the only bits not tested here is that which is
 * inherently specific to some use case.
 */
@Category(RowSetTests.class)
public class TestProjectedTuple extends BaseTest {

  /**
   * Null map means everything is projected
   */
  @Test
  public void testProjectionAll() {
    RequestedTuple projSet = Projections.parse(null);
    assertSame(TupleProjectionType.ALL, projSet.type());
    assertTrue(projSet.isProjected("foo"));
    assertTrue(projSet.projections().isEmpty());
  }

  /**
   * SELECT * means everything is projected
   */
  @Test
  public void testWildcard() {
    RequestedTuple projSet = Projections.parse(RowSetTestUtils.projectAll());
    assertSame(TupleProjectionType.ALL, projSet.type());
    assertTrue(projSet.isProjected("foo"));
    assertNull(projSet.get("foo"));
    assertEquals(1, projSet.projections().size());
  }

  /**
   * Test an empty projection which occurs in a
   * SELECT COUNT(*) query.
   * Empty list means nothing is projected.
   */
  @Test
  public void testProjectionNone() {
    RequestedTuple projSet = Projections.parse(new ArrayList<SchemaPath>());
    assertSame(TupleProjectionType.NONE, projSet.type());
    assertFalse(projSet.isProjected("foo"));
    assertTrue(projSet.projections().isEmpty());
  }

  /**
   * Simple non-map columns
   */
  @Test
  public void testProjectionSimple() {

    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a", "b", "c"));
    assertSame(TupleProjectionType.SOME, projSet.type());
    assertTrue(projSet.isProjected("a"));
    assertTrue(projSet.isProjected("b"));
    assertTrue(projSet.isProjected("c"));
    assertFalse(projSet.isProjected("d"));

    List<RequestedColumn> cols = projSet.projections();
    assertEquals(3, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isSimple());
    assertFalse(a.isArray());
    assertFalse(a.isTuple());
  }

  @Test
  public void testSimpleDups() {
    try {
      Projections.parse(RowSetTestUtils.projectList("a", "b", "a"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  /**
   * Whole-map projection (note, fully projected maps are
   * identical to projected simple columns at this level of
   * abstraction.)
   */
  @Test
  public void testProjectionWholeMap() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("map"));

    assertSame(TupleProjectionType.SOME, projSet.type());
    assertTrue(projSet.isProjected("map"));
    assertFalse(projSet.isProjected("another"));

    RequestedTuple mapProj = projSet.mapProjection("map");
    assertNotNull(mapProj);
    assertSame(TupleProjectionType.ALL, mapProj.type());
    assertTrue(mapProj.isProjected("foo"));

    RequestedTuple anotherProj = projSet.mapProjection("another");
    assertNotNull(anotherProj);
    assertSame(TupleProjectionType.NONE, anotherProj.type());
    assertFalse(anotherProj.isProjected("anyCol"));
  }

  /**
   * Peer inside the requested column at the qualifier to determine
   * if the given qualifier type exists.
   */
  private void assertQualifier(RequestedColumn col, Class<? extends Qualifier> qualClass) {
    RequestedColumnImpl colImpl = (RequestedColumnImpl) col;
    Qualifier qual = colImpl.qualifier(0);
    if (qual == null) {
      assertNull(qualClass);
    } else {
      assertTrue(qualClass.isInstance(qual));
    }
  }

  /**
   * Selected map projection, multiple levels, full projection
   * at leaf level.
   */
  @Test
  public void testProjectionMapSubset() {

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getCompoundPath("map", "a"));
    projCols.add(SchemaPath.getCompoundPath("map", "b"));
    projCols.add(SchemaPath.getCompoundPath("map", "map2", "x"));
    RequestedTuple projSet = Projections.parse(projCols);
    assertSame(TupleProjectionType.SOME, projSet.type());

    // Map itself is projected and has a map qualifier
    assertTrue(projSet.isProjected("map"));
    assertQualifier(projSet.get("map"), MapQualifier.class);

    // Map: an explicit map at top level

    RequestedTuple mapProj = projSet.mapProjection("map");
    assertSame(TupleProjectionType.SOME, projSet.type());
    assertTrue(mapProj.isProjected("a"));
    assertTrue(mapProj.isProjected("b"));
    assertTrue(mapProj.isProjected("map2"));
    assertFalse(mapProj.isProjected("bogus"));

    // Map b: an implied nested map

    assertTrue(mapProj.get("b").isSimple());
    RequestedTuple bMapProj = mapProj.mapProjection("b");
    assertNotNull(bMapProj);
    assertSame(TupleProjectionType.ALL, bMapProj.type());
    assertTrue(bMapProj.isProjected("foo"));

    // Map2, an nested map, has an explicit projection

    RequestedTuple map2Proj = mapProj.mapProjection("map2");
    assertNotNull(map2Proj);
    assertSame(TupleProjectionType.SOME, map2Proj.type());
    assertTrue(map2Proj.isProjected("x"));
    assertFalse(map2Proj.isProjected("bogus"));
  }

  /**
   * Project both a map member and the entire map.
   */
  @Test
  public void testProjectionMapAndSimple() {

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getCompoundPath("map", "a"));
    projCols.add(SchemaPath.getCompoundPath("map"));

    RequestedTuple projSet = Projections.parse(projCols);
    RequestedTuple mapProj = projSet.mapProjection("map");
    assertSame(TupleProjectionType.ALL, mapProj.type());
    assertTrue(mapProj.isProjected("a"));
    assertTrue(mapProj.isProjected("b"));
  }

  /**
   * Project both an entire map and a map member.
   */
    @Test
    public void testProjectionSimpleAndMap() {

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getCompoundPath("map"));
    projCols.add(SchemaPath.getCompoundPath("map", "a"));

    RequestedTuple projSet = Projections.parse(projCols);
    RequestedTuple mapProj = projSet.mapProjection("map");
    assertSame(TupleProjectionType.ALL, mapProj.type());
    assertTrue(mapProj.isProjected("a"));
    assertTrue(mapProj.isProjected("b"));
  }

  /**
   * Project both a map member and the entire map.
   */
  @Test
  public void testProjectionMapAndWildcard() {

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getCompoundPath("map", "a"));
    projCols.add(SchemaPath.getCompoundPath("map", SchemaPath.DYNAMIC_STAR));

    RequestedTuple projSet = Projections.parse(projCols);
    RequestedTuple mapProj = projSet.mapProjection("map");
    assertSame(TupleProjectionType.ALL, mapProj.type());
    assertTrue(mapProj.isProjected("a"));
    assertTrue(mapProj.isProjected("b"));
  }

  /**
   * Project both an entire map and a map member.
   */
    @Test
    public void testProjectionWildcardAndMap() {

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getCompoundPath("map", SchemaPath.DYNAMIC_STAR));
    projCols.add(SchemaPath.getCompoundPath("map", "a"));

    RequestedTuple projSet = Projections.parse(projCols);
    RequestedTuple mapProj = projSet.mapProjection("map");
    assertSame(TupleProjectionType.ALL, mapProj.type());
    assertTrue(mapProj.isProjected("a"));
    assertTrue(mapProj.isProjected("b"));
  }

  @Test
  public void testMapDetails() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a.b.c", "a.c", "d"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(2, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertFalse(a.isSimple());
    assertFalse(a.isArray());
    assertTrue(a.isTuple());

    {
      assertNotNull(a.mapProjection());
      List<RequestedColumn> aMembers = a.mapProjection().projections();
      assertEquals(2, aMembers.size());

      RequestedColumn a_b = aMembers.get(0);
      assertEquals("b", a_b.name());
      assertTrue(a_b.isTuple());

      {
        assertNotNull(a_b.mapProjection());
        List<RequestedColumn> a_bMembers = a_b.mapProjection().projections();
        assertEquals(1, a_bMembers.size());
        assertEquals("c", a_bMembers.get(0).name());
        assertTrue(a_bMembers.get(0).isSimple());
      }

      assertEquals("c", aMembers.get(1).name());
      assertTrue(aMembers.get(1).isSimple());
    }

    assertEquals("d", cols.get(1).name());
    assertTrue(cols.get(1).isSimple());
  }

  @Test
  public void testMapDups() {
    try {
      Projections.parse(
          RowSetTestUtils.projectList("a.b", "a.c", "a.b"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  /**
   * When the project list includes references to both the
   * map as a whole, and members, then the parser is forgiving
   * of duplicate map members since all members are projected.
   */

  @Test
  public void testMapDupsIgnored() {
    RequestedTuple projSet = Projections.parse(
          RowSetTestUtils.projectList("a", "a.b", "a.c", "a.b"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());
  }

  @Test
  public void testArray() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[1]", "a[3]"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertFalse(a.isSimple());
    assertFalse(a.isTuple());
    assertTrue(a.hasIndexes());
    boolean indexes[] = a.indexes();
    assertNotNull(indexes);
    assertEquals(4, indexes.length);
    assertFalse(indexes[0]);
    assertTrue(indexes[1]);
    assertFalse(indexes[2]);
    assertTrue(indexes[3]);
  }

  @Test
  public void testMultiDimArray() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[0][1][2]", "a[2][3]"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertFalse(a.isSimple());
    assertFalse(a.isTuple());
    boolean indexes[] = a.indexes();
    assertNotNull(indexes);
    assertEquals(3, indexes.length);
    assertTrue(indexes[0]);
    assertFalse(indexes[1]);
    assertTrue(indexes[2]);

    // Subsequent levels. Indexes not tracked.

    assertSame(RequestedColumn.PathType.ARRAY, a.pathType(1));
    assertSame(RequestedColumn.PathType.ARRAY, a.pathType(2));
    assertNull(a.pathType(3));
  }

  /**
   * Duplicate array entries are allowed to handle the
   * use case of a[1], a[1].z. Each element is reported once;
   * the project operator will create copies as needed.
   */
  @Test
  public void testArrayDupsIgnored() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[1]", "a[3]", "a[1]", "a[3].z"));

    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    boolean indexes[] = a.indexes();
    assertNotNull(indexes);
    assertEquals(4, indexes.length);
    assertFalse(indexes[0]);
    assertTrue(indexes[1]);
    assertFalse(indexes[2]);
    assertTrue(indexes[3]);
  }

  @Test
  public void testArrayAndSimple() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[1]", "a"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertNull(a.indexes());
  }

  @Test
  public void testSimpleAndArray() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a", "a[1]"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertFalse(a.hasIndexes());
    assertNull(a.indexes());
  }

  @Test
  // Drill syntax does not support map arrays
  public void testMapArray() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[1].x"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn a = cols.get(0);
    assertSame(RequestedColumn.PathType.ARRAY, a.pathType(0));
    assertSame(RequestedColumn.PathType.MAP, a.pathType(1));
    assertNull(a.pathType(2));

    // Column acts like an array
    assertTrue(a.isArray());
    assertTrue(a.hasIndexes());

    // And the column acts like a map
    assertTrue(a.isTuple());
    RequestedTuple aProj = a.mapProjection();
    assertSame(TupleProjectionType.SOME, aProj.type());
    assertTrue(aProj.isProjected("x"));
    assertFalse(aProj.isProjected("y"));
  }

  @Test
  // Drill syntax does not support map arrays
  public void testMap2DArray() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[1][2].x"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    RequestedColumn a = cols.get(0);
    assertSame(RequestedColumn.PathType.ARRAY, a.pathType(0));
    assertSame(RequestedColumn.PathType.ARRAY, a.pathType(1));
    assertSame(RequestedColumn.PathType.MAP, a.pathType(2));
    assertNull(a.pathType(3));

    // Column acts like an array
    assertTrue(a.isArray());
    assertTrue(a.hasIndexes());

    // Note that the multiple dimensions are inferred only through
    // the multiple levels of qualifiers.

    // And the column acts like a map
    assertTrue(a.isTuple());
    RequestedTuple aProj = a.mapProjection();
    assertSame(TupleProjectionType.SOME, aProj.type());
    assertTrue(aProj.isProjected("x"));
    assertFalse(aProj.isProjected("y"));
  }

  /**
   * Projection does not allow m.a and m[0]. The result implies a map
   * array. But, with inconsistent semantics: m[0] means to get the first full map,
   * while m.a means to get the a members for all array elements.
   */
  @Test
  public void testArrayAndMap() {
    try {
      Projections.parse(
          RowSetTestUtils.projectList("m.a", "m[0]"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testMapAndArray() {
    try {
      Projections.parse(
          RowSetTestUtils.projectList("m[0]", "m.a"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  @Ignore("The schema parser does not support Python key syntax yet")
  public void testDictKey() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[`key`]"));
    List<RequestedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());
  }

}
