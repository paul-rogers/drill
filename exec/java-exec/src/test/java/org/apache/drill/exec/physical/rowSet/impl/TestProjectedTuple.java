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
package org.apache.drill.exec.physical.rowSet.impl;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.rowSet.project.NullProjectedTuple;
import org.apache.drill.exec.physical.rowSet.project.ProjectedTuple;
import org.apache.drill.exec.physical.rowSet.project.ProjectedTuple.ProjectedColumn;
import org.apache.drill.exec.physical.rowSet.project.ProjectedTupleImpl;
import org.junit.Test;

public class TestProjectedTuple {

  @Test
  public void testProjectionAll() {

    // Null map means everything is projected

    ProjectedTuple projSet = ProjectedTupleImpl.parse(null);
    assertTrue(projSet instanceof NullProjectedTuple);
    assertTrue(projSet.isProjected("foo"));
  }


  /**
   * Test an empty projection which occurs in a
   * SELECT COUNT(*) query.
   */

  @Test
  public void testProjectionNone() {

    // Empty list means nothing is projected

    ProjectedTuple projSet = ProjectedTupleImpl.parse(new ArrayList<SchemaPath>());
    assertTrue(projSet instanceof NullProjectedTuple);
    List<ProjectedColumn> cols = projSet.projections();
    assertEquals(0, cols.size());
    assertFalse(projSet.isProjected("foo"));
  }

  @Test
  public void testProjectionSimple() {

    // Simple non-map columns

    ProjectedTuple projSet = ProjectedTupleImpl.parse(
        RowSetTestUtils.projectList("a", "b", "c"));
    assertTrue(projSet instanceof ProjectedTupleImpl);
    assertTrue(projSet.isProjected("a"));
    assertTrue(projSet.isProjected("b"));
    assertFalse(projSet.isProjected("d"));

    List<ProjectedColumn> cols = projSet.projections();
    assertEquals(3, cols.size());

    ProjectedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isSimple());
    assertFalse(a.isWildcard());
    assertNull(a.mapProjection());
    assertNull(a.indexes());

    assertEquals("b", cols.get(1).name());
    assertTrue(cols.get(1).isSimple());

    assertEquals("c", cols.get(2).name());
    assertTrue(cols.get(2).isSimple());
  }

  @Test
  public void testProjectionWholeMap() {

    // Whole-map projection (note, fully projected maps are
    // identical to projected simple columns at this level of
    // abstraction.)

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getSimplePath("map"));
    ProjectedTuple projSet = ProjectedTupleImpl.parse(projCols);
    assertTrue(projSet instanceof ProjectedTupleImpl);
    assertTrue(projSet.isProjected("map"));
    assertFalse(projSet.isProjected("another"));
    ProjectedTuple mapProj = projSet.mapProjection("map");
    assertNotNull(mapProj);
    assertTrue(mapProj instanceof NullProjectedTuple);
    assertTrue(mapProj.isProjected("foo"));
    assertNotNull(projSet.mapProjection("another"));
    assertFalse(projSet.mapProjection("another").isProjected("anyCol"));
  }

  @Test
  public void testProjectionMapSubset() {

    // Selected map projection, multiple levels, full projection
    // at leaf level.

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getCompoundPath("map", "a"));
    projCols.add(SchemaPath.getCompoundPath("map", "b"));
    projCols.add(SchemaPath.getCompoundPath("map", "map2", "x"));
    ProjectedTuple projSet = ProjectedTupleImpl.parse(projCols);
    assertTrue(projSet instanceof ProjectedTupleImpl);
    assertTrue(projSet.isProjected("map"));

    // Map: an explicit map at top level

    ProjectedTuple mapProj = projSet.mapProjection("map");
    assertTrue(mapProj instanceof ProjectedTupleImpl);
    assertTrue(mapProj.isProjected("a"));
    assertTrue(mapProj.isProjected("b"));
    assertTrue(mapProj.isProjected("map2"));
    assertFalse(projSet.isProjected("bogus"));

    // Map b: an implied nested map

    ProjectedTuple bMapProj = mapProj.mapProjection("b");
    assertNotNull(bMapProj);
    assertTrue(bMapProj instanceof NullProjectedTuple);
    assertTrue(bMapProj.isProjected("foo"));

    // Map2, an nested map, has an explicit projection

    ProjectedTuple map2Proj = mapProj.mapProjection("map2");
    assertNotNull(map2Proj);
    assertTrue(map2Proj instanceof ProjectedTupleImpl);
    assertTrue(map2Proj.isProjected("x"));
    assertFalse(map2Proj.isProjected("bogus"));
  }

  @Test
  public void testProjectionMapFieldAndMap() {

    // Project both a map member and the entire map.

    {
      List<SchemaPath> projCols = new ArrayList<>();
      projCols.add(SchemaPath.getCompoundPath("map", "a"));
      projCols.add(SchemaPath.getCompoundPath("map"));

      ProjectedTuple projSet = ProjectedTupleImpl.parse(projCols);
      assertTrue(projSet instanceof ProjectedTupleImpl);
      assertTrue(projSet.isProjected("map"));

      ProjectedTuple mapProj = projSet.mapProjection("map");
      assertTrue(mapProj instanceof NullProjectedTuple);
      assertTrue(mapProj.isProjected("a"));

      // Didn't ask for b, but did ask for whole map.

      assertTrue(mapProj.isProjected("b"));
    }

    // Now the other way around.

    {
      List<SchemaPath> projCols = new ArrayList<>();
      projCols.add(SchemaPath.getCompoundPath("map"));
      projCols.add(SchemaPath.getCompoundPath("map", "a"));

      ProjectedTuple projSet = ProjectedTupleImpl.parse(projCols);
      assertTrue(projSet instanceof ProjectedTupleImpl);
      assertTrue(projSet.isProjected("map"));

      ProjectedTuple mapProj = projSet.mapProjection("map");
      assertTrue(mapProj instanceof NullProjectedTuple);
      assertTrue(mapProj.isProjected("a"));
      assertTrue(mapProj.isProjected("b"));
    }
  }

  @Test
  public void testMapDetails() {
    ProjectedTuple projSet = ProjectedTupleImpl.parse(
        RowSetTestUtils.projectList("a.b.c", "a.c", "d"));
    List<ProjectedColumn> cols = projSet.projections();
    assertEquals(2, cols.size());

    ProjectedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertFalse(a.isSimple());
    assertFalse(a.isArray());
    assertTrue(a.isTuple());

    {
      assertNotNull(a.mapProjection());
      List<ProjectedColumn> aMembers = a.mapProjection().projections();
      assertEquals(2, aMembers.size());

      ProjectedColumn a_b = aMembers.get(0);
      assertEquals("b", a_b.name());
      assertTrue(a_b.isTuple());

      {
        assertNotNull(a_b.mapProjection());
        List<ProjectedColumn> a_bMembers = a_b.mapProjection().projections();
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
      ProjectedTupleImpl.parse(
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
    ProjectedTuple projSet = ProjectedTupleImpl.parse(
          RowSetTestUtils.projectList("a", "a.b", "a.c", "a.b"));
    List<ProjectedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());
  }

  @Test
  public void testWildcard() {
    ProjectedTuple projSet = ProjectedTupleImpl.parse(
        RowSetTestUtils.projectList(SchemaPath.WILDCARD));
    List<ProjectedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    ProjectedColumn wildcard = cols.get(0);
    assertEquals(SchemaPath.WILDCARD, wildcard.name());
    assertTrue(wildcard.isSimple());
    assertTrue(wildcard.isWildcard());
    assertNull(wildcard.mapProjection());
    assertNull(wildcard.indexes());
  }

  @Test
  public void testSimpleDups() {
    try {
      ProjectedTupleImpl.parse(RowSetTestUtils.projectList("a", "b", "a"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testArray() {
    ProjectedTuple projSet = ProjectedTupleImpl.parse(
        RowSetTestUtils.projectList("a[1]", "a[3]"));
    List<ProjectedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    ProjectedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertFalse(a.isSimple());
    assertFalse(a.isTuple());
    boolean indexes[] = a.indexes();
    assertNotNull(indexes);
    assertEquals(4, indexes.length);
    assertFalse(indexes[0]);
    assertTrue(indexes[1]);
    assertFalse(indexes[2]);
    assertTrue(indexes[3]);
  }

  @Test
  public void testArrayDups() {
    try {
      ProjectedTupleImpl.parse(
          RowSetTestUtils.projectList("a[1]", "a[3]", "a[1]"));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testArrayAndSimple() {
    ProjectedTuple projSet = ProjectedTupleImpl.parse(
        RowSetTestUtils.projectList("a[1]", "a"));
    List<ProjectedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    ProjectedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertNull(a.indexes());
  }

  @Test
  public void testSimpleAndArray() {
    ProjectedTuple projSet = ProjectedTupleImpl.parse(
        RowSetTestUtils.projectList("a", "a[1]"));
    List<ProjectedColumn> cols = projSet.projections();
    assertEquals(1, cols.size());

    ProjectedColumn a = cols.get(0);
    assertEquals("a", a.name());
    assertTrue(a.isArray());
    assertNull(a.indexes());
    assertTrue(projSet.isProjected("a"));
    assertFalse(projSet.isProjected("foo"));
  }
}
