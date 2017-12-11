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

import org.apache.drill.common.expression.SchemaPath;
import org.junit.Test;

public class TestProjectionSet {


  @Test
  public void testProjectionAll() {

    // Null map means everything is projected

    ProjectionSet projSet = ProjectionSetImpl.parse(null);
    assertTrue(projSet instanceof NullProjectionSet);
    assertTrue(projSet.isProjected("foo"));
  }

  @Test
  public void testProjectionNone() {

    // Empty list means nothing is projected

    ProjectionSet projSet = ProjectionSetImpl.parse(new ArrayList<SchemaPath>());
    assertTrue(projSet instanceof NullProjectionSet);
    assertFalse(projSet.isProjected("foo"));
  }

  @Test
  public void testProjectionSimple() {

    // Simple non-map columns

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getSimplePath("foo"));
    projCols.add(SchemaPath.getSimplePath("bar"));
    ProjectionSet projSet = ProjectionSetImpl.parse(projCols);
    assertTrue(projSet instanceof ProjectionSetImpl);
    assertTrue(projSet.isProjected("foo"));
    assertTrue(projSet.isProjected("bar"));
    assertFalse(projSet.isProjected("mumble"));
  }

  @Test
  public void testProjectionWholeMap() {

    // Whole-map projection (note, fully projected maps are
    // identical to projected simple columns at this level of
    // abstraction.)

    List<SchemaPath> projCols = new ArrayList<>();
    projCols.add(SchemaPath.getSimplePath("map"));
    ProjectionSet projSet = ProjectionSetImpl.parse(projCols);
    assertTrue(projSet instanceof ProjectionSetImpl);
    assertTrue(projSet.isProjected("map"));
    assertFalse(projSet.isProjected("another"));
    ProjectionSet mapProj = projSet.mapProjection("map");
    assertNotNull(mapProj);
    assertTrue(mapProj instanceof NullProjectionSet);
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
    ProjectionSet projSet = ProjectionSetImpl.parse(projCols);
    assertTrue(projSet instanceof ProjectionSetImpl);
    assertTrue(projSet.isProjected("map"));

    // Map: an explicit map at top level

    ProjectionSet mapProj = projSet.mapProjection("map");
    assertTrue(mapProj instanceof ProjectionSetImpl);
    assertTrue(mapProj.isProjected("a"));
    assertTrue(mapProj.isProjected("b"));
    assertTrue(mapProj.isProjected("map2"));
    assertFalse(projSet.isProjected("bogus"));

    // Map b: an implied nested map

    ProjectionSet bMapProj = mapProj.mapProjection("b");
    assertNotNull(bMapProj);
    assertTrue(bMapProj instanceof NullProjectionSet);
    assertTrue(bMapProj.isProjected("foo"));

    // Map2, an nested map, has an explicit projection

    ProjectionSet map2Proj = mapProj.mapProjection("map2");
    assertNotNull(map2Proj);
    assertTrue(map2Proj instanceof ProjectionSetImpl);
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

      ProjectionSet projSet = ProjectionSetImpl.parse(projCols);
      assertTrue(projSet instanceof ProjectionSetImpl);
      assertTrue(projSet.isProjected("map"));

      ProjectionSet mapProj = projSet.mapProjection("map");
      assertTrue(mapProj instanceof NullProjectionSet);
      assertTrue(mapProj.isProjected("a"));

      // Didn't ask for b, but did ask for whole map.

      assertTrue(mapProj.isProjected("b"));
    }

    // Now the other way around.

    {
      List<SchemaPath> projCols = new ArrayList<>();
      projCols.add(SchemaPath.getCompoundPath("map"));
      projCols.add(SchemaPath.getCompoundPath("map", "a"));

      ProjectionSet projSet = ProjectionSetImpl.parse(projCols);
      assertTrue(projSet instanceof ProjectionSetImpl);
      assertTrue(projSet.isProjected("map"));

      ProjectionSet mapProj = projSet.mapProjection("map");
      assertTrue(mapProj instanceof NullProjectionSet);
      assertTrue(mapProj.isProjected("a"));
      assertTrue(mapProj.isProjected("b"));
    }
  }

}
