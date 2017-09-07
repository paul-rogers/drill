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
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestResultSetLoaderProjection extends SubOperatorTest {

  @Test
  public void testProjectionMap() {

    // Null map means everything is projected

    {
      ProjectionSet projSet = ProjectionSetImpl.parse(null);
      assertTrue(projSet instanceof NullProjectionSet);
      assertTrue(projSet.isProjected("foo"));
    }

    // Empty list means everything is projected

    {
      ProjectionSet projSet = ProjectionSetImpl.parse(new ArrayList<SchemaPath>());
      assertTrue(projSet instanceof NullProjectionSet);
      assertTrue(projSet.isProjected("foo"));
    }

    // Simple non-map columns

    {
      List<SchemaPath> projCols = new ArrayList<>();
      projCols.add(SchemaPath.getSimplePath("foo"));
      projCols.add(SchemaPath.getSimplePath("bar"));
      ProjectionSet projSet = ProjectionSetImpl.parse(projCols);
      assertTrue(projSet instanceof ProjectionSetImpl);
      assertTrue(projSet.isProjected("foo"));
      assertTrue(projSet.isProjected("bar"));
      assertFalse(projSet.isProjected("mumble"));
    }

    // Whole-map projection (note, fully projected maps are
    // identical to projected simple columns at this level of
    // abstraction.)

    {
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
      assertNull(projSet.mapProjection("another"));
    }

    // Selected map projection, multiple levels, full projection
    // at leaf level.

    {
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
  }

  /**
   * Test imposing a selection mask between the client and the underlying
   * vector container.
   */

  @Test
  public void testSelection() {
    List<String> selection = Lists.newArrayList("c", "b");
    ResultSetOptions options = new OptionBuilder()
        .setProjection(selection)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();
    assertTrue(rootWriter instanceof LogicalRowSetLoader);
    TupleMetadata schema = rootWriter.schema();
    schema.add(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("b", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.REQUIRED));
    schema.add(SchemaBuilder.columnSchema("d", MinorType.INT, DataMode.REQUIRED));

    assertEquals(4, schema.size());
    assertEquals("a", schema.column(0).getName());
    assertEquals("b", schema.column(1).getName());
    assertEquals("c", schema.column(2).getName());
    assertEquals("d", schema.column(3).getName());
    assertEquals(0, schema.index("A"));
    assertEquals(3, schema.index("d"));
    assertEquals(-1, schema.index("e"));

    rsLoader.startBatch();
    for (int i = 1; i < 3; i++) {
      rsLoader.start();
      assertNull(rootWriter.column(0));
      rootWriter.scalar(1).setInt(i);
      rootWriter.scalar(2).setInt(i * 10);
      assertNull(rootWriter.column(3));
      rsLoader.saveRow();
    }

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("b", MinorType.INT)
        .add("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, 10)
        .addRow(2, 20)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(rsLoader.harvest()));
    rsLoader.close();
  }

  // TODO: Add a method that resets current row to default values

  // TODO: Test initial vector allocation

}