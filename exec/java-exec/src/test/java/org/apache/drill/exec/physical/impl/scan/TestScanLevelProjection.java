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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ProjectionType;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnType;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

/**
 * Test the level of projection done at the level of the scan as a whole;
 * before knowledge of table "implicit" columns or the specific table schema.
 */

public class TestScanLevelProjection extends SubOperatorTest {

  /**
   * Basic test: select a set of columns (a, b, c) when the
   * data source has an early schema of (a, c, d). (a, c) are
   * projected, (d) is null.
   */

  @Test
  public void testBasics() {
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder();

    // Simulate SELECT a, b, c ...

    builder.projectedCols(ScanTestUtils.projectList("a", "b", "c"));

    // Build the projection plan and verify

    ScanLevelProjection scanProj = builder.build();
    assertFalse(scanProj.isProjectAll());
    assertEquals(ProjectionType.LIST, scanProj.projectType());

    assertEquals(3, scanProj.requestedCols().size());
    assertEquals("a", scanProj.requestedCols().get(0).name());
    assertEquals("b", scanProj.requestedCols().get(1).name());
    assertEquals("c", scanProj.requestedCols().get(2).name());

    assertEquals(3, scanProj.outputCols().size());
    assertEquals("a", scanProj.outputCols().get(0).name());
    assertEquals("b", scanProj.outputCols().get(1).name());
    assertEquals("c", scanProj.outputCols().get(2).name());

    // Verify bindings

    assertSame(scanProj.outputCols().get(0), scanProj.requestedCols().get(0).resolution());
    assertSame(scanProj.outputCols().get(1), scanProj.requestedCols().get(1).resolution());
    assertSame(scanProj.outputCols().get(2), scanProj.requestedCols().get(2).resolution());

    assertSame(scanProj.outputCols().get(0).source(), scanProj.requestedCols().get(0));
    assertSame(scanProj.outputCols().get(1).source(), scanProj.requestedCols().get(1));
    assertSame(scanProj.outputCols().get(2).source(), scanProj.requestedCols().get(2));

    // Verify column type

    assertEquals(ColumnType.TABLE, scanProj.outputCols().get(0).columnType());

    // Table column selection

    assertEquals(3, scanProj.tableColNames().size());
    assertEquals("a", scanProj.tableColNames().get(0));
    assertEquals("b", scanProj.tableColNames().get(1));
    assertEquals("c", scanProj.tableColNames().get(2));
  }

  /**
   * Simulate a SELECT * query: selects all data source columns in the order
   * defined by the data source.
   */

  @Test
  public void testProjectAll() {
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder();

    // Simulate SELECT * ...

    builder.projectAll();

    ScanLevelProjection scanProj = builder.build();
    assertTrue(scanProj.isProjectAll());
    assertEquals(ProjectionType.WILDCARD, scanProj.projectType());
    assertTrue(scanProj.tableColNames().isEmpty());

    assertEquals(1, scanProj.requestedCols().size());
    assertTrue(scanProj.requestedCols().get(0).isWildcard());

    assertEquals(1, scanProj.outputCols().size());
    assertEquals(ScanLevelProjection.WILDCARD, scanProj.outputCols().get(0).name());

    // Verify bindings

    assertSame(scanProj.outputCols().get(0), scanProj.requestedCols().get(0).resolution());
    assertSame(scanProj.outputCols().get(0).source(), scanProj.requestedCols().get(0));

    // Verify column type

    assertEquals(ColumnType.WILDCARD, scanProj.outputCols().get(0).columnType());
  }

  /**
   * Simulate a SELECT * query by passing "*" as a column name.
   */

  @Test
  public void testWildcard() {
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder();

    // Simulate SELECT * ...

    builder.projectedCols(ScanTestUtils.projectList(ScanLevelProjection.WILDCARD));

    ScanLevelProjection scanProj = builder.build();
    assertTrue(scanProj.isProjectAll());
    assertEquals(ProjectionType.WILDCARD, scanProj.projectType());
    assertEquals(1, scanProj.requestedCols().size());
    assertTrue(scanProj.requestedCols().get(0).isWildcard());

    assertEquals(1, scanProj.outputCols().size());
    assertEquals(ScanLevelProjection.WILDCARD, scanProj.outputCols().get(0).name());

    // Verify bindings

    assertSame(scanProj.outputCols().get(0), scanProj.requestedCols().get(0).resolution());
    assertSame(scanProj.outputCols().get(0).source(), scanProj.requestedCols().get(0));

    // Verify column type

    assertEquals(ColumnType.WILDCARD, scanProj.outputCols().get(0).columnType());
  }

  /**
   * Can't include both a wildcard and a column name.
   */

  @Test
  public void testErrorWildcardAndColumns() {
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder();

    builder.projectedCols(ScanTestUtils.projectList(ScanLevelProjection.WILDCARD, "a"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * Can't include both a column name and a wildcard.
   */
  @Test
  public void testErrorColumnAndWildcard() {
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder();

    builder.projectedCols(ScanTestUtils.projectList("a", ScanLevelProjection.WILDCARD));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
  /**
   * Can't include a wildcard twice.
   * <p>
   * Note: Drill actually allows this, but the work should be done
   * in the project operator; scan should see at most one wildcard.
   */

  @Test
  public void testErrorTwoWildcards() {
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder();

    builder.projectedCols(ScanTestUtils.projectList(ScanLevelProjection.WILDCARD, ScanLevelProjection.WILDCARD));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}
