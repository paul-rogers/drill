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

import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayParser;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ProjectionType;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnType;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

public class TestColumnsArrayParser extends SubOperatorTest {

  private ScanLevelProjection buildProj(String... queryCols) {
    return new ProjectionFixture()

        // Add columns parser

        .withColumnsArrayParser()

        // Simulate SELECT columns ...

        .projectedCols(queryCols)

        // Build the planner and verify

        .build();
  }

  /**
   * Test the special "columns" column that asks to return all columns
   * as an array. No need for early schema. This case is special: it actually
   * creates the one and only table column to match the desired output column.
   */

  @Test
  public void testColumnsArray() {
    ScanLevelProjection scanProj = buildProj(ColumnsArrayParser.COLUMNS_COL);
    assertFalse(scanProj.isProjectAll());
    assertEquals(ProjectionType.COLUMNS_ARRAY, scanProj.projectType());
    assertEquals(1, scanProj.requestedCols().size());
    assertTrue(scanProj.tableColNames().isEmpty());

    assertEquals(1, scanProj.outputCols().size());
    assertEquals("columns", scanProj.outputCols().get(0).name());

    // Verify bindings

//    assertSame(scanProj.outputCols().get(0), scanProj.requestedCols().get(0).resolution());
    assertSame(scanProj.outputCols().get(0).source(), scanProj.requestedCols().get(0));

    // Verify column type

    assertEquals(ColumnType.COLUMNS_ARRAY, scanProj.outputCols().get(0).columnType());
  }

  /**
   * The `columns` column is special; can't include both `columns` and
   * a named column in the same project.
   * <p>
   * TODO: This should only be true for text readers, make this an option.
   */

  @Test
  public void testErrorColumnsArrayAndColumn() {
    try {
      buildProj(ColumnsArrayParser.COLUMNS_COL, "a");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * Exclude a column and `columns` (reversed order of previous test).
   */

  @Test
  public void testErrorColumnAndColumnsArray() {
    try {
      buildProj("a", ColumnsArrayParser.COLUMNS_COL);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * Can't request `columns` twice.
   */

  @Test
  public void testErrorTwoColumnsArray() {
    try {
      buildProj(ColumnsArrayParser.COLUMNS_COL, ColumnsArrayParser.COLUMNS_COL);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  // TODO: Test Columns element projection
}
