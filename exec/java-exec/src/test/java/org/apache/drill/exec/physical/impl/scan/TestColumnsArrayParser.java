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
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnsArrayParser;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ProjectionType;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnType;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

public class TestColumnsArrayParser extends SubOperatorTest {

  /**
   * Test the special "columns" column that asks to return all columns
   * as an array. No need for early schema. This case is special: it actually
   * creates the one and only table column to match the desired output column.
   */

  @Test
  public void testColumnsArray() {
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());

    // Add columns parser

    builder.addParser(new ColumnsArrayParser());

    // Simulate SELECT columns ...

    builder.projectedCols(ScanTestUtils.projectList("columns"));

    // Build the planner and verify

    ScanLevelProjection scanProj = builder.build();
    assertFalse(scanProj.isProjectAll());
    assertEquals(ProjectionType.COLUMNS_ARRAY, scanProj.projectType());
    assertEquals(1, scanProj.requestedCols().size());
    assertTrue(scanProj.tableColNames().isEmpty());

    assertEquals(1, scanProj.outputCols().size());
    assertEquals("columns", scanProj.outputCols().get(0).name());

    // Verify bindings

    assertSame(scanProj.outputCols().get(0), scanProj.requestedCols().get(0).resolution());
    assertSame(scanProj.outputCols().get(0).source(), scanProj.requestedCols().get(0));

    // Verify column type

    assertEquals(ColumnType.COLUMNS_ARRAY, scanProj.outputCols().get(0).columnType());
  }


  /**
   * The `columns` column is special: can't be used with other column names.
   * Make sure that the rule <i>does not</i> apply to implicit columns.
   */

  @Test
  public void testMetadataColumnsWithColumnsArray() {
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());

    // Add columns parser

    builder.addParser(new ColumnsArrayParser());

    builder.projectedCols(ScanTestUtils.projectList("filename", "columns", "suffix"));

    ScanLevelProjection scanProj = builder.build();
    assertFalse(scanProj.isProjectAll());
    assertEquals(ProjectionType.COLUMNS_ARRAY, scanProj.projectType());

    assertEquals(3, scanProj.outputCols().size());

    assertEquals("filename", scanProj.outputCols().get(0).name());
    assertEquals("columns", scanProj.outputCols().get(1).name());
    assertEquals("suffix", scanProj.outputCols().get(2).name());

    // Verify column type

    assertEquals(ColumnType.FILE_METADATA, scanProj.outputCols().get(0).columnType());
    assertEquals(ColumnType.COLUMNS_ARRAY, scanProj.outputCols().get(1).columnType());
    assertEquals(ColumnType.FILE_METADATA, scanProj.outputCols().get(2).columnType());
  }


  /**
   * The `columns` column is special; can't include both `columns` and
   * a named column in the same project.
   * <p>
   * TODO: This should only be true for text readers, make this an option.
   */

  @Test
  public void testErrorColumnsArrayAndColumn() {
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());

    // Add columns parser

    builder.addParser(new ColumnsArrayParser());

    builder.projectedCols(ScanTestUtils.projectList("columns", "a"));
    try {
      builder.build();
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
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());

    // Add columns parser

    builder.addParser(new ColumnsArrayParser());

    builder.projectedCols(ScanTestUtils.projectList("a", "columns"));
    try {
      builder.build();
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
    ScanLevelProjection.ScanProjectionBuilder builder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());

    // Add columns parser

    builder.addParser(new ColumnsArrayParser());

    builder.projectedCols(ScanTestUtils.projectList("columns", "columns"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

}
