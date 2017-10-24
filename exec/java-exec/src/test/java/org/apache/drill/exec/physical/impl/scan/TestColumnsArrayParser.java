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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayManager;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayManager.UnresolvedColumnsArrayColumn;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayParser;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestColumnsArrayParser extends SubOperatorTest {

  /**
   * Test the special "columns" column that asks to return all columns
   * as an array. No need for early schema. This case is special: it actually
   * creates the one and only table column to match the desired output column.
   */

  @Test
  public void testColumnsArray() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL),
        Lists.newArrayList(new ColumnsArrayParser()));

    assertFalse(scanProj.projectAll());
    assertEquals(1, scanProj.requestedCols().size());

    assertEquals(1, scanProj.columns().size());
    assertEquals(ColumnsArrayManager.COLUMNS_COL, scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(0).nodeType());
  }

  @Test
  public void testColumnsArrayCaseInsensitive() {

    // Sic: case variation of standard name

    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList("Columns"),
        Lists.newArrayList(new ColumnsArrayParser()));

    assertFalse(scanProj.projectAll());
    assertEquals(1, scanProj.requestedCols().size());

    assertEquals(1, scanProj.columns().size());
    assertEquals("Columns", scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(0).nodeType());
  }

  @Test
  public void testColumnsElements() {

   ScanLevelProjection scanProj = new ScanLevelProjection(
        Lists.newArrayList(
            SchemaPath.parseFromString(ColumnsArrayManager.COLUMNS_COL + "[3]"),
            SchemaPath.parseFromString(ColumnsArrayManager.COLUMNS_COL + "[1]")),
        Lists.newArrayList(new ColumnsArrayParser()));

    assertFalse(scanProj.projectAll());
    assertEquals(2, scanProj.requestedCols().size());

    assertEquals(1, scanProj.columns().size());
    assertEquals(ColumnsArrayManager.COLUMNS_COL, scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(0).nodeType());
    UnresolvedColumnsArrayColumn colsCol = (UnresolvedColumnsArrayColumn) scanProj.columns().get(0);
    boolean indexes[] = colsCol.selectedIndexes();
    assertNotNull(indexes);
    assertEquals(4, indexes.length);
    assertFalse(indexes[0]);
    assertTrue(indexes[1]);
    assertFalse(indexes[0]);
    assertTrue(indexes[1]);
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
      new ScanLevelProjection(
          ScanTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL, "a"),
          Lists.newArrayList(new ColumnsArrayParser()));
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
      new ScanLevelProjection(
          ScanTestUtils.projectList("a", ColumnsArrayManager.COLUMNS_COL),
          Lists.newArrayList(new ColumnsArrayParser()));
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
      new ScanLevelProjection(
          ScanTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL, ColumnsArrayManager.COLUMNS_COL),
          Lists.newArrayList(new ColumnsArrayParser()));
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorSimpleAndIndex() {
    try {
      new ScanLevelProjection(
          Lists.newArrayList(
            SchemaPath.getSimplePath(ColumnsArrayManager.COLUMNS_COL),
            SchemaPath.parseFromString(ColumnsArrayManager.COLUMNS_COL + "[1]")),
          Lists.newArrayList(new ColumnsArrayParser()));
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * The `columns` column is special: can't be used with other column names.
   * Make sure that the rule <i>does not</i> apply to implicit columns.
   */

  @Test
  public void testMetadataColumnsWithColumnsArray() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList(ScanTestUtils.FILE_NAME_COL,
            ColumnsArrayManager.COLUMNS_COL,
            ScanTestUtils.SUFFIX_COL),
        Lists.newArrayList(new ColumnsArrayParser(),
            metadataManager.projectionParser()));

    assertFalse(scanProj.projectAll());

    assertEquals(3, scanProj.columns().size());

    assertEquals(ScanTestUtils.FILE_NAME_COL, scanProj.columns().get(0).name());
    assertEquals(ColumnsArrayManager.COLUMNS_COL, scanProj.columns().get(1).name());
    assertEquals(ScanTestUtils.SUFFIX_COL, scanProj.columns().get(2).name());

    // Verify column type

    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(0).nodeType());
    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(1).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(2).nodeType());
  }

  // TODO: Test Columns element projection
}
