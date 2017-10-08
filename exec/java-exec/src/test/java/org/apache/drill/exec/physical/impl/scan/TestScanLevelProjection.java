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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.ScanLevelProjection.FileMetadata;
import org.apache.drill.exec.physical.impl.scan.ScanLevelProjection.ProjectionType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Test the level of projection done at the level of the scan as a whole;
 * before knowledge of table "implicit" columns or the specific table schema.
 */

public class TestScanLevelProjection extends SubOperatorTest {

  static List<SchemaPath> projectList(String... names) {
    List<SchemaPath> selected = new ArrayList<>();
    for (String name: names) {
      selected.add(SchemaPath.getSimplePath(name));
    }
    return selected;
  }

  /**
   * Basic test: select a set of columns (a, b, c) when the
   * data source has an early schema of (a, c, d). (a, c) are
   * projected, (d) is null.
   */

  @Test
  public void testBasics() {
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.projectedCols(projectList("a", "b", "c"));

    // Build the projection plan and verify

    ScanLevelProjection scanProj = builder.build();
    assertFalse(scanProj.isProjectAll());
    assertEquals(ProjectionType.LIST, scanProj.projectType());
    assertFalse(scanProj.hasMetadata());
    assertTrue(scanProj.useLegacyWildcardPartition());

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
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());
    builder.useLegacyWildcardExpansion(false);

    // Simulate SELECT * ...

    builder.projectAll();

    ScanLevelProjection scanProj = builder.build();
    assertTrue(scanProj.isProjectAll());
    assertEquals(ProjectionType.WILDCARD, scanProj.projectType());
    assertFalse(scanProj.useLegacyWildcardPartition());
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
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());
    builder.useLegacyWildcardExpansion(false);

    // Simulate SELECT * ...

    builder.projectedCols(projectList(ScanLevelProjection.WILDCARD));

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
   * Test the special "columns" column that asks to return all columns
   * as an array. No need for early schema. This case is special: it actually
   * creates the one and only table column to match the desired output column.
   */

  @Test
  public void testColumnsArray() {
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    // Simulate SELECT columns ...

    builder.projectedCols(projectList("columns"));

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
   * Test including file metadata (AKA "implicit columns") in the project
   * list.
   */

  @Test
  public void testFileMetadataColumnSelection() {
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.projectedCols(projectList("a", "fqn",
                                 "filEPath", // Sic, to test case sensitivity
                                 "filename", "suffix"));

    ScanLevelProjection scanProj = builder.build();
    assertFalse(scanProj.isProjectAll());
    assertEquals(5, scanProj.requestedCols().size());

    assertEquals(5, scanProj.outputCols().size());

    assertEquals("a", scanProj.outputCols().get(0).name());
    assertEquals("fqn", scanProj.outputCols().get(1).name());
    assertEquals("filEPath", scanProj.outputCols().get(2).name());
    assertEquals("filename", scanProj.outputCols().get(3).name());
    assertEquals("suffix", scanProj.outputCols().get(4).name());

    assertEquals(MinorType.VARCHAR, ((ScanOutputColumn.TypedColumn) (scanProj.outputCols().get(1))).type().getMinorType());
    assertEquals(DataMode.REQUIRED, ((ScanOutputColumn.TypedColumn) (scanProj.outputCols().get(1))).type().getMode());

    // Verify bindings

    assertSame(scanProj.outputCols().get(1), scanProj.requestedCols().get(1).resolution());
    assertSame(scanProj.outputCols().get(1).source(), scanProj.requestedCols().get(1));

    // Verify column type

    assertEquals(ColumnType.TABLE, scanProj.outputCols().get(0).columnType());
    assertEquals(ColumnType.FILE_METADATA, scanProj.outputCols().get(1).columnType());
  }

  /**
   * The `columns` column is special: can't be used with other column names.
   * Make sure that the rule <i>does not</i> apply to implicit columns.
   */

  @Test
  public void testMetadataColumnsWithColumnsArray() {
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    builder.projectedCols(projectList("filename", "columns", "suffix"));

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
   * Verify that partition columns, in any case, work.
   */

  @Test
  public void testPartitionColumnSelection() {
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    builder.projectedCols(projectList("dir2", "DIR1", "dir0", "a"));

    ScanLevelProjection scanProj = builder.build();

    assertEquals(4, scanProj.outputCols().size());
    assertEquals("dir2", scanProj.outputCols().get(0).name());
    assertEquals("DIR1", scanProj.outputCols().get(1).name());
    assertEquals("dir0", scanProj.outputCols().get(2).name());
    assertEquals("a", scanProj.outputCols().get(3).name());

    // Verify bindings

    assertSame(scanProj.outputCols().get(1), scanProj.requestedCols().get(1).resolution());
    assertSame(scanProj.outputCols().get(1).source(), scanProj.requestedCols().get(1));

    // Verify column type

    assertEquals(ColumnType.PARTITION, scanProj.outputCols().get(0).columnType());

    // Verify data type

    assertEquals(MinorType.VARCHAR, ((ScanOutputColumn.TypedColumn) (scanProj.outputCols().get(0))).type().getMinorType());
    assertEquals(DataMode.OPTIONAL, ((ScanOutputColumn.TypedColumn) (scanProj.outputCols().get(0))).type().getMode());
  }

  /**
   * Can't explicitly list file metadata columns with a wildcard in
   * "legacy" mode: that is, when the wildcard already includes partition
   * and file metadata columns.
   */

  @Test
  public void testErrorWildcardLegacyAndFileMetaata() {
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());
    builder.useLegacyWildcardExpansion(true);

    builder.projectedCols(projectList("filename", ScanLevelProjection.WILDCARD));

    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Can't include both a wildcard and a column name.
   */

  @Test
  public void testErrorWildcardAndColumns() {
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    builder.projectedCols(projectList(ScanLevelProjection.WILDCARD, "a"));
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
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    builder.projectedCols(projectList("a", ScanLevelProjection.WILDCARD));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * Can't include both a wildcard and a partition column.
   */

  @Test
  public void testErrorWildcardLegacyAndPartition() {
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());
    builder.useLegacyWildcardExpansion(true);

    builder.projectedCols(projectList(ScanLevelProjection.WILDCARD, "dir8"));

    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // expected
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
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    builder.projectedCols(projectList(ScanLevelProjection.WILDCARD, ScanLevelProjection.WILDCARD));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * The `columns` column is special; can't include both `columns` and
   * a named column in the same project.
   * <p>
   * TODO: This should only be true for text readers, make this an option.
   */

  @Test
  public void testErrorColumnsArrayAndColumn() {
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    builder.projectedCols(projectList("columns", "a"));
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
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    builder.projectedCols(projectList("a", "columns"));
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
    ScanLevelProjection.Builder builder = new ScanLevelProjection.Builder(fixture.options());

    builder.projectedCols(projectList("columns", "columns"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * Can't project partition columns if the file path is not rooted in the
   * base path.
   */

  @Test
  public void testErrorInvalidPath() {
    Path path = new Path("hdfs:///w/x/y/z.csv");
    try {
      new FileMetadata(path, "hdfs:///bad");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /**
   * Can't project partition columns if the file is above the
   * base path.
   */

  @Test
  public void testErrorShortPath() {
    Path path = new Path("hdfs:///w/z.csv");
    try {
      new FileMetadata(path, "hdfs:///w/x/y");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}
