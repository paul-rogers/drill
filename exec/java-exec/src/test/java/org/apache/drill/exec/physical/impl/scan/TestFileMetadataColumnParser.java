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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.project.FileMetadataColumnsParser.FileMetadata;
import org.apache.drill.exec.physical.impl.scan.project.FileMetadataColumnsParser.FileMetadataProjection;
import org.apache.drill.exec.physical.impl.scan.project.ColumnsArrayParser;
import org.apache.drill.exec.physical.impl.scan.project.FileMetadataColumnsParser;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ProjectionType;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFileMetadataColumnParser extends SubOperatorTest {

  private ProjectionFixture buildProj(String... queryCols) {
    ProjectionFixture proj = new ProjectionFixture()
        .withFileParser(fixture.options())
        .projectedCols(queryCols);
    proj.build();
    return proj;
  }

  @Test
  public void testBasics() {

    // Simulate SELECT a, b, c ...

    ProjectionFixture projFixture = buildProj("a", "b", "c");

    // Build the projection plan and verify

    ScanLevelProjection scanProj = projFixture.scanProj;
    assertFalse(scanProj.isProjectAll());
    assertEquals(ProjectionType.LIST, scanProj.projectType());

    FileMetadataProjection metadataProj = projFixture.metadataProj;
    assertNotNull(metadataProj);
    assertFalse(metadataProj.hasMetadata());
    assertTrue(metadataProj.useLegacyWildcardPartition());
  }

  /**
   * Test including file metadata (AKA "implicit columns") in the project
   * list.
   */

  @Test
  public void testFileMetadataColumnSelection() {

    // Simulate SELECT a, b, c ...

    ProjectionFixture projFixture = buildProj(
        "a",
        FileMetadataColumnsParser.FULLY_QUALIFIED_NAME_COL,
        "filEPath", // Sic, to test case sensitivity
        FileMetadataColumnsParser.FILE_NAME_COL,
        FileMetadataColumnsParser.SUFFIX_COL);

    ScanLevelProjection scanProj = projFixture.scanProj;
    assertFalse(scanProj.isProjectAll());
    assertEquals(5, scanProj.requestedCols().size());

    assertEquals(5, scanProj.outputCols().size());

    assertEquals("a", scanProj.outputCols().get(0).name());
    assertEquals(FileMetadataColumnsParser.FULLY_QUALIFIED_NAME_COL, scanProj.outputCols().get(1).name());
    assertEquals("filEPath", scanProj.outputCols().get(2).name());
    assertEquals(FileMetadataColumnsParser.FILE_NAME_COL, scanProj.outputCols().get(3).name());
    assertEquals(FileMetadataColumnsParser.SUFFIX_COL, scanProj.outputCols().get(4).name());

    assertEquals(MinorType.VARCHAR, ((ScanOutputColumn.TypedColumn) (scanProj.outputCols().get(1))).type().getMinorType());
    assertEquals(DataMode.REQUIRED, ((ScanOutputColumn.TypedColumn) (scanProj.outputCols().get(1))).type().getMode());

    // Verify bindings

    assertSame(scanProj.outputCols().get(1), scanProj.requestedCols().get(1).resolution());
    assertSame(scanProj.outputCols().get(1).source(), scanProj.requestedCols().get(1));

    // Verify column type

    assertEquals(ColumnType.TABLE, scanProj.outputCols().get(0).columnType());
    assertEquals(ColumnType.FILE_METADATA, scanProj.outputCols().get(1).columnType());

    FileMetadataProjection metadataProj = projFixture.metadataProj;
    assertNotNull(metadataProj);
    assertTrue(metadataProj.hasMetadata());
    assertTrue(metadataProj.useLegacyWildcardPartition());
  }

  /**
   * Verify that partition columns, in any case, work.
   */

  @Test
  public void testPartitionColumnSelection() {

    String dir0 = FileMetadataColumnsParser.partitionColName(0);
    String dir1 = "DIR1"; // Sic: case insensitivity
    String dir2 = FileMetadataColumnsParser.partitionColName(2);
    ProjectionFixture projFixture = buildProj(
        dir2, dir1, dir0, "a");

    ScanLevelProjection scanProj = projFixture.scanProj;

    assertEquals(4, scanProj.outputCols().size());
    assertEquals(dir2, scanProj.outputCols().get(0).name());
    assertEquals(dir1, scanProj.outputCols().get(1).name());
    assertEquals(dir0, scanProj.outputCols().get(2).name());
    assertEquals("a", scanProj.outputCols().get(3).name());

    // Verify bindings

    assertSame(scanProj.outputCols().get(1), scanProj.requestedCols().get(1).resolution());
    assertSame(scanProj.outputCols().get(1).source(), scanProj.requestedCols().get(1));

    // Verify column type

    assertEquals(ColumnType.PARTITION, scanProj.outputCols().get(0).columnType());

    // Verify data type

    assertEquals(MinorType.VARCHAR, ((ScanOutputColumn.TypedColumn) (scanProj.outputCols().get(0))).type().getMinorType());
    assertEquals(DataMode.OPTIONAL, ((ScanOutputColumn.TypedColumn) (scanProj.outputCols().get(0))).type().getMode());

    FileMetadataProjection metadataProj = projFixture.metadataProj;
    assertNotNull(metadataProj);
    assertTrue(metadataProj.hasMetadata());
    assertTrue(metadataProj.useLegacyWildcardPartition());
  }

  /**
   * Can't explicitly list file metadata columns with a wildcard in
   * "legacy" mode: that is, when the wildcard already includes partition
   * and file metadata columns.
   */

  @Test
  public void testErrorWildcardLegacyAndFileMetaata() {
    try {
      buildProj(FileMetadataColumnsParser.FILE_NAME_COL,
                ScanLevelProjection.WILDCARD);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Can't include both a wildcard and a partition column.
   */

  @Test
  public void testErrorWildcardLegacyAndPartition() {
    try {
      buildProj(ScanLevelProjection.WILDCARD,
          FileMetadataColumnsParser.partitionColName(8));
      fail();
    } catch (IllegalArgumentException e) {
      // expected
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

  /**
   * The `columns` column is special: can't be used with other column names.
   * Make sure that the rule <i>does not</i> apply to implicit columns.
   */

  @Test
  public void testMetadataColumnsWithColumnsArray() {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options())
        .withColumnsArrayParser();
    projFixture.scanBuilder.projectedCols(ScanTestUtils.projectList(
        FileMetadataColumnsParser.FILE_NAME_COL,
        ColumnsArrayParser.COLUMNS_COL,
        FileMetadataColumnsParser.SUFFIX_COL));
    projFixture.build();

    ScanLevelProjection scanProj = projFixture.scanProj;

    assertFalse(scanProj.isProjectAll());
    assertEquals(ProjectionType.COLUMNS_ARRAY, scanProj.projectType());

    assertEquals(3, scanProj.outputCols().size());

    assertEquals(FileMetadataColumnsParser.FILE_NAME_COL, scanProj.outputCols().get(0).name());
    assertEquals(ColumnsArrayParser.COLUMNS_COL, scanProj.outputCols().get(1).name());
    assertEquals(FileMetadataColumnsParser.SUFFIX_COL, scanProj.outputCols().get(2).name());

    // Verify column type

    assertEquals(ColumnType.FILE_METADATA, scanProj.outputCols().get(0).columnType());
    assertEquals(ColumnType.COLUMNS_ARRAY, scanProj.outputCols().get(1).columnType());
    assertEquals(ColumnType.FILE_METADATA, scanProj.outputCols().get(2).columnType());
  }
}
