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
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.UnresolvedColumn;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

public class TestFileMetadataColumnParser extends SubOperatorTest {

  private ProjectionFixture buildProj(String... queryCols) {
    ProjectionFixture proj = new ProjectionFixture()
        .withFileManager(fixture.options())
        .useLegacyWildcardExpansion(true)
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
    assertFalse(scanProj.projectAll());

    FileMetadataManager metadataProj = projFixture.metadataProj;
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
        ScanTestUtils.FULLY_QUALIFIED_NAME_COL,
        "filEPath", // Sic, to test case sensitivity
        ScanTestUtils.FILE_NAME_COL,
        ScanTestUtils.SUFFIX_COL);

    ScanLevelProjection scanProj = projFixture.scanProj;
    assertFalse(scanProj.projectAll());
    assertEquals(5, scanProj.requestedCols().size());

    assertEquals(5, scanProj.outputCols().size());

    assertEquals("a", scanProj.outputCols().get(0).name());
    assertEquals(ScanTestUtils.FULLY_QUALIFIED_NAME_COL, scanProj.outputCols().get(1).name());
    assertEquals("filEPath", scanProj.outputCols().get(2).name());
    assertEquals(ScanTestUtils.FILE_NAME_COL, scanProj.outputCols().get(3).name());
    assertEquals(ScanTestUtils.SUFFIX_COL, scanProj.outputCols().get(4).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.outputCols().get(0).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.outputCols().get(1).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.outputCols().get(2).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.outputCols().get(3).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.outputCols().get(4).nodeType());

    FileMetadataManager metadataProj = projFixture.metadataProj;
    assertNotNull(metadataProj);
    assertTrue(metadataProj.hasMetadata());
    assertTrue(metadataProj.useLegacyWildcardPartition());
  }

  /**
   * Verify that partition columns, in any case, work.
   */

  @Test
  public void testPartitionColumnSelection() {

    String dir0 = ScanTestUtils.partitionColName(0);
    // Sic: case insensitivity, but name in project list
    // is preferred over "natural" name.
    String dir1 = "DIR1";
    String dir2 = ScanTestUtils.partitionColName(2);
    ProjectionFixture projFixture = buildProj(
        dir2, dir1, dir0, "a");

    ScanLevelProjection scanProj = projFixture.scanProj;

    assertEquals(4, scanProj.outputCols().size());
    assertEquals(dir2, scanProj.outputCols().get(0).name());
    assertEquals(dir1, scanProj.outputCols().get(1).name());
    assertEquals(dir0, scanProj.outputCols().get(2).name());
    assertEquals("a", scanProj.outputCols().get(3).name());

    // Verify column type

    assertEquals(PartitionColumn.ID, scanProj.outputCols().get(0).nodeType());

    FileMetadataManager metadataProj = projFixture.metadataProj;
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
      buildProj(ScanTestUtils.FILE_NAME_COL,
          SchemaPath.WILDCARD);
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
      buildProj(SchemaPath.WILDCARD,
          ScanTestUtils.partitionColName(8));
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
