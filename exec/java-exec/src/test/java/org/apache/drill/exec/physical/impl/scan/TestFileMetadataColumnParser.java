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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.UnresolvedColumn;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestFileMetadataColumnParser extends SubOperatorTest {

  @Test
  public void testBasics() {

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    // Simulate SELECT a, b, c ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList("a", "b", "c"),
        Lists.newArrayList(metadataManager.projectionParser()));

    // Verify

    assertFalse(scanProj.projectAll());
    assertFalse(metadataManager.hasMetadata());
    assertTrue(metadataManager.useLegacyWildcardPartition());
  }

  /**
   * Test including file metadata (AKA "implicit columns") in the project
   * list.
   */

  @Test
  public void testFileMetadataColumnSelection() {

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    // Simulate SELECT a, fqn, filEPath, filename, suffix ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList("a",
            ScanTestUtils.FULLY_QUALIFIED_NAME_COL,
            "filEPath", // Sic, to test case sensitivity
            ScanTestUtils.FILE_NAME_COL,
            ScanTestUtils.SUFFIX_COL),
        Lists.newArrayList(metadataManager.projectionParser()));

    assertFalse(scanProj.projectAll());
    assertEquals(5, scanProj.requestedCols().size());

    assertEquals(5, scanProj.columns().size());

    assertEquals("a", scanProj.columns().get(0).name());
    assertEquals(ScanTestUtils.FULLY_QUALIFIED_NAME_COL, scanProj.columns().get(1).name());
    assertEquals("filEPath", scanProj.columns().get(2).name());
    assertEquals(ScanTestUtils.FILE_NAME_COL, scanProj.columns().get(3).name());
    assertEquals(ScanTestUtils.SUFFIX_COL, scanProj.columns().get(4).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(0).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(1).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(2).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(3).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(4).nodeType());

    assertTrue(metadataManager.hasMetadata());
  }

  /**
   * Verify that partition columns, in any case, work.
   */

  @Test
  public void testPartitionColumnSelection() {

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    String dir0 = ScanTestUtils.partitionColName(0);
    // Sic: case insensitivity, but name in project list
    // is preferred over "natural" name.
    String dir1 = "DIR1";
    String dir2 = ScanTestUtils.partitionColName(2);
    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectList(dir2, dir1, dir0, "a"),
        Lists.newArrayList(metadataManager.projectionParser()));

    assertEquals(4, scanProj.columns().size());
    assertEquals(dir2, scanProj.columns().get(0).name());
    assertEquals(dir1, scanProj.columns().get(1).name());
    assertEquals(dir0, scanProj.columns().get(2).name());
    assertEquals("a", scanProj.columns().get(3).name());

    // Verify column type

    assertEquals(PartitionColumn.ID, scanProj.columns().get(0).nodeType());
  }

  /**
   * Can't explicitly list file metadata columns with a wildcard in
   * "legacy" mode: that is, when the wildcard already includes partition
   * and file metadata columns.
   */

  @Test
  public void testErrorWildcardLegacyAndFileMetaata() {

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    try {
      new ScanLevelProjection(
          ScanTestUtils.projectList(ScanTestUtils.FILE_NAME_COL,
              SchemaPath.WILDCARD),
          Lists.newArrayList(metadataManager.projectionParser()));
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

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    try {
      new ScanLevelProjection(
          ScanTestUtils.projectList(SchemaPath.WILDCARD,
              ScanTestUtils.partitionColName(8)),
          Lists.newArrayList(metadataManager.projectionParser()));
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
