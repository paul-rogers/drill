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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.metadata.FileLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.MetadataColumn;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestFileLevelProjection extends SubOperatorTest {

  private ProjectionFixture buildProj(String... queryCols) {
    return new ProjectionFixture()
        .withFileParser(fixture.options())
        .projectedCols(queryCols);
  }

  private ProjectionFixture buildProjAll() {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    return projFixture;
  }

  /**
   * Test the file projection planner with metadata.
   */

  @Test
  public void testWithMetadata() {
    ProjectionFixture projFixture = buildProj("filename", "a", "dir0");
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir("hdfs:///w");
    ScanLevelProjection scanProj = projFixture.build();

    FileLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");
    assertTrue(fileProj.hasMetadata());
    assertSame(scanProj, fileProj.scanProjection());
    assertEquals(3, fileProj.output().size());

    assertEquals(ColumnType.FILE_METADATA, fileProj.output().get(0).columnType());
    assertEquals("filename", fileProj.output().get(0).name());
    assertEquals("z.csv", ((ScanOutputColumn.MetadataColumn) fileProj.output().get(0)).value());
    assertEquals(ColumnType.TABLE, fileProj.output().get(1).columnType());
    assertEquals("a", fileProj.output().get(1).name());
    assertEquals(ColumnType.PARTITION, fileProj.output().get(2).columnType());
    assertEquals("dir0", fileProj.output().get(2).name());
    assertEquals("x", ((ScanOutputColumn.MetadataColumn) fileProj.output().get(2)).value());

    assertEquals(2, fileProj.metadataColumns().size());
    assertSame(fileProj.output().get(0), fileProj.metadataColumns().get(0));
    assertSame(fileProj.output().get(2), fileProj.metadataColumns().get(1));
  }

  /**
   * Test the file projection planner without metadata.
   * In some version of Drill, 1.11 or later, SELECT * will select only table
   * columns, not implicit columns or partitions.
   */

  @Test
  public void testWithoutMetadata() {
    ProjectionFixture projFixture = buildProj("a");
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir("hdfs:///w");
    projFixture.build();

    FileLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");
    assertFalse(fileProj.hasMetadata());
    assertEquals(1, fileProj.output().size());

    assertEquals(ColumnType.TABLE, fileProj.output().get(0).columnType());
    assertEquals("a", fileProj.output().get(0).name());

    // Not guaranteed to be null. Actually, if hasMetadata() is false,
    // don't even look at the metadata information.
    assertNull(fileProj.metadataColumns());
  }

  /**
   * For obscure reasons, Drill 1.10 and earlier would add all implicit
   * columns in a SELECT *, then would remove them again in a PROJECT
   * if not needed.
   */

  @Test
  public void testLegacyWildcard() {
    ProjectionFixture projFixture = buildProjAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(true);
    projFixture.metdataParser.setScanRootDir("hdfs:///w");
    projFixture.build();

    FileLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");

    assertTrue(fileProj.hasMetadata());
    assertEquals(7, fileProj.output().size());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable(SchemaPath.WILDCARD, MinorType.NULL)
        .buildSchema();
    expectedSchema = projFixture.expandMetadata(expectedSchema, 2);
    assertTrue(fileProj.outputSchema().isEquivalent(expectedSchema));

    assertEquals("/w/x/y/z.csv", ((MetadataColumn) fileProj.output().get(1)).value());
    assertEquals("/w/x/y", ((MetadataColumn) fileProj.output().get(2)).value());
    assertEquals("z.csv", ((MetadataColumn) fileProj.output().get(3)).value());
    assertEquals("csv", ((MetadataColumn) fileProj.output().get(4)).value());
    assertEquals("x", ((MetadataColumn) fileProj.output().get(5)).value());
    assertEquals("y", ((MetadataColumn) fileProj.output().get(6)).value());
  }

  /**
   * Test a query with explicit mention of file metadata columns.
   */

  @Test
  public void testFileMetadata() {
    ProjectionFixture projFixture = buildProj("a", "fqn",
        "filEPath", // Sic, to test case sensitivity
        "filename", "suffix");
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir("hdfs:///w");
    projFixture.build();
    FileLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");

    assertTrue(fileProj.hasMetadata());
    assertEquals(5, fileProj.output().size());

    assertEquals("fqn", fileProj.output().get(1).name());
    assertEquals("filEPath", fileProj.output().get(2).name());
    assertEquals("filename", fileProj.output().get(3).name());
    assertEquals("suffix", fileProj.output().get(4).name());

    assertEquals("/w/x/y/z.csv", ((MetadataColumn) fileProj.output().get(1)).value());
    assertEquals("/w/x/y", ((MetadataColumn) fileProj.output().get(2)).value());
    assertEquals("z.csv", ((MetadataColumn) fileProj.output().get(3)).value());
    assertEquals("csv", ((MetadataColumn) fileProj.output().get(4)).value());
  }

  /**
   * Test the obscure case that the partition column contains two digits:
   * dir11. Also tests the obscure case that the output only has partition
   * columns.
   */

  @Test
  public void testPartitionColumnTwoDigits() {
    ProjectionFixture projFixture = buildProj("dir11");
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir("hdfs:///x");
    projFixture.build();

    FileLevelProjection fileProj = projFixture.resolve("hdfs:///x/0/1/2/3/4/5/6/7/8/9/10/d11/z.csv");
    assertEquals("d11", ((MetadataColumn) fileProj.output().get(0)).value());
  }

  // TODO: Test more partition cols in select than are available dirs
  // TODO: Test customizing partition name, implicit col names.
}
