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
import static org.junit.Assert.fail;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadata;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.MetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ReaderLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.UnresolvedColumn;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFileLevelProjection extends SubOperatorTest {

  @Test
  public void testMetadataBuilder() {
    {
      // Degenerate case: no file or root

      FileMetadata md = new FileMetadata(null, null);
      assertFalse(md.isSet());
      assertNull(md.filePath());
      assertEquals(0, md.dirPathLength());
      assertNull(md.partition(0));
    }

    {
      // Degenerate case: no file path, but with as selection root
      // Should never occur in practice.

      Path root = new Path("hdfs://a/b");
      FileMetadata md = new FileMetadata(null, root);
      assertFalse(md.isSet());
      assertNull(md.filePath());
      assertEquals(0, md.dirPathLength());
      assertNull(md.partition(0));
    }

    {
      // Simple file, no selection root.
      // Should never really occur, but let's test it anyway.

      Path input = new Path("hdfs://foo.csv");
      FileMetadata md = new FileMetadata(input, null);
      assertTrue(md.isSet());
      assertSame(input, md.filePath());
      assertEquals(0, md.dirPathLength());
      assertNull(md.partition(0));
    }

    {
      // Normal file, no selection root.

      Path input = new Path("hdfs://a/b/c/foo.csv");
      FileMetadata md = new FileMetadata(input, null);
      assertTrue(md.isSet());
      assertSame(input, md.filePath());
      assertEquals(0, md.dirPathLength());
      assertNull(md.partition(0));
    }

    {
      // Normal file, resides in selection root.

      Path root = new Path("hdfs://a/b");
      Path input = new Path("hdfs://a/b/foo.csv");
      FileMetadata md = new FileMetadata(input, root);
      assertTrue(md.isSet());
      assertSame(input, md.filePath());
      assertEquals(0, md.dirPathLength());
      assertNull(md.partition(0));
    }

    {
      // Normal file, below selection root.

      Path root = new Path("hdfs://a/b");
      Path input = new Path("hdfs://a/b/c/foo.csv");
      FileMetadata md = new FileMetadata(input, root);
      assertTrue(md.isSet());
      assertSame(input, md.filePath());
      assertEquals(1, md.dirPathLength());
      assertEquals("c", md.partition(0));
      assertNull(md.partition(1));
    }

    {
      // Normal file, above selection root.
      // This is an error condition.

      Path root = new Path("hdfs://a/b");
      Path input = new Path("hdfs://a/foo.csv");
      try {
        new FileMetadata(input, root);
        fail();
      } catch (IllegalArgumentException e) {
        // Expected
      }
    }

    {
      // Normal file, disjoint with selection root.
      // This is an error condition.

      Path root = new Path("hdfs://a/b");
      Path input = new Path("hdfs://d/foo.csv");
      try {
        new FileMetadata(input, root);
        fail();
      } catch (IllegalArgumentException e) {
        // Expected
      }
    }
  }

  private ProjectionFixture buildProj(String... queryCols) {
    return new ProjectionFixture()
        .withFileManager(fixture.options())
        .projectedCols(queryCols);
  }

  private ProjectionFixture buildProjAll() {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileManager(fixture.options());
    projFixture.projectAll();
    return projFixture;
  }

  /**
   * Test the file projection planner with metadata.
   */

  @Test
  public void testWithMetadata() {
    ProjectionFixture projFixture = buildProj(ScanTestUtils.FILE_NAME_COL,
        "a", ScanTestUtils.partitionColName(0));
    projFixture.useLegacyWildcardExpansion(false);
    projFixture.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();

    ReaderLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");
    assertEquals(3, fileProj.output().size());

    assertTrue(fileProj.output().get(0) instanceof FileMetadataColumn);
    FileMetadataColumn col0 = (FileMetadataColumn) fileProj.output().get(0);
    assertEquals(FileMetadataColumn.ID, col0.nodeType());
    assertEquals(ScanTestUtils.FILE_NAME_COL, col0.name());
    assertEquals("z.csv", col0.value());
    assertEquals(MinorType.VARCHAR, col0.schema().getType().getMinorType());
    assertEquals(DataMode.REQUIRED, col0.schema().getType().getMode());

    ColumnProjection col1 = fileProj.output().get(1);
    assertEquals(UnresolvedColumn.UNRESOLVED, col1.nodeType());
    assertEquals("a", col1.name());

    assertTrue(fileProj.output().get(2) instanceof PartitionColumn);
    PartitionColumn col2 = (PartitionColumn) fileProj.output().get(2);
    assertEquals(PartitionColumn.ID, col2.nodeType());
    assertEquals(ScanTestUtils.partitionColName(0), col2.name());
    assertEquals("x", col2.value());
    assertEquals(MinorType.VARCHAR, col2.schema().getType().getMinorType());
    assertEquals(DataMode.OPTIONAL, col2.schema().getType().getMode());

//    // Verify that the file metadata columns were picked out
//
//    assertEquals(2, fileProj.metadataColumns().size());
//    assertSame(fileProj.output().get(0), fileProj.metadataColumns().get(0));
//    assertSame(fileProj.output().get(2), fileProj.metadataColumns().get(1));
  }

  /**
   * Test the file projection planner without metadata.
   * In some version of Drill, 1.11 or later, SELECT * will select only table
   * columns, not implicit columns or partitions.
   */

  @Test
  public void testWithoutMetadata() {
    ProjectionFixture projFixture = buildProj("a");
    projFixture.useLegacyWildcardExpansion(false);
    projFixture.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();

    ReaderLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");
//    assertFalse(fileProj.hasMetadata());
    assertEquals(1, fileProj.output().size());

    assertEquals(UnresolvedColumn.UNRESOLVED, fileProj.output().get(0).nodeType());
    assertEquals("a", fileProj.output().get(0).name());

//    // Not guaranteed to be null. Actually, if hasMetadata() is false,
//    // don't even look at the metadata information.
//    assertNull(fileProj.metadataColumns());
  }

  /**
   * For obscure reasons, Drill 1.10 and earlier would add all implicit
   * columns in a SELECT *, then would remove them again in a PROJECT
   * if not needed.
   */

  @Test
  public void testLegacyWildcard() {
    ProjectionFixture projFixture = buildProjAll();
    projFixture.useLegacyWildcardExpansion(true);
    projFixture.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();

    ReaderLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");

//    assertTrue(fileProj.hasMetadata());
    assertEquals(7, fileProj.output().size());

    @SuppressWarnings("unused")
    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable(SchemaPath.WILDCARD, MinorType.NULL)
        .buildSchema();
//    expectedSchema = projFixture.expandMetadata(expectedSchema, 2);
//    assertTrue(ScanTestUtils.schema(fileProj.output()).isEquivalent(expectedSchema));

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
    projFixture.useLegacyWildcardExpansion(false);
    projFixture.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ReaderLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");

//    assertTrue(fileProj.hasMetadata());
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
    projFixture.useLegacyWildcardExpansion(false);
    projFixture.setScanRootDir(new Path("hdfs:///x"));
    projFixture.build();

    ReaderLevelProjection fileProj = projFixture.resolve("hdfs:///x/0/1/2/3/4/5/6/7/8/9/10/d11/z.csv");
    assertEquals("d11", ((MetadataColumn) fileProj.output().get(0)).value());
  }

  // TODO: Test more partition cols in select than are available dirs
  // TODO: Test customizing partition name, implicit col names.
}
