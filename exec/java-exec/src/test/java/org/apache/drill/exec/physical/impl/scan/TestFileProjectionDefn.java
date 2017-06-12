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

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.MetadataColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjectionDefn.FileMetadataColumnDefn;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.FileMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFileProjectionDefn extends SubOperatorTest {

  /**
   * Test the file projection planner with metadata.
   */

  @Test
  public void testWithMetadata() {
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);

    scanProjBuilder.queryCols(TestScanProjectionDefn.projectList("filename", "a", "dir0"));
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileProjectionDefn fileProj = new FileProjectionDefn(scanProj, fileInfo);
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
    assertEquals(0, fileProj.metadataProjection()[0]);
    assertEquals(2, fileProj.metadataProjection()[1]);
  }

  /**
   * Test the file projection planner without metadata.
   * In some version of Drill, 1.11 or later, SELECT * will select only table
   * columns, not implicit columns or partitions.
   */

  @Test
  public void testWithoutMetadata() {
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);

    scanProjBuilder.queryCols(TestScanProjectionDefn.projectList("a"));
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileProjectionDefn fileProj = new FileProjectionDefn(scanProj, fileInfo);
    assertFalse(fileProj.hasMetadata());
    assertEquals(1, fileProj.output().size());

    assertEquals(ColumnType.TABLE, fileProj.output().get(0).columnType());
    assertEquals("a", fileProj.output().get(0).name());

    // Not guaranteed to be null. Actually, if hasMetadata() is false,
    // don't even look at the metadata information.
    assertNull(fileProj.metadataColumns());
  }

  /**
   * Mimic legacy wildcard expansion of metadata columns. Is not a full
   * emulation because this version only works if the wildcard were at the end
   * of the list (or alone.)
   * @param scanProj scan projection definition (provides the partition column names)
   * @param base the table part of the expansion
   * @param dirCount number of partition directories
   * @return schema with the metadata columns appended to the table columns
   */

  public static MaterializedSchema expandMetadata(ScanProjectionDefn scanProj, MaterializedSchema base, int dirCount) {
    MaterializedSchema metadataSchema = new MaterializedSchema();
    for (FileMetadataColumnDefn fileColDefn : scanProj.fileMetadataColDefns()) {
      metadataSchema.add(MaterializedField.create(fileColDefn.colName(), fileColDefn.dataType()));
    }
    for (int i = 0; i < dirCount; i++) {
      metadataSchema.add(MaterializedField.create(scanProj.partitionName(i),
          ScanProjectionDefn.partitionColType()));
    }
    return base.merge(metadataSchema);
  }

  /**
   * For obscure reasons, Drill 1.10 and earlier would add all implicit
   * columns in a SELECT *, then would remove them again in a PROJECT
   * if not needed.
   */

  @Test
  public void testLegacyWildcard() {
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(true);
    scanProjBuilder.projectAll();
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileProjectionDefn fileProj = new FileProjectionDefn(scanProj, fileInfo);

    assertTrue(fileProj.hasMetadata());
    assertEquals(7, fileProj.output().size());

    MaterializedSchema expectedSchema = new SchemaBuilder()
        .addNullable(ScanProjectionDefn.WILDCARD, MinorType.NULL)
        .buildSchema();
    expectedSchema = expandMetadata(scanProj, expectedSchema, 2);
    assertTrue(fileProj.outputSchema().isEquivalent(expectedSchema));

    assertEquals("hdfs:/w/x/y/z.csv", ((MetadataColumn) fileProj.output().get(1)).value());
    assertEquals("hdfs:/w/x/y", ((MetadataColumn) fileProj.output().get(2)).value());
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
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(true);
    scanProjBuilder.queryCols(TestScanProjectionDefn.projectList("a", "fqn",
        "filEPath", // Sic, to test case sensitivity
        "filename", "suffix"));
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileProjectionDefn fileProj = new FileProjectionDefn(scanProj, fileInfo);

    assertTrue(fileProj.hasMetadata());
    assertEquals(5, fileProj.output().size());

    assertEquals("fqn", fileProj.output().get(1).name());
    assertEquals("filEPath", fileProj.output().get(2).name());
    assertEquals("filename", fileProj.output().get(3).name());
    assertEquals("suffix", fileProj.output().get(4).name());

    assertEquals("hdfs:/w/x/y/z.csv", ((MetadataColumn) fileProj.output().get(1)).value());
    assertEquals("hdfs:/w/x/y", ((MetadataColumn) fileProj.output().get(2)).value());
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
    ScanProjectionDefn.Builder builder = new ScanProjectionDefn.Builder(fixture.options());

    builder.queryCols(TestScanProjectionDefn.projectList("dir11"));

    ScanProjectionDefn scanProj = builder.build();
    assertEquals("dir11", scanProj.outputCols().get(0).name());

    Path path = new Path("hdfs:///x/0/1/2/3/4/5/6/7/8/9/10/d11/z.csv");
    FileMetadata fileInfo = new FileMetadata(path, "hdfs:///x");
    FileProjectionDefn fileProj = new FileProjectionDefn(scanProj, fileInfo);

     assertEquals("d11", ((MetadataColumn) fileProj.output().get(0)).value());
  }

  // TODO: Test more partition cols in select than are available dirs
  // TODO: Test customizing partition name, implicit col names.
}
