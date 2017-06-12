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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.FileMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestTableProjectionDefn extends SubOperatorTest {

  /**
   * Resolve a selection list using SELECT *.
   */

  @Test
  public void testWildcard() {
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);

    scanProjBuilder.queryCols(TestScanProjectionDefn.projectList("filename", "*", "dir0"));
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileLevelProjection fileProj = new FileLevelProjection(scanProj, fileInfo);
    assertEquals(3, fileProj.output().size());

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    MaterializedSchema expectedSchema = new SchemaBuilder()
        .add("filename", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));

    assertEquals(ColumnType.FILE_METADATA, tableProj.output().get(0).columnType());
    assertEquals(ColumnType.PROJECTED, tableProj.output().get(1).columnType());
    assertEquals(ColumnType.PROJECTED, tableProj.output().get(2).columnType());
    assertEquals(ColumnType.PROJECTED, tableProj.output().get(3).columnType());
    assertEquals(ColumnType.PARTITION, tableProj.output().get(4).columnType());

    boolean selMap[] = tableProj.selectionMap();
    assertEquals(3, selMap.length);
    assertTrue(selMap[0]);
    assertTrue(selMap[1]);
    assertTrue(selMap[2]);

    int lToPMap[] = tableProj.logicalToPhysicalMap();
    assertEquals(0, lToPMap[0]);
    assertEquals(1, lToPMap[1]);
    assertEquals(2, lToPMap[2]);

    int projMap[] = tableProj.tableColumnProjectionMap();
    assertEquals(1, projMap[0]);
    assertEquals(2, projMap[1]);
    assertEquals(3, projMap[2]);
  }

  /**
   * Test columns array. The table must be able to support it by having all
   * Varchar columns.
   */

  @Test
  public void testColumnsArray() {
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);

    scanProjBuilder.queryCols(TestScanProjectionDefn.projectList("filename", "columns", "dir0"));
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileLevelProjection fileProj = new FileLevelProjection(scanProj, fileInfo);

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    MaterializedSchema expectedSchema = new SchemaBuilder()
        .add("filename", MinorType.VARCHAR)
        .addArray("columns", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));

    assertEquals(ColumnType.FILE_METADATA, tableProj.output().get(0).columnType());

    // The columns array is now an actual table column.

    assertEquals(ColumnType.PROJECTED, tableProj.output().get(1).columnType());
    assertEquals(ColumnType.PARTITION, tableProj.output().get(2).columnType());

    boolean selMap[] = tableProj.selectionMap();
    assertTrue(selMap[0]);

    int lToPMap[] = tableProj.logicalToPhysicalMap();
    assertEquals(0, lToPMap[0]);

    int projMap[] = tableProj.tableColumnProjectionMap();
    assertEquals(1, projMap[0]);
  }

  /**
   * Test attempting to use the columns array with an early schema with
   * column types not compatible with a varchar array.
   */

  @Test
  public void testColumnsArrayIncompatible() {
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);

    scanProjBuilder.queryCols(TestScanProjectionDefn.projectList("filename", "columns", "dir0"));
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileLevelProjection fileProj = new FileLevelProjection(scanProj, fileInfo);

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .add("d", MinorType.FLOAT8)
        .buildSchema();

    try {
      fileProj.resolve(tableSchema);
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  /**
   * Test SELECT list with columns defined in a order and with
   * name case different than the early-schema table.
   */

  @Test
  public void testFullList() {
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());

    // Simulate SELECT c, b, a ...

    scanProjBuilder.queryCols(TestScanProjectionDefn.projectList("c", "b", "a"));
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileLevelProjection fileProj = new FileLevelProjection(scanProj, fileInfo);

    // Simulate a data source, with early schema, of (a, b, c)

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    MaterializedSchema expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));
    assertEquals(ColumnType.PROJECTED, tableProj.output().get(0).columnType());
    assertEquals(ColumnType.PROJECTED, tableProj.output().get(1).columnType());
    assertEquals(ColumnType.PROJECTED, tableProj.output().get(2).columnType());

    boolean selMap[] = tableProj.selectionMap();
    assertEquals(3, selMap.length);
    assertTrue(selMap[0]);
    assertTrue(selMap[1]);
    assertTrue(selMap[2]);

    int ltoPMap[] = tableProj.logicalToPhysicalMap();
    assertEquals(3, ltoPMap.length);
    assertEquals(0, ltoPMap[0]);
    assertEquals(1, ltoPMap[1]);
    assertEquals(2, ltoPMap[2]);

    int projMap[] = tableProj.tableColumnProjectionMap();
    assertEquals(2, projMap[0]);
    assertEquals(1, projMap[1]);
    assertEquals(0, projMap[2]);
  }

  /**
   * Test SELECT list with columns missing from the table schema.
   */

  @Test
  public void testMissing() {
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());

    // Simulate SELECT c, b, a ...

    scanProjBuilder.queryCols(TestScanProjectionDefn.projectList("c", "v", "b", "w"));
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileLevelProjection fileProj = new FileLevelProjection(scanProj, fileInfo);

    // Simulate a data source, with early schema, of (a, b, c)

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertTrue(tableProj.hasNullColumns());

    MaterializedSchema expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .addNullable("v", MinorType.NULL)
        .add("b", MinorType.VARCHAR)
        .addNullable("w", MinorType.NULL)
        .buildSchema();

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));

    boolean selMap[] = tableProj.selectionMap();
    assertEquals(3, selMap.length);
    assertFalse(selMap[0]);
    assertTrue(selMap[1]);
    assertTrue(selMap[2]);

    int ltoPMap[] = tableProj.logicalToPhysicalMap();
    assertEquals(3, ltoPMap.length);
    assertEquals(-1, ltoPMap[0]);
    assertEquals(0, ltoPMap[1]);
    assertEquals(1, ltoPMap[2]);

    int projMap[] = tableProj.tableColumnProjectionMap();
    assertEquals(-1, projMap[0]);
    assertEquals(2, projMap[1]);
    assertEquals(0, projMap[2]);

    assertEquals(2, tableProj.nullColumns().size());
    assertEquals("v", tableProj.nullColumns().get(0).name());
    assertEquals("w", tableProj.nullColumns().get(1).name());

    int nullProjMap[] = tableProj.nullProjectionMap();
    assertEquals(1, nullProjMap[0]);
    assertEquals(3, nullProjMap[1]);
  }

  @Test
  public void testSubset() {
    ScanProjectionDefn.Builder scanProjBuilder = new ScanProjectionDefn.Builder(fixture.options());

    // Simulate SELECT c, a ...

    scanProjBuilder.queryCols(TestScanProjectionDefn.projectList("c", "a"));
    ScanProjectionDefn scanProj = scanProjBuilder.build();

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileLevelProjection fileProj = new FileLevelProjection(scanProj, fileInfo);

    // Simulate a data source, with early schema, of (a, b, c)

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    MaterializedSchema expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));

    boolean selMap[] = tableProj.selectionMap();
    assertEquals(3, selMap.length);
    assertTrue(selMap[0]);
    assertFalse(selMap[1]);
    assertTrue(selMap[2]);

    int lToPMap[] = tableProj.logicalToPhysicalMap();
    assertEquals(3, lToPMap.length);
    assertEquals(0, lToPMap[0]);
    assertEquals(-1, lToPMap[1]);
    assertEquals(1, lToPMap[2]);

    int projMap[] = tableProj.tableColumnProjectionMap();
    assertEquals(1, projMap[0]);
    assertEquals(-1, projMap[1]);
    assertEquals(0, projMap[2]);
  }

  // TODO: Custom columns array type
}
