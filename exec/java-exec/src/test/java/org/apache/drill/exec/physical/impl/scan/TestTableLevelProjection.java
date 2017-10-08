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
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanOutputColumn.ColumnType;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestTableLevelProjection extends SubOperatorTest {

  /**
   * Resolve a selection list using SELECT *.
   */

  @Test
  public void testWildcardNew() {
    ScanLevelProjection.ScanProjectionBuilder scanProjBuilder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(ScanTestUtils.projectList("filename", "*", "dir0"));
    ScanLevelProjection scanProj = scanProjBuilder.build();

    FileLevelProjection fileProj = scanProj.resolve(new Path("hdfs:///w/x/y/z.csv"));
    assertEquals(3, fileProj.output().size());
    assertTrue(fileProj.hasMetadata());

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    TupleMetadata expectedSchema = new SchemaBuilder()
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

    boolean selMap[] = tableProj.projectionMap();
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

    int mdProj[] = tableProj.metadataProjection();
    assertEquals(0, mdProj[0]);
    assertEquals(4, mdProj[1]);
  }

  @Test
  public void testWildcardLegacy() {
    ScanLevelProjection.ScanProjectionBuilder scanProjBuilder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(true);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(ScanTestUtils.projectList("*"));
    ScanLevelProjection scanProj = scanProjBuilder.build();

    FileLevelProjection fileProj = scanProj.resolve(new Path("hdfs:///w/x/y/z.csv"));
    assertEquals(7, fileProj.output().size());
    assertTrue(fileProj.hasMetadata());

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertEquals(9, tableProj.output().size());
    assertFalse(tableProj.hasNullColumns());

    TupleMetadata expectedSchema = TestFileLevelProjection.expandMetadata(scanProj, tableSchema, 2);

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));

    boolean selMap[] = tableProj.projectionMap();
    assertEquals(3, selMap.length);
    assertTrue(selMap[0]);
    assertTrue(selMap[1]);
    assertTrue(selMap[2]);

    int lToPMap[] = tableProj.logicalToPhysicalMap();
    assertEquals(0, lToPMap[0]);
    assertEquals(1, lToPMap[1]);
    assertEquals(2, lToPMap[2]);

    int projMap[] = tableProj.tableColumnProjectionMap();
    assertEquals(0, projMap[0]);
    assertEquals(1, projMap[1]);
    assertEquals(2, projMap[2]);

    int mdProj[] = tableProj.metadataProjection();
    int mdCount = fileProj.metadataColumns().size();
    for (int i = 0; i < mdCount; i++) {
      assertEquals(i + 3, mdProj[i]);
    }
  }

  /**
   * Test columns array. The table must be able to support it by having all
   * Varchar columns.
   */

  @Test
  public void testColumnsArray() {
    ScanLevelProjection.ScanProjectionBuilder scanProjBuilder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.projectedCols(ScanTestUtils.projectList("filename", "columns", "dir0"));
    scanProjBuilder.setScanRootDir("hdfs:///w");
    ScanLevelProjection scanProj = scanProjBuilder.build();

    FileLevelProjection fileProj = scanProj.resolve(new Path("hdfs:///w/x/y/z.csv"));

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("filename", MinorType.VARCHAR)
        .addArray("columns", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));

    assertEquals(ColumnType.FILE_METADATA, tableProj.output().get(0).columnType());

    // The columns array is now an actual table column.

    assertEquals(ColumnType.PROJECTED, tableProj.output().get(1).columnType());
    assertEquals(ColumnType.PARTITION, tableProj.output().get(2).columnType());

    boolean selMap[] = tableProj.projectionMap();
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
    ScanLevelProjection.ScanProjectionBuilder scanProjBuilder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.projectedCols(ScanTestUtils.projectList("filename", "columns", "dir0"));
    scanProjBuilder.setScanRootDir("hdfs:///w");
    ScanLevelProjection scanProj = scanProjBuilder.build();

    FileLevelProjection fileProj = scanProj.resolve(new Path("hdfs:///w/x/y/z.csv"));

    TupleMetadata tableSchema = new SchemaBuilder()
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
    ScanLevelProjection.ScanProjectionBuilder scanProjBuilder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());
    scanProjBuilder.setScanRootDir("hdfs:///w");

    // Simulate SELECT c, b, a ...

    scanProjBuilder.projectedCols(ScanTestUtils.projectList("c", "b", "a"));
    ScanLevelProjection scanProj = scanProjBuilder.build();

    FileLevelProjection fileProj = scanProj.resolve(new Path("hdfs:///w/x/y/z.csv"));

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));
    assertEquals(ColumnType.PROJECTED, tableProj.output().get(0).columnType());
    assertEquals(ColumnType.PROJECTED, tableProj.output().get(1).columnType());
    assertEquals(ColumnType.PROJECTED, tableProj.output().get(2).columnType());

    boolean selMap[] = tableProj.projectionMap();
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
    ScanLevelProjection.ScanProjectionBuilder scanProjBuilder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());
    scanProjBuilder.setScanRootDir("hdfs:///w");

    // Simulate SELECT c, b, a ...

    scanProjBuilder.projectedCols(ScanTestUtils.projectList("c", "v", "b", "w"));
    ScanLevelProjection scanProj = scanProjBuilder.build();

    FileLevelProjection fileProj = scanProj.resolve(new Path("hdfs:///w/x/y/z.csv"));

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertTrue(tableProj.hasNullColumns());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .addNullable("v", MinorType.NULL)
        .add("b", MinorType.VARCHAR)
        .addNullable("w", MinorType.NULL)
        .buildSchema();

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));

    boolean selMap[] = tableProj.projectionMap();
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
    ScanLevelProjection.ScanProjectionBuilder scanProjBuilder = new ScanLevelProjection.ScanProjectionBuilder(fixture.options());
    scanProjBuilder.setScanRootDir("hdfs:///w");

    // Simulate SELECT c, a ...

    scanProjBuilder.projectedCols(ScanTestUtils.projectList("c", "a"));
    ScanLevelProjection scanProj = scanProjBuilder.build();

    FileLevelProjection fileProj = scanProj.resolve(new Path("hdfs:///w/x/y/z.csv"));

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolve(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    assertTrue(tableProj.outputSchema().isEquivalent(expectedSchema));

    boolean selMap[] = tableProj.projectionMap();
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
