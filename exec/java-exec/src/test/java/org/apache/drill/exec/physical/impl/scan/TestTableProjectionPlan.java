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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.MetadataColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ResolvedFileInfo;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestTableProjectionPlan extends SubOperatorTest {

  /**
   * Resolve a selection list using SELECT *.
   */

  @Test
  public void testWildcard() {
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());
    querySelBuilder.useLegacyStarPlan(false);

    querySelBuilder.queryCols(TestQuerySelectionPlan.selectList("filename", "*", "dir0"));
    QuerySelectionPlan selectList = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(selectList, fileInfo);
    assertEquals(3, fileSel.output().size());

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    TableProjectionPlan tableSel = fileSel.resolve(tableSchema);
    assertFalse(tableSel.hasNullColumns());
    assertEquals(5, tableSel.output().size());

    assertEquals(ColumnType.FILE_METADATA, tableSel.output().get(0).columnType());
    assertEquals("filename", tableSel.output().get(0).name());
    assertEquals(ColumnType.PROJECTED, tableSel.output().get(1).columnType());
    assertEquals("a", tableSel.output().get(1).name());
    assertEquals(ColumnType.PROJECTED, tableSel.output().get(2).columnType());
    assertEquals("c", tableSel.output().get(2).name());
    assertEquals(ColumnType.PROJECTED, tableSel.output().get(3).columnType());
    assertEquals("d", tableSel.output().get(3).name());
    assertEquals(ColumnType.PARTITION, tableSel.output().get(4).columnType());
    assertEquals("dir0", tableSel.output().get(4).name());

    boolean selMap[] = tableSel.selectionMap();
    assertEquals(3, selMap.length);
    assertTrue(selMap[0]);
    assertTrue(selMap[1]);
    assertTrue(selMap[2]);

    int lToPMap[] = tableSel.logicalToPhysicalMap();
    assertEquals(0, lToPMap[0]);
    assertEquals(1, lToPMap[1]);
    assertEquals(2, lToPMap[2]);

    int projMap[] = tableSel.tableColumnProjectionMap();
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
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());
    querySelBuilder.useLegacyStarPlan(false);

    querySelBuilder.queryCols(TestQuerySelectionPlan.selectList("filename", "columns", "dir0"));
    QuerySelectionPlan selectList = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(selectList, fileInfo);

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .buildSchema();

    TableProjectionPlan tableSel = fileSel.resolve(tableSchema);
    assertFalse(tableSel.hasNullColumns());
    assertEquals(3, tableSel.output().size());

    assertEquals(ColumnType.FILE_METADATA, tableSel.output().get(0).columnType());
    assertEquals("filename", tableSel.output().get(0).name());

    // The columns array is now an actual table column.

    assertEquals(ColumnType.PROJECTED, tableSel.output().get(1).columnType());
    assertEquals("columns", tableSel.output().get(1).name());
    assertEquals(ColumnType.PARTITION, tableSel.output().get(2).columnType());
    assertEquals("dir0", tableSel.output().get(2).name());
    assertEquals(MinorType.VARCHAR, tableSel.output().get(1).type().getMinorType());
    assertEquals(DataMode.REPEATED, tableSel.output().get(1).type().getMode());

    boolean selMap[] = tableSel.selectionMap();
    assertTrue(selMap[0]);

    int lToPMap[] = tableSel.logicalToPhysicalMap();
    assertEquals(0, lToPMap[0]);

    int projMap[] = tableSel.tableColumnProjectionMap();
    assertEquals(1, projMap[0]);
  }

  /**
   * Test attempting to use the columns array with an early schema with
   * column types not compatible with a varchar array.
   */

  @Test
  public void testColumnsArrayIncompatible() {
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());
    querySelBuilder.useLegacyStarPlan(false);

    querySelBuilder.queryCols(TestQuerySelectionPlan.selectList("filename", "columns", "dir0"));
    QuerySelectionPlan selectList = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(selectList, fileInfo);

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .add("d", MinorType.FLOAT8)
        .buildSchema();

    try {
      fileSel.resolve(tableSchema);
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
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());

    // Simulate SELECT c, b, a ...

    querySelBuilder.queryCols(TestQuerySelectionPlan.selectList("c", "b", "a"));
    QuerySelectionPlan selectList = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(selectList, fileInfo);

    // Simulate a data source, with early schema, of (a, b, c)

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    TableProjectionPlan tableSel = fileSel.resolve(tableSchema);
    assertFalse(tableSel.hasNullColumns());

    assertEquals(3, tableSel.output().size());
    assertEquals(ColumnType.PROJECTED, tableSel.output().get(0).columnType());
    assertEquals(MinorType.VARCHAR, tableSel.output().get(0).type().getMinorType());
    assertEquals(DataMode.REQUIRED, tableSel.output().get(0).type().getMode());
    assertEquals("c", tableSel.output().get(0).name());
    assertEquals(ColumnType.PROJECTED, tableSel.output().get(1).columnType());
    assertEquals("b", tableSel.output().get(1).name());
    assertEquals(ColumnType.PROJECTED, tableSel.output().get(2).columnType());
    assertEquals("a", tableSel.output().get(2).name());

    boolean selMap[] = tableSel.selectionMap();
    assertEquals(3, selMap.length);
    assertTrue(selMap[0]);
    assertTrue(selMap[1]);
    assertTrue(selMap[2]);

    int ltoPMap[] = tableSel.logicalToPhysicalMap();
    assertEquals(3, ltoPMap.length);
    assertEquals(0, ltoPMap[0]);
    assertEquals(1, ltoPMap[1]);
    assertEquals(2, ltoPMap[2]);

    int projMap[] = tableSel.tableColumnProjectionMap();
    assertEquals(2, projMap[0]);
    assertEquals(1, projMap[1]);
    assertEquals(0, projMap[2]);
  }

  /**
   * Test SELECT list with columns missing from the table schema.
   */

  @Test
  public void testMissing() {
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());

    // Simulate SELECT c, b, a ...

    querySelBuilder.queryCols(TestQuerySelectionPlan.selectList("c", "v", "b", "w"));
    QuerySelectionPlan selectList = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(selectList, fileInfo);

    // Simulate a data source, with early schema, of (a, b, c)

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    TableProjectionPlan tableSel = fileSel.resolve(tableSchema);
    assertTrue(tableSel.hasNullColumns());

    assertEquals(4, tableSel.output().size());
    assertEquals(ColumnType.PROJECTED, tableSel.output().get(0).columnType());
    assertEquals("c", tableSel.output().get(0).name());
    assertEquals(ColumnType.NULL, tableSel.output().get(1).columnType());
    assertEquals("v", tableSel.output().get(1).name());
    assertEquals(ColumnType.PROJECTED, tableSel.output().get(2).columnType());
    assertEquals("b", tableSel.output().get(2).name());
    assertEquals(ColumnType.NULL, tableSel.output().get(3).columnType());
    assertEquals("w", tableSel.output().get(3).name());
    assertNull(tableSel.output().get(3).type());

    boolean selMap[] = tableSel.selectionMap();
    assertEquals(3, selMap.length);
    assertFalse(selMap[0]);
    assertTrue(selMap[1]);
    assertTrue(selMap[2]);

    int ltoPMap[] = tableSel.logicalToPhysicalMap();
    assertEquals(3, ltoPMap.length);
    assertEquals(-1, ltoPMap[0]);
    assertEquals(0, ltoPMap[1]);
    assertEquals(1, ltoPMap[2]);

    int projMap[] = tableSel.tableColumnProjectionMap();
    assertEquals(-1, projMap[0]);
    assertEquals(2, projMap[1]);
    assertEquals(0, projMap[2]);

    assertEquals(2, tableSel.nullColumns().size());
    assertEquals("v", tableSel.nullColumns().get(0).name());
    assertEquals("w", tableSel.nullColumns().get(1).name());

    int nullProjMap[] = tableSel.nullProjectionMap();
    assertEquals(1, nullProjMap[0]);
    assertEquals(3, nullProjMap[1]);
  }

  @Test
  public void testSubset() {
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());

    // Simulate SELECT c, b, a ...

    querySelBuilder.queryCols(TestQuerySelectionPlan.selectList("c", "a"));
    QuerySelectionPlan selectList = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(selectList, fileInfo);

    // Simulate a data source, with early schema, of (a, b, c)

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    TableProjectionPlan tableSel = fileSel.resolve(tableSchema);
    assertFalse(tableSel.hasNullColumns());

    assertEquals(2, tableSel.output().size());
    assertEquals("c", tableSel.output().get(0).name());
    assertEquals("a", tableSel.output().get(1).name());

    boolean selMap[] = tableSel.selectionMap();
    assertEquals(3, selMap.length);
    assertTrue(selMap[0]);
    assertFalse(selMap[1]);
    assertTrue(selMap[2]);

    int lToPMap[] = tableSel.logicalToPhysicalMap();
    assertEquals(3, lToPMap.length);
    assertEquals(0, lToPMap[0]);
    assertEquals(-1, lToPMap[1]);
    assertEquals(1, lToPMap[2]);

    int projMap[] = tableSel.tableColumnProjectionMap();
    assertEquals(1, projMap[0]);
    assertEquals(-1, projMap[1]);
    assertEquals(0, projMap[2]);
  }
}
