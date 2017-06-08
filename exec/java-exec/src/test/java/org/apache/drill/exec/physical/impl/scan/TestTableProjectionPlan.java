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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.TableSelectionPlan.TableSelectionBuilder;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestTableProjectionPlan extends SubOperatorTest {

  @Test
  public void testEarlySchemaStar() {
    SelectionListPlan.Builder sBuilder = new SelectionListPlan.Builder(fixture.options());
    sBuilder.useLegacyStarPlan(false);

    sBuilder.queryCols(TestSelectionListPlan.selectList("filename", "*", "dir0"));
    SelectionListPlan selectList = sBuilder.build();

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    TableSelectionBuilder tBuilder = new TableSelectionBuilder(selectList);
    tBuilder.setTableSchema(tableSchema);
    tBuilder.setFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    TableSelectionPlan fullSelect = tBuilder.build();

    assertTrue(fullSelect.isResolved());
    assertEquals(5, fullSelect.output().size());

    assertEquals(ColumnType.FILE_METADATA, fullSelect.output().get(0).columnType());
    assertEquals("filename", fullSelect.output().get(0).name());
    assertEquals(ColumnType.PROJECTED, fullSelect.output().get(1).columnType());
    assertEquals("a", fullSelect.output().get(1).name());
    assertEquals(ColumnType.PROJECTED, fullSelect.output().get(2).columnType());
    assertEquals("c", fullSelect.output().get(2).name());
    assertEquals(ColumnType.PROJECTED, fullSelect.output().get(3).columnType());
    assertEquals("d", fullSelect.output().get(3).name());
    assertEquals(ColumnType.PARTITION, fullSelect.output().get(4).columnType());
    assertEquals("dir0", fullSelect.output().get(4).name());
  }

  /**
   * Test columns array. The table must be able to support it by having all
   * Varchar columns.
   */

  @Test
  public void testEarlyColumnsArray() {
    SelectionListPlan.Builder sBuilder = new SelectionListPlan.Builder(fixture.options());
    sBuilder.useLegacyStarPlan(false);

    sBuilder.queryCols(TestSelectionListPlan.selectList("filename", "columns", "dir0"));
    SelectionListPlan selectList = sBuilder.build();

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .buildSchema();

    TableSelectionBuilder tBuilder = new TableSelectionBuilder(selectList);
    tBuilder.setTableSchema(tableSchema);
    tBuilder.setFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    TableSelectionPlan fullSelect = tBuilder.build();

    assertTrue(fullSelect.isResolved());
    assertEquals(3, fullSelect.output().size());

    assertEquals(ColumnType.FILE_METADATA, fullSelect.output().get(0).columnType());
    assertEquals("filename", fullSelect.output().get(0).name());
    assertEquals(ColumnType.COLUMNS_ARRAY, fullSelect.output().get(1).columnType());
    assertEquals("columns", fullSelect.output().get(1).name());
    assertEquals(ColumnType.PARTITION, fullSelect.output().get(2).columnType());
    assertEquals("dir0", fullSelect.output().get(2).name());
  }

  @Test
  public void testEarlyColumnsArrayIncompatible() {
    SelectionListPlan.Builder sBuilder = new SelectionListPlan.Builder(fixture.options());
    sBuilder.useLegacyStarPlan(false);

    sBuilder.queryCols(TestSelectionListPlan.selectList("filename", "columns", "dir0"));
    SelectionListPlan selectList = sBuilder.build();

    MaterializedSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .add("d", MinorType.FLOAT8)
        .buildSchema();

    TableSelectionBuilder tBuilder = new TableSelectionBuilder(selectList);
    tBuilder.setTableSchema(tableSchema);
    tBuilder.setFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    try {
      tBuilder.build();
      fail();
    } catch (UserException e) {
      // Expected
    }
  }
}
