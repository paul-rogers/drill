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

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ColumnProjectionBuilder;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.OutputColumn.ColumnType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestProjectionPlanner extends SubOperatorTest {

  /**
   * Basic test: select a set of columns (a, b, c) when the
   * data source has an early schema of (a, c, d). (a, c) are
   * projected, (d) is null.
   */

  @Test
  public void testBasics() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    // Simulate SELECT a, b, c ...

    SchemaPath aSel = SchemaPath.getSimplePath("a");
    SchemaPath bSel = SchemaPath.getSimplePath("b");
    SchemaPath cSel = SchemaPath.getSimplePath("c");
    List<SchemaPath> selected = Lists.newArrayList(aSel, bSel, cSel);
    builder.queryCols(selected);

    // Simulate a data source, with early schema, of (a, b, d)

    MaterializedField aCol = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
    MaterializedField cCol = SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.OPTIONAL);
    MaterializedField dCol = SchemaBuilder.columnSchema("d", MinorType.FLOAT8, DataMode.REPEATED);
    List<MaterializedField> dataCols = Lists.newArrayList(aCol, cCol, dCol);
    builder.tableColumns(dataCols);

    // Not required to set source as not needed here.

    // Build the planner and verify

    ProjectionPlanner planner = builder.build();
    assertFalse(planner.isSelectAll());
    assertNull(planner.columnsCol());
    assertEquals(0, planner.partitionCols().size());

    assertEquals(3, planner.queryCols().size());
    assertEquals("a", planner.queryCols().get(0).name());
    assertEquals("b", planner.queryCols().get(1).name());
    assertEquals("c", planner.queryCols().get(2).name());

    assertEquals(3, planner.tableCols().size());
    assertEquals("a", planner.tableCols().get(0).name());
    assertEquals("c", planner.tableCols().get(1).name());
    assertEquals("d", planner.tableCols().get(2).name());
    assertSame(aCol, planner.tableCols().get(0).schema());
    assertSame(cCol, planner.tableCols().get(1).schema());
    assertSame(dCol, planner.tableCols().get(2).schema());

    assertEquals(2, planner.projectedCols().size());
    assertEquals("a", planner.projectedCols().get(0).name());
    assertEquals("c", planner.projectedCols().get(1).name());

    assertSame(planner.projectedCols().get(0).source(), planner.tableCols().get(0));
    assertSame(planner.projectedCols().get(1).source(), planner.tableCols().get(1));
    assertSame(planner.projectedCols().get(0), planner.tableCols().get(0).projection());
    assertSame(planner.projectedCols().get(1), planner.tableCols().get(1).projection());

    assertEquals(1, planner.nullCols().size());
    assertEquals("b", planner.nullCols().get(0).name());
    assertSame(planner.nullCols().get(0), planner.queryCols().get(1).projection());
    assertSame(planner.nullCols().get(0).projection(), planner.queryCols().get(1));

    assertEquals(3, planner.outputCols().size());
    assertEquals("a", planner.outputCols().get(0).name());
    assertEquals("b", planner.outputCols().get(1).name());
    assertEquals("c", planner.outputCols().get(2).name());

    assertEquals(ColumnType.DATA_SOURCE, planner.outputCols().get(0).columnType());
    assertEquals(ColumnType.NULL, planner.outputCols().get(1).columnType());
    assertEquals(ColumnType.DATA_SOURCE, planner.outputCols().get(2).columnType());

    assertSame(planner.outputCols().get(0), planner.projectedCols().get(0));
    assertSame(planner.outputCols().get(1), planner.nullCols().get(0));
    assertSame(planner.outputCols().get(2), planner.projectedCols().get(1));

    assertEquals(0, planner.outputCols().get(0).index());
    assertEquals(1, planner.outputCols().get(1).index());
    assertEquals(2, planner.outputCols().get(2).index());

    assertEquals(aCol, planner.outputCols().get(0).schema());
    assertEquals(planner.outputCols().get(1).schema().getType().getMinorType(), MinorType.VARCHAR);
    assertEquals(planner.outputCols().get(1).schema().getDataMode(), DataMode.OPTIONAL);
    assertEquals(cCol, planner.outputCols().get(2).schema());
  }

  /**
   * Simulate a SELECT * query: selects all data source columns in the order
   * defined by the data source.
   */

  @Test
  public void testStarQuery() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    // Simulate SELECT * ...

    builder.selectAll();

    // Simulate a data source, with early schema, of (a, b, d)

    MaterializedField aCol = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
    MaterializedField bCol = SchemaBuilder.columnSchema("C", MinorType.VARCHAR, DataMode.REQUIRED);
    MaterializedField dCol = SchemaBuilder.columnSchema("d", MinorType.VARCHAR, DataMode.REQUIRED);
    List<MaterializedField> dataCols = Lists.newArrayList(aCol, bCol, dCol);
    builder.tableColumns(dataCols);

    // Not required to set source as not needed here.

    // Build the planner and verify

    ProjectionPlanner planner = builder.build();
    assertTrue(planner.isSelectAll());
    assertNull(planner.columnsCol());
    assertNull(planner.queryCols());
    assertEquals(3, planner.tableCols().size());
    assertEquals(3, planner.projectedCols().size());
    assertTrue(planner.nullCols().isEmpty());
    assertEquals(0, planner.partitionCols().size());
    assertEquals(3, planner.outputCols().size());

    assertEquals("a", planner.outputCols().get(0).name());
    assertEquals("C", planner.outputCols().get(1).name());
    assertEquals("d", planner.outputCols().get(2).name());
  }

  /**
   * Test the special "columns" column that asks to return all columns
   * as an array. No need for early schema.
   */

  @Test
  public void testColumnsArray() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    // Simulate SELECT columns ...

    SchemaPath colsSel = SchemaPath.getSimplePath("columns");
    List<SchemaPath> selected = Lists.newArrayList(new SchemaPath[]{colsSel});
    builder.queryCols(selected);

    // For columns, need no early schema

    // Not required to set source as not needed here.

    // Build the planner and verify

    ProjectionPlanner planner = builder.build();
    assertFalse(planner.isSelectAll());
    assertNotNull(planner.columnsCol());
    assertEquals(1, planner.queryCols().size());
    assertEquals(0, planner.tableCols().size());
    assertEquals(0, planner.projectedCols().size());
    assertTrue(planner.nullCols().isEmpty());
    assertEquals(0, planner.partitionCols().size());
    assertEquals(1, planner.outputCols().size());

    assertEquals("columns", planner.outputCols().get(0).name());
    assertSame(planner.outputCols().get(0), planner.columnsCol());
    assertEquals(planner.outputCols().get(0).schema().getType().getMinorType(), MinorType.VARCHAR);
    assertEquals(planner.outputCols().get(0).schema().getDataMode(), DataMode.REPEATED);
    assertSame(planner.outputCols().get(0).projection(), planner.queryCols().get(0));
    assertSame(planner.outputCols().get(0), planner.queryCols().get(0).projection());
  }

  /**
   * Variation on early schema: SELECT order differs from data source column order.
   * SELECT order wins for output. Also verifies case insensitivity.
   */

  @Test
  public void testEarlySchema() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    // Simulate SELECT a, b, c ...

    SchemaPath aSel = SchemaPath.getSimplePath("a");
    SchemaPath bSel = SchemaPath.getSimplePath("b");
    SchemaPath cSel = SchemaPath.getSimplePath("c");
    List<SchemaPath> selected = Lists.newArrayList(cSel, bSel, aSel);
    builder.queryCols(selected);

    // Simulate a data source, with early schema, of (a, b, d)

    MaterializedField aCol = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
    MaterializedField bCol = SchemaBuilder.columnSchema("B", MinorType.VARCHAR, DataMode.REQUIRED);
    MaterializedField cCol = SchemaBuilder.columnSchema("C", MinorType.VARCHAR, DataMode.REQUIRED);
    List<MaterializedField> dataCols = Lists.newArrayList(aCol, bCol, cCol);
    builder.tableColumns(dataCols);

    // Not required to set source as not needed here.

    // Build the planner and verify

    ProjectionPlanner planner = builder.build();
    assertFalse(planner.isSelectAll());
    assertNull(planner.columnsCol());
    assertEquals(3, planner.queryCols().size());
    assertEquals(3, planner.tableCols().size());
    assertEquals(0, planner.partitionCols().size());

    assertEquals(3, planner.projectedCols().size());
    assertEquals("c", planner.projectedCols().get(0).name());
    assertEquals("b", planner.projectedCols().get(1).name());
    assertEquals("a", planner.projectedCols().get(2).name());
    assertSame(planner.projectedCols().get(0).source(), planner.tableCols().get(2));
    assertSame(planner.projectedCols().get(1).source(), planner.tableCols().get(1));
    assertSame(planner.projectedCols().get(2).source(), planner.tableCols().get(0));

    assertEquals(0, planner.nullCols().size());

    // Output columns take the case of the SELECT clause

    assertEquals(3, planner.outputCols().size());
    assertEquals("c", planner.outputCols().get(0).name());
    assertEquals("b", planner.outputCols().get(1).name());
    assertEquals("a", planner.outputCols().get(2).name());
  }

  @Test
  public void testLateSchema() {

  }

  @Test
  public void testImplicitColumnSelection() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    // Simulate SELECT a, b, c ...

    SchemaPath aSel = SchemaPath.getSimplePath("a");
    SchemaPath fqnSel = SchemaPath.getSimplePath("fqn");
    SchemaPath filePathSel = SchemaPath.getSimplePath("filEPath"); // Deliberate
    SchemaPath fileNameSel = SchemaPath.getSimplePath("filename");
    SchemaPath suffixSel = SchemaPath.getSimplePath("suffix");
    List<SchemaPath> selected = Lists.newArrayList(aSel, fqnSel, filePathSel, fileNameSel, suffixSel);
    builder.queryCols(selected);

    MaterializedField aCol = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
    List<MaterializedField> dataCols = Lists.newArrayList(aCol);
    builder.tableColumns(dataCols);

    Path path = new Path("hdfs:///x/y/z.csv");
    builder.setSource(path, null);

    ProjectionPlanner planner = builder.build();
    assertFalse(planner.isSelectAll());
    assertNull(planner.columnsCol());
    assertEquals(5, planner.queryCols().size());
    assertEquals(1, planner.tableCols().size());
    assertEquals(1, planner.projectedCols().size());
    assertEquals(0, planner.nullCols().size());
    assertEquals(0, planner.partitionCols().size());

    assertEquals(4, planner.implicitCols().size());

    assertEquals("fqn", planner.implicitCols().get(0).name());
    assertEquals("filEPath", planner.implicitCols().get(1).name());
    assertEquals("filename", planner.implicitCols().get(2).name());
    assertEquals("suffix", planner.implicitCols().get(3).name());

    assertEquals("hdfs:/x/y/z.csv", planner.implicitCols().get(0).value());
    assertEquals("hdfs:/x/y", planner.implicitCols().get(1).value());
    assertEquals("z.csv", planner.implicitCols().get(2).value());
    assertEquals("csv", planner.implicitCols().get(3).value());

    assertEquals(MinorType.VARCHAR, planner.implicitCols().get(0).schema().getType().getMinorType());
    assertEquals(DataMode.REQUIRED, planner.implicitCols().get(0).schema().getDataMode());

    assertEquals(5, planner.outputCols().size());

    assertEquals("a", planner.outputCols().get(0).name());
    assertEquals("fqn", planner.outputCols().get(1).name());
    assertEquals("filEPath", planner.outputCols().get(2).name());
    assertEquals("filename", planner.outputCols().get(3).name());
    assertEquals("suffix", planner.outputCols().get(4).name());

    assertSame(planner.implicitCols().get(0), planner.outputCols().get(1));
    assertSame(planner.implicitCols().get(1), planner.outputCols().get(2));
    assertSame(planner.implicitCols().get(2), planner.outputCols().get(3));
    assertSame(planner.implicitCols().get(3), planner.outputCols().get(4));

    // Verify bindings

    assertSame(planner.queryCols().get(1).projection(), planner.implicitCols().get(0));
    assertSame(planner.queryCols().get(2).projection(), planner.implicitCols().get(1));
    assertSame(planner.queryCols().get(3).projection(), planner.implicitCols().get(2));
    assertSame(planner.queryCols().get(4).projection(), planner.implicitCols().get(3));

    assertSame(planner.queryCols().get(1), planner.implicitCols().get(0).projection());
    assertSame(planner.queryCols().get(2), planner.implicitCols().get(1).projection());
    assertSame(planner.queryCols().get(3), planner.implicitCols().get(2).projection());
    assertSame(planner.queryCols().get(4), planner.implicitCols().get(3).projection());
  }

  /**
   * Test selection of implicit columns with implicit intermixed with
   * table columns.
   */

  @Test
  public void testImplicitColumnSelectSome() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    // Simulate SELECT a, b, c ...

    SchemaPath aSel = SchemaPath.getSimplePath("a");
    SchemaPath fileNameSel = SchemaPath.getSimplePath("filename");
    SchemaPath suffixSel = SchemaPath.getSimplePath("suffix");
    List<SchemaPath> selected = Lists.newArrayList(fileNameSel, aSel, suffixSel);
    builder.queryCols(selected);

    MaterializedField aCol = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
    List<MaterializedField> dataCols = Lists.newArrayList(aCol);
    builder.tableColumns(dataCols);

    Path path = new Path("hdfs:///x/y/z.csv");
    builder.setSource(path, null);

    ProjectionPlanner planner = builder.build();
    assertFalse(planner.isSelectAll());
    assertNull(planner.columnsCol());
    assertEquals(3, planner.queryCols().size());
    assertEquals(1, planner.tableCols().size());
    assertEquals(1, planner.projectedCols().size());
    assertEquals(0, planner.nullCols().size());
    assertEquals(2, planner.implicitCols().size());
    assertEquals(0, planner.partitionCols().size());
    assertEquals(3, planner.outputCols().size());

    assertEquals("filename", planner.outputCols().get(0).name());
    assertEquals("a", planner.outputCols().get(1).name());
    assertEquals("suffix", planner.outputCols().get(2).name());
  }

  @Test
  public void testImplicitColumnStar() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    builder.selectAll();

    MaterializedField aCol = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
    List<MaterializedField> dataCols = Lists.newArrayList(aCol);
    builder.tableColumns(dataCols);

    Path path = new Path("hdfs:///x/y/z.csv");
    builder.setSource(path, null);

    ProjectionPlanner planner = builder.build();

    assertTrue(planner.isSelectAll());
    assertNull(planner.columnsCol());
    assertNull(planner.queryCols());
    assertEquals(1, planner.tableCols().size());
    assertEquals(1, planner.projectedCols().size());
    assertEquals(0, planner.nullCols().size());
    assertEquals(4, planner.implicitCols().size());
    assertEquals(0, planner.partitionCols().size());
    assertEquals(5, planner.outputCols().size());

    assertEquals("A", planner.outputCols().get(0).name());
    assertEquals("fqn", planner.outputCols().get(1).name());
    assertEquals("filepath", planner.outputCols().get(2).name());
    assertEquals("filename", planner.outputCols().get(3).name());
    assertEquals("suffix", planner.outputCols().get(4).name());

    assertEquals(ColumnType.DATA_SOURCE, planner.outputCols().get(0).columnType());
    assertEquals(ColumnType.IMPLICIT, planner.outputCols().get(1).columnType());
  }

  @Test
  public void testPartitionColumnSelection() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    SchemaPath aSel = SchemaPath.getSimplePath("a");
    SchemaPath dir0Sel = SchemaPath.getSimplePath("dir0");
    SchemaPath dir1Sel = SchemaPath.getSimplePath("DIR1");
    SchemaPath dir2Sel = SchemaPath.getSimplePath("dir2");
    List<SchemaPath> selected = Lists.newArrayList(dir2Sel, dir1Sel, dir0Sel, aSel);
    builder.queryCols(selected);

    MaterializedField aCol = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
    List<MaterializedField> dataCols = Lists.newArrayList(aCol);
    builder.tableColumns(dataCols);

    Path path = new Path("hdfs:///w/x/y/z.csv");
    builder.setSource(path, "hdfs:///w");

    ProjectionPlanner planner = builder.build();
    assertFalse(planner.isSelectAll());
    assertNull(planner.columnsCol());
    assertEquals(4, planner.queryCols().size());
    assertEquals(1, planner.tableCols().size());
    assertEquals(1, planner.projectedCols().size());
    assertEquals(0, planner.nullCols().size());
    assertEquals(0, planner.implicitCols().size());
    assertEquals(3, planner.partitionCols().size());
    assertEquals(4, planner.outputCols().size());

    assertEquals("dir2", planner.outputCols().get(0).name());
    assertEquals("DIR1", planner.outputCols().get(1).name());
    assertEquals("dir0", planner.outputCols().get(2).name());
    assertEquals("a", planner.outputCols().get(3).name());

    assertEquals(MinorType.VARCHAR, planner.outputCols().get(0).schema().getType().getMinorType());
    assertEquals(DataMode.OPTIONAL, planner.outputCols().get(0).schema().getDataMode());

    assertEquals(ColumnType.PARTITION, planner.outputCols().get(0).columnType());
    assertEquals(ColumnType.DATA_SOURCE, planner.outputCols().get(3).columnType());

    assertNull(planner.partitionCols().get(0).value());
    assertEquals("y", planner.partitionCols().get(1).value());
    assertEquals("x", planner.partitionCols().get(2).value());
  }

  /**
   * Test the obscure case that the partition column contains two digits:
   * dir11. Also tests the obscure case that the output only has partition
   * columns.
   */

  @Test
  public void testPartitionColumnTwoDigits() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    SchemaPath dir11Sel = SchemaPath.getSimplePath("dir11");
    List<SchemaPath> selected = Lists.newArrayList(new SchemaPath[]{dir11Sel});
    builder.queryCols(selected);

    MaterializedField aCol = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
    List<MaterializedField> dataCols = Lists.newArrayList(aCol);
    builder.tableColumns(dataCols);

    Path path = new Path("hdfs:///x/0/1/2/3/4/5/6/7/8/9/10/d11/z.csv");
    builder.setSource(path, "hdfs:///x");

    ProjectionPlanner planner = builder.build();
    assertTrue(planner.projectedCols().isEmpty());

    assertEquals("dir11", planner.outputCols().get(0).name());
    assertEquals("d11", planner.partitionCols().get(0).value());
  }

  /**
   * Test a SELECT * query with a partition path. Creates entries for
   * the dirn items plus the implicit columns.
   */

  @Test
  public void testPartitionColumnStar() {
    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    builder.selectAll();

    MaterializedField aCol = SchemaBuilder.columnSchema("A", MinorType.VARCHAR, DataMode.REQUIRED);
    List<MaterializedField> dataCols = Lists.newArrayList(aCol);
    builder.tableColumns(dataCols);

    Path path = new Path("hdfs:///w/x/y/z.csv");
    builder.setSource(path, "hdfs:///w");

    ProjectionPlanner planner = builder.build();
    assertEquals(7, planner.outputCols().size());

    assertEquals("A", planner.outputCols().get(0).name());
    assertEquals("dir0", planner.outputCols().get(1).name());
    assertEquals("dir1", planner.outputCols().get(2).name());
    assertEquals("fqn", planner.outputCols().get(3).name());

    assertEquals("x", planner.partitionCols().get(0).value());
    assertEquals("y", planner.partitionCols().get(1).value());
  }

  // TODO: Test customizing partition name, implicit col names.
}
