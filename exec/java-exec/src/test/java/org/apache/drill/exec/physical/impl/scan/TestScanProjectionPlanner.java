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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.OutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.TableSchemaType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestScanProjectionPlanner extends SubOperatorTest {

  static List<SchemaPath> selectList(String... names) {
    List<SchemaPath> selected = new ArrayList<>();
    for (String name: names) {
      selected.add(SchemaPath.getSimplePath(name));
    }
    return selected;
  }

  /**
   * Basic test: select a set of columns (a, b, c) when the
   * data source has an early schema of (a, c, d). (a, c) are
   * projected, (d) is null.
   */

  @Test
  public void testBasics() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.queryCols(selectList("a", "b", "c"));

    // Simulate a table with early schema, of (a, b, d)

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .build();
    builder.tableColumns(tableSchema);

    // Not required to set source as not needed here.

    // Build the projection plan and verify

    ScanProjection projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertEquals(TableSchemaType.EARLY, projection.tableSchemaType());
    assertNull(projection.columnsCol());
    assertEquals(0, projection.partitionCols().size());

    assertEquals(3, projection.queryCols().size());
    assertEquals("a", projection.queryCols().get(0).name());
    assertEquals("b", projection.queryCols().get(1).name());
    assertEquals("c", projection.queryCols().get(2).name());

    assertEquals(3, projection.tableCols().size());
    assertEquals("a", projection.tableCols().get(0).name());
    assertEquals("c", projection.tableCols().get(1).name());
    assertEquals("d", projection.tableCols().get(2).name());
    assertSame(tableSchema.getColumn(0), projection.tableCols().get(0).schema());
    assertSame(tableSchema.getColumn(1), projection.tableCols().get(1).schema());
    assertSame(tableSchema.getColumn(2), projection.tableCols().get(2).schema());

    assertEquals(2, projection.projectedCols().size());
    assertEquals("a", projection.projectedCols().get(0).name());
    assertEquals("c", projection.projectedCols().get(1).name());

    assertSame(projection.projectedCols().get(0).source(), projection.tableCols().get(0));
    assertSame(projection.projectedCols().get(1).source(), projection.tableCols().get(1));
    assertSame(projection.projectedCols().get(0), projection.tableCols().get(0).projection());
    assertSame(projection.projectedCols().get(1), projection.tableCols().get(1).projection());

    assertEquals(1, projection.nullCols().size());
    assertEquals("b", projection.nullCols().get(0).name());
    assertSame(projection.nullCols().get(0), projection.queryCols().get(1).projection());
    assertSame(projection.nullCols().get(0).projection(), projection.queryCols().get(1));

    assertEquals(3, projection.outputCols().size());
    assertEquals("a", projection.outputCols().get(0).name());
    assertEquals("b", projection.outputCols().get(1).name());
    assertEquals("c", projection.outputCols().get(2).name());

    assertEquals(ColumnType.EARLY_TABLE, projection.outputCols().get(0).columnType());
    assertEquals(ColumnType.NULL, projection.outputCols().get(1).columnType());
    assertEquals(ColumnType.EARLY_TABLE, projection.outputCols().get(2).columnType());

    assertSame(projection.outputCols().get(0), projection.projectedCols().get(0));
    assertSame(projection.outputCols().get(1), projection.nullCols().get(0));
    assertSame(projection.outputCols().get(2), projection.projectedCols().get(1));

    assertEquals(0, projection.outputCols().get(0).index());
    assertEquals(1, projection.outputCols().get(1).index());
    assertEquals(2, projection.outputCols().get(2).index());

    assertEquals(tableSchema.getColumn(0), projection.outputCols().get(0).schema());
    // Dummy column, defaults to nullable int
    assertEquals(projection.outputCols().get(1).schema().getType().getMinorType(), MinorType.INT);
    assertEquals(projection.outputCols().get(1).schema().getDataMode(), DataMode.OPTIONAL);
    assertEquals(tableSchema.getColumn(1), projection.outputCols().get(2).schema());
  }

  /**
   * Simulate a SELECT * query: selects all data source columns in the order
   * defined by the data source.
   */

  @Test
  public void testSelectAll() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.useLegacyStarPlan(false);

    // Simulate SELECT * ...

    builder.selectAll();

    // Simulate a data source, with early schema, of (a, C, d)

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    // Not required to set source as not needed here.

    // Build the planner and verify

    ScanProjection projection = builder.build();
    assertTrue(projection.isSelectAll());
    assertEquals(TableSchemaType.EARLY, projection.tableSchemaType());
    assertNull(projection.columnsCol());
    assertEquals(1, projection.queryCols().size());
    assertTrue(projection.queryCols().get(0).isStar());
    assertEquals(3, projection.tableCols().size());
    assertEquals(3, projection.projectedCols().size());
    assertTrue(projection.nullCols().isEmpty());
    assertEquals(0, projection.partitionCols().size());
    assertEquals(3, projection.outputCols().size());

    assertEquals("a", projection.outputCols().get(0).name());
    assertEquals("C", projection.outputCols().get(1).name());
    assertEquals("d", projection.outputCols().get(2).name());
  }

  /**
   * Simulate a SELECT * query by passing "*" as a column name.
   */

  @Test
  public void testStarQuery() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    // Simulate SELECT * ...

    builder.queryCols(selectList("*"));

    // Simulate a data source, with early schema, of (a, C, d)

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    // Not required to set source as not needed here.

    // Build the planner and verify

    ScanProjection projection = builder.build();
    assertEquals(TableSchemaType.EARLY, projection.tableSchemaType());
    assertEquals("a", projection.outputCols().get(0).name());
    assertEquals("C", projection.outputCols().get(1).name());
    assertEquals("d", projection.outputCols().get(2).name());
  }

  /**
   * Test the special "columns" column that asks to return all columns
   * as an array. No need for early schema. This case is special: it actually
   * creates the one and only table column to match the desired output column.
   */

  @Test
  public void testColumnsArray() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    // Simulate SELECT columns ...

    builder.queryCols(selectList("columns"));

    // For columns, need no early schema

    // Not required to set source as not needed here.

    // Build the planner and verify

    ScanProjection projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertEquals(TableSchemaType.EARLY, projection.tableSchemaType());
    assertNotNull(projection.columnsCol());
    assertEquals(1, projection.queryCols().size());
    assertEquals(1, projection.projectedCols().size());
    assertTrue(projection.nullCols().isEmpty());
    assertEquals(0, projection.partitionCols().size());
    assertEquals(1, projection.outputCols().size());

    assertEquals("columns", projection.outputCols().get(0).name());
    assertSame(projection.outputCols().get(0), projection.columnsCol());
    assertEquals(projection.outputCols().get(0).schema().getType().getMinorType(), MinorType.VARCHAR);
    assertEquals(projection.outputCols().get(0).schema().getDataMode(), DataMode.REPEATED);
    assertSame(projection.outputCols().get(0).projection(), projection.queryCols().get(0));
    assertSame(projection.outputCols().get(0), projection.queryCols().get(0).projection());

    assertEquals(1, projection.tableCols().size());
    assertEquals("columns", projection.tableCols().get(0).name());
    assertSame(projection.tableCols().get(0).projection(), projection.columnsCol());
    assertEquals(projection.tableCols().get(0).schema().getType().getMinorType(), MinorType.VARCHAR);
    assertEquals(projection.tableCols().get(0).schema().getDataMode(), DataMode.REPEATED);
  }

  /**
   * Variation on early schema: SELECT order differs from data source column order.
   * SELECT order wins for output. Also verifies case insensitivity.
   */

  @Test
  public void testEarlySchema() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    // Simulate SELECT c, b, a ...

    builder.queryCols(selectList("c", "b", "a"));

    // Simulate a data source, with early schema, of (a, b, d)

    BatchSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    // Not required to set source as not needed here.

    // Build the planner and verify

    ScanProjection projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertEquals(TableSchemaType.EARLY, projection.tableSchemaType());
    assertNull(projection.columnsCol());
    assertEquals(3, projection.queryCols().size());
    assertEquals(3, projection.tableCols().size());
    assertEquals(0, projection.partitionCols().size());

    assertEquals(3, projection.projectedCols().size());
    assertEquals("c", projection.projectedCols().get(0).name());
    assertEquals("b", projection.projectedCols().get(1).name());
    assertEquals("a", projection.projectedCols().get(2).name());
    assertSame(projection.projectedCols().get(0).source(), projection.tableCols().get(2));
    assertSame(projection.projectedCols().get(1).source(), projection.tableCols().get(1));
    assertSame(projection.projectedCols().get(2).source(), projection.tableCols().get(0));

    assertEquals(0, projection.nullCols().size());

    // Output columns take the case of the SELECT clause

    assertEquals(3, projection.outputCols().size());
    assertEquals("c", projection.outputCols().get(0).name());
    assertEquals("b", projection.outputCols().get(1).name());
    assertEquals("a", projection.outputCols().get(2).name());
  }

  @Test
  public void testLateSchema() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.queryCols(selectList("c", "b", "a"));

    // Late schema: no table schema provided

    // Not required to set source as not needed here.

    // Build the plan and verify

    ScanProjection projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertEquals(TableSchemaType.LATE, projection.tableSchemaType());
    assertNull(projection.columnsCol());
    assertEquals(3, projection.queryCols().size());
    assertNull(projection.tableCols());
    assertEquals(0, projection.partitionCols().size());
    assertEquals(0, projection.nullCols().size());

    assertEquals(3, projection.projectedCols().size());
    assertEquals("c", projection.projectedCols().get(0).name());
    assertEquals("b", projection.projectedCols().get(1).name());
    assertEquals("a", projection.projectedCols().get(2).name());
    assertNull(projection.projectedCols().get(0).source());
    assertNull(projection.projectedCols().get(1).source());
    assertNull(projection.projectedCols().get(2).source());
    assertEquals(ColumnType.LATE_TABLE, projection.outputCols().get(0).columnType());
    assertEquals(ColumnType.LATE_TABLE, projection.outputCols().get(1).columnType());
    assertEquals(ColumnType.LATE_TABLE, projection.outputCols().get(2).columnType());
    assertSame(projection.projectedCols().get(0).projection(), projection.queryCols().get(0));
    assertSame(projection.projectedCols().get(1).projection(), projection.queryCols().get(1));
    assertSame(projection.projectedCols().get(2).projection(), projection.queryCols().get(2));
  }

  @Test
  public void testImplicitColumnSelection() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.queryCols(selectList("a", "fqn",
                                 "filEPath", // Sic, to test case sensitivity
                                 "filename", "suffix"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    Path path = new Path("hdfs:///x/y/z.csv");
    builder.setSource(path, null);

    ScanProjection projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertNull(projection.columnsCol());
    assertEquals(5, projection.queryCols().size());
    assertEquals(1, projection.tableCols().size());
    assertEquals(1, projection.projectedCols().size());
    assertEquals(0, projection.nullCols().size());
    assertEquals(0, projection.partitionCols().size());

    assertEquals(4, projection.fileInfoCols().size());

    assertEquals("fqn", projection.fileInfoCols().get(0).name());
    assertEquals("filEPath", projection.fileInfoCols().get(1).name());
    assertEquals("filename", projection.fileInfoCols().get(2).name());
    assertEquals("suffix", projection.fileInfoCols().get(3).name());

    assertEquals("hdfs:/x/y/z.csv", projection.fileInfoCols().get(0).value());
    assertEquals("hdfs:/x/y", projection.fileInfoCols().get(1).value());
    assertEquals("z.csv", projection.fileInfoCols().get(2).value());
    assertEquals("csv", projection.fileInfoCols().get(3).value());

    assertEquals(MinorType.VARCHAR, projection.fileInfoCols().get(0).schema().getType().getMinorType());
    assertEquals(DataMode.REQUIRED, projection.fileInfoCols().get(0).schema().getDataMode());

    assertEquals(5, projection.outputCols().size());

    assertEquals("a", projection.outputCols().get(0).name());
    assertEquals("fqn", projection.outputCols().get(1).name());
    assertEquals("filEPath", projection.outputCols().get(2).name());
    assertEquals("filename", projection.outputCols().get(3).name());
    assertEquals("suffix", projection.outputCols().get(4).name());

    assertSame(projection.fileInfoCols().get(0), projection.outputCols().get(1));
    assertSame(projection.fileInfoCols().get(1), projection.outputCols().get(2));
    assertSame(projection.fileInfoCols().get(2), projection.outputCols().get(3));
    assertSame(projection.fileInfoCols().get(3), projection.outputCols().get(4));

    // Verify bindings

    assertSame(projection.queryCols().get(1).projection(), projection.fileInfoCols().get(0));
    assertSame(projection.queryCols().get(2).projection(), projection.fileInfoCols().get(1));
    assertSame(projection.queryCols().get(3).projection(), projection.fileInfoCols().get(2));
    assertSame(projection.queryCols().get(4).projection(), projection.fileInfoCols().get(3));

    assertSame(projection.queryCols().get(1), projection.fileInfoCols().get(0).projection());
    assertSame(projection.queryCols().get(2), projection.fileInfoCols().get(1).projection());
    assertSame(projection.queryCols().get(3), projection.fileInfoCols().get(2).projection());
    assertSame(projection.queryCols().get(4), projection.fileInfoCols().get(3).projection());
  }

  /**
   * Test selection of implicit columns with implicit intermixed with
   * table columns.
   */

  @Test
  public void testImplicitColumnSelectSome() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.queryCols(selectList("filename", "a", "suffix"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    Path path = new Path("hdfs:///x/y/z.csv");
    builder.setSource(path, null);

    ScanProjection projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertNull(projection.columnsCol());
    assertEquals(3, projection.queryCols().size());
    assertEquals(1, projection.tableCols().size());
    assertEquals(1, projection.projectedCols().size());
    assertEquals(0, projection.nullCols().size());
    assertEquals(2, projection.fileInfoCols().size());
    assertEquals(0, projection.partitionCols().size());
    assertEquals(3, projection.outputCols().size());

    assertEquals("filename", projection.outputCols().get(0).name());
    assertEquals("a", projection.outputCols().get(1).name());
    assertEquals("suffix", projection.outputCols().get(2).name());
  }

  /**
   * The `columns` column is special: can't be used with other column names.
   * Make sure that the rule <i>does not</i> apply to implicit columns.
   */

  @Test
  public void testImplicitColumnsWithColumnsArray() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.queryCols(selectList("filename", "columns", "suffix"));

    Path path = new Path("hdfs:///x/y/z.csv");
    builder.setSource(path, null);

    ScanProjection projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertNotNull(projection.columnsCol());
    assertEquals(3, projection.queryCols().size());
    assertEquals(1, projection.tableCols().size());
    assertEquals(1, projection.projectedCols().size());
    assertEquals(0, projection.nullCols().size());
    assertEquals(2, projection.fileInfoCols().size());
    assertEquals(0, projection.partitionCols().size());
    assertEquals(3, projection.outputCols().size());

    assertEquals("filename", projection.outputCols().get(0).name());
    assertEquals("columns", projection.outputCols().get(1).name());
    assertEquals("suffix", projection.outputCols().get(2).name());
  }

  /**
   * For obscure reasons, Drill 1.10 and earlier would add all implicit
   * columns in a SELECT *, then would remove them again in a PROJECT
   * if not needed.
   */

  @Test
  public void testImplicitColumnStarLegacy() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.useLegacyStarPlan(true);

    builder.selectAll();

    BatchSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    Path path = new Path("hdfs:///x/y/z.csv");
    builder.setSource(path, null);

    ScanProjection projection = builder.build();

    assertTrue(projection.isSelectAll());
    assertNull(projection.columnsCol());
    assertEquals(1, projection.queryCols().size());
    assertEquals(1, projection.tableCols().size());
    assertEquals(1, projection.projectedCols().size());
    assertEquals(0, projection.nullCols().size());
    assertEquals(4, projection.fileInfoCols().size());
    assertEquals(0, projection.partitionCols().size());
    assertEquals(5, projection.outputCols().size());

    assertEquals("A", projection.outputCols().get(0).name());
    assertEquals("fqn", projection.outputCols().get(1).name());
    assertEquals("filepath", projection.outputCols().get(2).name());
    assertEquals("filename", projection.outputCols().get(3).name());
    assertEquals("suffix", projection.outputCols().get(4).name());

    assertEquals(ColumnType.EARLY_TABLE, projection.outputCols().get(0).columnType());
    assertEquals(ColumnType.IMPLICIT, projection.outputCols().get(1).columnType());
  }

  /**
   * Verify that partition columns, in any case, work.
   */

  @Test
  public void testPartitionColumnSelection() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.queryCols(selectList("dir2", "DIR1", "dir0", "a"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    Path path = new Path("hdfs:///w/x/y/z.csv");
    builder.setSource(path, "hdfs:///w");

    ScanProjection projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertNull(projection.columnsCol());
    assertEquals(4, projection.queryCols().size());
    assertEquals(1, projection.tableCols().size());
    assertEquals(1, projection.projectedCols().size());
    assertEquals(0, projection.nullCols().size());
    assertEquals(0, projection.fileInfoCols().size());
    assertEquals(3, projection.partitionCols().size());
    assertEquals(4, projection.outputCols().size());

    assertEquals("dir2", projection.outputCols().get(0).name());
    assertEquals("DIR1", projection.outputCols().get(1).name());
    assertEquals("dir0", projection.outputCols().get(2).name());
    assertEquals("a", projection.outputCols().get(3).name());

    assertEquals(MinorType.VARCHAR, projection.outputCols().get(0).schema().getType().getMinorType());
    assertEquals(DataMode.OPTIONAL, projection.outputCols().get(0).schema().getDataMode());

    assertEquals(ColumnType.PARTITION, projection.outputCols().get(0).columnType());
    assertEquals(ColumnType.EARLY_TABLE, projection.outputCols().get(3).columnType());

    assertNull(projection.partitionCols().get(0).value());
    assertEquals("y", projection.partitionCols().get(1).value());
    assertEquals("x", projection.partitionCols().get(2).value());
  }

  /**
   * Test the obscure case that the partition column contains two digits:
   * dir11. Also tests the obscure case that the output only has partition
   * columns.
   */

  @Test
  public void testPartitionColumnTwoDigits() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.queryCols(selectList("dir11"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    Path path = new Path("hdfs:///x/0/1/2/3/4/5/6/7/8/9/10/d11/z.csv");
    builder.setSource(path, "hdfs:///x");

    ScanProjection projection = builder.build();
    assertTrue(projection.projectedCols().isEmpty());

    assertEquals("dir11", projection.outputCols().get(0).name());
    assertEquals("d11", projection.partitionCols().get(0).value());
  }

  /**
   * Test a SELECT * query with a partition path. Creates entries for
   * the dirn items plus the implicit columns in Drill 1.10 and before
   * (and perhaps later.) In those versions, SELECT * includes all implicit
   * columns, and all partition columns if a path is provided.
   */

  @Test
  public void testPartitionColumnStarLegacy() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.useLegacyStarPlan(true);

    builder.selectAll();

    BatchSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    Path path = new Path("hdfs:///w/x/y/z.csv");
    builder.setSource(path, "hdfs:///w");

    ScanProjection projection = builder.build();
    assertEquals(7, projection.outputCols().size());

    assertEquals("A", projection.outputCols().get(0).name());
    assertEquals("dir0", projection.outputCols().get(1).name());
    assertEquals("dir1", projection.outputCols().get(2).name());
    assertEquals("fqn", projection.outputCols().get(3).name());

    assertEquals("x", projection.partitionCols().get(0).value());
    assertEquals("y", projection.partitionCols().get(1).value());
  }

  /**
   * In some version of Drill, 1.11 or later, SELECT * will select only table
   * columns, not implicit columns or partitions.
   * Ensure that the projection planner handles this case.
   */

  @Test
  public void testPartitionColumnStarRevised() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.useLegacyStarPlan(false);

    builder.selectAll();

    BatchSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    Path path = new Path("hdfs:///w/x/y/z.csv");
    builder.setSource(path, "hdfs:///w");

    ScanProjection projection = builder.build();
    assertEquals(1, projection.outputCols().size());

    assertEquals("A", projection.outputCols().get(0).name());
  }

  @Test
  public void testErrorStarAndColumns() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.queryCols(selectList("*", "a"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorColumnAndStar() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.queryCols(selectList("a", "*"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorTwoStars() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.queryCols(selectList("*", "*"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorColumnsAndColumn() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.queryCols(selectList("columns", "a"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorColumnAndColumns() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.queryCols(selectList("a", "columns"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorTwoColumnsArray() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.queryCols(selectList("columns", "columns"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorColumnsAndSchema() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    BatchSchema tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    builder.queryCols(selectList("columns"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorInvalidPath() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.selectAll();
    Path path = new Path("hdfs:///w/x/y/z.csv");
    try {
      builder.setSource(path, "hdfs:///bad");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorShortPath() {
    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());

    builder.selectAll();
    Path path = new Path("hdfs:///w/z.csv");
    try {
      builder.setSource(path, "hdfs:///w/x/y");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  //-----------------------------------------------------------------
  // Prior schema tests

  /**
   * Case in which the table schema is a superset of the prior
   * schema. Discard prior schema.
   */

  @Test
  public void testPriorSmaller() {
    BatchSchema priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();
    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    ScanProjection projection = new ProjectionPlanner(fixture.options())
        .selectAll()
        .priorSchema(priorSchema)
        .tableColumns(tableSchema)
        .build();
    assertTrue(projection.outputSchema().isEquivalent(tableSchema));
  }

  /**
   * Case in which the table schema and prior are disjoint
   * sets. Discard the prior schema.
   */

  @Test
  public void testPriorDisjoint() {
    BatchSchema priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();
    BatchSchema tableSchema = new SchemaBuilder()
        .add("b", MinorType.VARCHAR)
        .build();
    ScanProjection projection = new ProjectionPlanner(fixture.options())
        .selectAll()
        .priorSchema(priorSchema)
        .tableColumns(tableSchema)
        .build();
    assertTrue(projection.outputSchema().isEquivalent(tableSchema));
  }

  /**
   * Column names match, but types differe. Discard the prior schema.
   */

  @Test
  public void testPriorDifferentTypes() {
    BatchSchema priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR)
        .build();
    ScanProjection projection = new ProjectionPlanner(fixture.options())
        .selectAll()
        .priorSchema(priorSchema)
        .tableColumns(tableSchema)
        .build();
    assertTrue(projection.outputSchema().isEquivalent(tableSchema));
  }

  /**
   * The prior and table schemas are identical. Discard the prior
   * schema (though, the output is no different than if we preserved
   * the prior schema...)
   */

  @Test
  public void testPriorSameSchemas() {
    BatchSchema priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    ScanProjection projection = new ProjectionPlanner(fixture.options())
        .selectAll()
        .priorSchema(priorSchema)
        .tableColumns(tableSchema)
        .build();
    assertTrue(projection.outputSchema().isEquivalent(tableSchema));
  }

  /**
   * Can't preserve the prior schema if it had required columns
   * where the table has no columns.
   */

  @Test
  public void testPriorRequired() {
    BatchSchema priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR)
        .build();
    BatchSchema tableSchema = new SchemaBuilder()
        .addNullable("b", MinorType.VARCHAR)
        .build();
    ScanProjection projection = new ProjectionPlanner(fixture.options())
        .selectAll()
        .priorSchema(priorSchema)
        .tableColumns(tableSchema)
        .build();
    assertTrue(projection.outputSchema().isEquivalent(tableSchema));
  }

  /**
   * Preserve the prior schema if table is a subset and missing columns
   * are nullable or repeated.
   */

  @Test
  public void testPriorSmoothing() {
    BatchSchema priorSchema = new SchemaBuilder()
        .addNullable("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addArray("c", MinorType.BIGINT)
        .build();
    BatchSchema tableSchema = new SchemaBuilder()
        .add("b", MinorType.VARCHAR)
        .build();
    ScanProjection projection = new ProjectionPlanner(fixture.options())
        .selectAll()
        .priorSchema(priorSchema)
        .tableColumns(tableSchema)
        .build();
    assertTrue(projection.outputSchema().isEquivalent(priorSchema));
  }

  /**
   * Preserve the prior schema if table is a subset. Map the table
   * columns to the output using the prior schema orderng.
   */

  @Test
  public void testPriorReordering() {
    BatchSchema priorSchema = new SchemaBuilder()
        .addNullable("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addArray("c", MinorType.BIGINT)
        .build();
    BatchSchema tableSchema = new SchemaBuilder()
        .add("b", MinorType.VARCHAR)
        .addNullable("a", MinorType.INT)
        .build();
    ScanProjection projection = new ProjectionPlanner(fixture.options())
        .selectAll()
        .priorSchema(priorSchema)
        .tableColumns(tableSchema)
        .build();
    assertTrue(projection.outputSchema().isEquivalent(priorSchema));
  }

  // TODO: Test customizing partition name, implicit col names.
}
