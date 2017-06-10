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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ResolvedFileInfo;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.TypedColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.WildcardColumn;
import org.apache.drill.exec.physical.impl.scan.QuerySelectionPlan.SelectType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestQuerySelectionPlan extends SubOperatorTest {

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
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.queryCols(selectList("a", "b", "c"));

    // Build the projection plan and verify

    QuerySelectionPlan querySelPlan = builder.build();
    assertFalse(querySelPlan.isSelectAll());
    assertEquals(SelectType.LIST, querySelPlan.selectType());
    assertFalse(querySelPlan.hasMetadata());
    assertTrue(querySelPlan.useLegacyWildcardPartition());

    assertEquals(3, querySelPlan.queryCols().size());
    assertEquals("a", querySelPlan.queryCols().get(0).name());
    assertEquals("b", querySelPlan.queryCols().get(1).name());
    assertEquals("c", querySelPlan.queryCols().get(2).name());

    assertEquals(3, querySelPlan.outputCols().size());
    assertEquals("a", querySelPlan.outputCols().get(0).name());
    assertEquals("b", querySelPlan.outputCols().get(1).name());
    assertEquals("c", querySelPlan.outputCols().get(2).name());

    // Verify bindings

    assertSame(querySelPlan.outputCols().get(0), querySelPlan.queryCols().get(0).resolution());
    assertSame(querySelPlan.outputCols().get(1), querySelPlan.queryCols().get(1).resolution());
    assertSame(querySelPlan.outputCols().get(2), querySelPlan.queryCols().get(2).resolution());

    assertSame(querySelPlan.outputCols().get(0).source(), querySelPlan.queryCols().get(0));
    assertSame(querySelPlan.outputCols().get(1).source(), querySelPlan.queryCols().get(1));
    assertSame(querySelPlan.outputCols().get(2).source(), querySelPlan.queryCols().get(2));

    // Verify column type

    assertEquals(ColumnType.TABLE, querySelPlan.outputCols().get(0).columnType());

    // Table column selection

    assertEquals(3, querySelPlan.tableColNames().size());
    assertEquals("a", querySelPlan.tableColNames().get(0));
    assertEquals("b", querySelPlan.tableColNames().get(1));
    assertEquals("c", querySelPlan.tableColNames().get(2));
  }

  /**
   * Simulate a SELECT * query: selects all data source columns in the order
   * defined by the data source.
   */

  @Test
  public void testSelectAll() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());
    builder.useLegacyStarPlan(false);

    // Simulate SELECT * ...

    builder.selectAll();

    QuerySelectionPlan querySelPlan = builder.build();
    assertTrue(querySelPlan.isSelectAll());
    assertEquals(SelectType.WILDCARD, querySelPlan.selectType());
    assertFalse(querySelPlan.useLegacyWildcardPartition());
    assertTrue(querySelPlan.tableColNames().isEmpty());

    assertEquals(1, querySelPlan.queryCols().size());
    assertTrue(querySelPlan.queryCols().get(0).isWildcard());

    assertEquals(1, querySelPlan.outputCols().size());
    assertEquals("*", querySelPlan.outputCols().get(0).name());
    assertFalse(((OutputColumn.WildcardColumn) (querySelPlan.outputCols().get(0))).includePartitions());

    // Verify bindings

    assertSame(querySelPlan.outputCols().get(0), querySelPlan.queryCols().get(0).resolution());
    assertSame(querySelPlan.outputCols().get(0).source(), querySelPlan.queryCols().get(0));

    // Verify column type

    assertEquals(ColumnType.WILDCARD, querySelPlan.outputCols().get(0).columnType());
  }

  /**
   * Simulate a SELECT * query by passing "*" as a column name.
   */

  @Test
  public void testStarQuery() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());
    builder.useLegacyStarPlan(false);

    // Simulate SELECT * ...

    builder.queryCols(selectList("*"));

    QuerySelectionPlan querySelPlan = builder.build();
    assertTrue(querySelPlan.isSelectAll());
    assertEquals(SelectType.WILDCARD, querySelPlan.selectType());
    assertEquals(1, querySelPlan.queryCols().size());
    assertTrue(querySelPlan.queryCols().get(0).isWildcard());

    assertEquals(1, querySelPlan.outputCols().size());
    assertEquals("*", querySelPlan.outputCols().get(0).name());
    assertFalse(((OutputColumn.WildcardColumn) (querySelPlan.outputCols().get(0))).includePartitions());

    // Verify bindings

    assertSame(querySelPlan.outputCols().get(0), querySelPlan.queryCols().get(0).resolution());
    assertSame(querySelPlan.outputCols().get(0).source(), querySelPlan.queryCols().get(0));

    // Verify column type

    assertEquals(ColumnType.WILDCARD, querySelPlan.outputCols().get(0).columnType());
  }

  /**
   * Test the special "columns" column that asks to return all columns
   * as an array. No need for early schema. This case is special: it actually
   * creates the one and only table column to match the desired output column.
   */

  @Test
  public void testColumnsArray() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    // Simulate SELECT columns ...

    builder.queryCols(selectList("columns"));

    // Build the planner and verify

    QuerySelectionPlan querySelPlan = builder.build();
    assertFalse(querySelPlan.isSelectAll());
    assertEquals(SelectType.COLUMNS_ARRAY, querySelPlan.selectType());
    assertEquals(1, querySelPlan.queryCols().size());
    assertTrue(querySelPlan.tableColNames().isEmpty());

    assertEquals(1, querySelPlan.outputCols().size());
    assertEquals("columns", querySelPlan.outputCols().get(0).name());

    // Verify bindings

    assertSame(querySelPlan.outputCols().get(0), querySelPlan.queryCols().get(0).resolution());
    assertSame(querySelPlan.outputCols().get(0).source(), querySelPlan.queryCols().get(0));

    // Verify column type

    assertEquals(ColumnType.COLUMNS_ARRAY, querySelPlan.outputCols().get(0).columnType());
  }


  @Test
  public void testFileMetadataColumnSelection() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.queryCols(selectList("a", "fqn",
                                 "filEPath", // Sic, to test case sensitivity
                                 "filename", "suffix"));

    QuerySelectionPlan projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertEquals(5, projection.queryCols().size());

    assertEquals(5, projection.outputCols().size());

    assertEquals("a", projection.outputCols().get(0).name());
    assertEquals("fqn", projection.outputCols().get(1).name());
    assertEquals("filEPath", projection.outputCols().get(2).name());
    assertEquals("filename", projection.outputCols().get(3).name());
    assertEquals("suffix", projection.outputCols().get(4).name());

    assertEquals(MinorType.VARCHAR, ((OutputColumn.TypedColumn) (projection.outputCols().get(1))).type().getMinorType());
    assertEquals(DataMode.REQUIRED, ((OutputColumn.TypedColumn) (projection.outputCols().get(1))).type().getMode());

    // Verify bindings

    assertSame(projection.outputCols().get(1), projection.queryCols().get(1).resolution());
    assertSame(projection.outputCols().get(1).source(), projection.queryCols().get(1));

    // Verify column type

    assertEquals(ColumnType.TABLE, projection.outputCols().get(0).columnType());
    assertEquals(ColumnType.FILE_METADATA, projection.outputCols().get(1).columnType());
  }

  /**
   * The `columns` column is special: can't be used with other column names.
   * Make sure that the rule <i>does not</i> apply to implicit columns.
   */

  @Test
  public void testImplicitColumnsWithColumnsArray() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    builder.queryCols(selectList("filename", "columns", "suffix"));

    QuerySelectionPlan projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertEquals(SelectType.COLUMNS_ARRAY, projection.selectType());

    assertEquals(3, projection.outputCols().size());

    assertEquals("filename", projection.outputCols().get(0).name());
    assertEquals("columns", projection.outputCols().get(1).name());
    assertEquals("suffix", projection.outputCols().get(2).name());

    // Verify column type

    assertEquals(ColumnType.FILE_METADATA, projection.outputCols().get(0).columnType());
    assertEquals(ColumnType.COLUMNS_ARRAY, projection.outputCols().get(1).columnType());
    assertEquals(ColumnType.FILE_METADATA, projection.outputCols().get(2).columnType());
  }

  /**
   * Verify that partition columns, in any case, work.
   */

  @Test
  public void testPartitionColumnSelection() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    builder.queryCols(selectList("dir2", "DIR1", "dir0", "a"));

    QuerySelectionPlan projection = builder.build();

    assertEquals(4, projection.outputCols().size());
    assertEquals("dir2", projection.outputCols().get(0).name());
    assertEquals("DIR1", projection.outputCols().get(1).name());
    assertEquals("dir0", projection.outputCols().get(2).name());
    assertEquals("a", projection.outputCols().get(3).name());

    // Verify bindings

    assertSame(projection.outputCols().get(1), projection.queryCols().get(1).resolution());
    assertSame(projection.outputCols().get(1).source(), projection.queryCols().get(1));

    // Verify column type

    assertEquals(ColumnType.PARTITION, projection.outputCols().get(0).columnType());

    // Verify data type

    assertEquals(MinorType.VARCHAR, ((OutputColumn.TypedColumn) (projection.outputCols().get(0))).type().getMinorType());
    assertEquals(DataMode.OPTIONAL, ((OutputColumn.TypedColumn) (projection.outputCols().get(0))).type().getMode());
  }

  @Test
  public void testErrorWildcardLegacyAndFileMetaata() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());
    builder.useLegacyStarPlan(true);

    builder.queryCols(selectList("filename", "*"));

    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testErrorStarAndColumns() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    builder.queryCols(selectList("*", "a"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorWildcardLegacyAndPartition() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());
    builder.useLegacyStarPlan(true);

    builder.queryCols(selectList("*", "dir8"));

    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testErrorColumnAndStar() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

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
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    builder.queryCols(selectList("*", "*"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorColumnsArrayAndColumn() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    builder.queryCols(selectList("columns", "a"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorColumnAndColumnsArray() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

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
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    builder.queryCols(selectList("columns", "columns"));
    try {
      builder.build();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }


  @Test
  public void testErrorInvalidPath() {
    Path path = new Path("hdfs:///w/x/y/z.csv");
    try {
      new OutputColumn.ResolvedFileInfo(path, "hdfs:///bad");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testErrorShortPath() {
    Path path = new Path("hdfs:///w/z.csv");
    try {
      new OutputColumn.ResolvedFileInfo(path, "hdfs:///w/x/y");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}
