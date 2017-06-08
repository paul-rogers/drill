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
import org.apache.drill.exec.physical.impl.scan.SelectionListPlan.SelectType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestSelectionListPlan extends SubOperatorTest {

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
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.queryCols(selectList("a", "b", "c"));

    // Build the projection plan and verify

    SelectionListPlan projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertEquals(SelectType.LIST, projection.selectType());

    assertEquals(3, projection.queryCols().size());
    assertEquals("a", projection.queryCols().get(0).name());
    assertEquals("b", projection.queryCols().get(1).name());
    assertEquals("c", projection.queryCols().get(2).name());

    assertEquals(3, projection.outputCols().size());
    assertEquals("a", projection.outputCols().get(0).name());
    assertEquals("b", projection.outputCols().get(1).name());
    assertEquals("c", projection.outputCols().get(2).name());

    // Verify bindings

    assertSame(projection.outputCols().get(0), projection.queryCols().get(0).resolution());
    assertSame(projection.outputCols().get(1), projection.queryCols().get(1).resolution());
    assertSame(projection.outputCols().get(2), projection.queryCols().get(2).resolution());

    assertSame(projection.outputCols().get(0).source(), projection.queryCols().get(0));
    assertSame(projection.outputCols().get(1).source(), projection.queryCols().get(1));
    assertSame(projection.outputCols().get(2).source(), projection.queryCols().get(2));

    // Verify column type

    assertEquals(ColumnType.TABLE, projection.outputCols().get(0).columnType());
  }

  /**
   * Simulate a SELECT * query: selects all data source columns in the order
   * defined by the data source.
   */

  @Test
  public void testSelectAll() {
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());
    builder.useLegacyStarPlan(false);

    // Simulate SELECT * ...

    builder.selectAll();

    SelectionListPlan projection = builder.build();
    assertTrue(projection.isSelectAll());
    assertEquals(SelectType.WILDCARD, projection.selectType());

    assertEquals(1, projection.queryCols().size());
    assertTrue(projection.queryCols().get(0).isWildcard());

    assertEquals(1, projection.outputCols().size());
    assertEquals("*", projection.outputCols().get(0).name());
    assertFalse(((OutputColumn.WildcardColumn) (projection.outputCols().get(0))).includePartitions());

    // Verify bindings

    assertSame(projection.outputCols().get(0), projection.queryCols().get(0).resolution());
    assertSame(projection.outputCols().get(0).source(), projection.queryCols().get(0));

    // Verify column type

    assertEquals(ColumnType.WILDCARD, projection.outputCols().get(0).columnType());
  }

  /**
   * Simulate a SELECT * query by passing "*" as a column name.
   */

  @Test
  public void testStarQuery() {
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());
    builder.useLegacyStarPlan(false);

    // Simulate SELECT * ...

    builder.queryCols(selectList("*"));

    SelectionListPlan projection = builder.build();
    assertTrue(projection.isSelectAll());
    assertEquals(SelectType.WILDCARD, projection.selectType());

    assertEquals(1, projection.queryCols().size());
    assertTrue(projection.queryCols().get(0).isWildcard());

    assertEquals(1, projection.outputCols().size());
    assertEquals("*", projection.outputCols().get(0).name());
    assertFalse(((OutputColumn.WildcardColumn) (projection.outputCols().get(0))).includePartitions());

    // Verify bindings

    assertSame(projection.outputCols().get(0), projection.queryCols().get(0).resolution());
    assertSame(projection.outputCols().get(0).source(), projection.queryCols().get(0));

    // Verify column type

    assertEquals(ColumnType.WILDCARD, projection.outputCols().get(0).columnType());
  }

  /**
   * Test the special "columns" column that asks to return all columns
   * as an array. No need for early schema. This case is special: it actually
   * creates the one and only table column to match the desired output column.
   */

  @Test
  public void testColumnsArray() {
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

    // Simulate SELECT columns ...

    builder.queryCols(selectList("columns"));

    // Build the planner and verify

    SelectionListPlan projection = builder.build();
    assertFalse(projection.isSelectAll());
    assertEquals(SelectType.COLUMNS_ARRAY, projection.selectType());
    assertEquals(1, projection.queryCols().size());

    assertEquals(1, projection.outputCols().size());
    assertEquals("columns", projection.outputCols().get(0).name());

    // Verify bindings

    assertSame(projection.outputCols().get(0), projection.queryCols().get(0).resolution());
    assertSame(projection.outputCols().get(0).source(), projection.queryCols().get(0));

    // Verify column type

    assertEquals(ColumnType.COLUMNS_ARRAY, projection.outputCols().get(0).columnType());
  }


  @Test
  public void testFileMetadataColumnSelection() {
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

    // Simulate SELECT a, b, c ...

    builder.queryCols(selectList("a", "fqn",
                                 "filEPath", // Sic, to test case sensitivity
                                 "filename", "suffix"));

    SelectionListPlan projection = builder.build();
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

    // Verify resolving the value for the metadata columns

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///x/y/z.csv"), (String) null);
    OutputColumn.FileMetadataColumn mdCol = (OutputColumn.FileMetadataColumn) projection.outputCols().get(1);
    assertEquals("hdfs:/x/y/z.csv", mdCol.value(fileInfo));
    mdCol = (OutputColumn.FileMetadataColumn) projection.outputCols().get(2);
    assertEquals("hdfs:/x/y", mdCol.value(fileInfo));
    mdCol = (OutputColumn.FileMetadataColumn) projection.outputCols().get(3);
    assertEquals("z.csv", mdCol.value(fileInfo));
    mdCol = (OutputColumn.FileMetadataColumn) projection.outputCols().get(4);
    assertEquals("csv", mdCol.value(fileInfo));
  }

  /**
   * The `columns` column is special: can't be used with other column names.
   * Make sure that the rule <i>does not</i> apply to implicit columns.
   */

  @Test
  public void testImplicitColumnsWithColumnsArray() {
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

    builder.queryCols(selectList("filename", "columns", "suffix"));

    SelectionListPlan projection = builder.build();
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
   * For obscure reasons, Drill 1.10 and earlier would add all implicit
   * columns in a SELECT *, then would remove them again in a PROJECT
   * if not needed.
   */

  @Test
  public void testImplicitColumnStarLegacy() {
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());
    builder.useLegacyStarPlan(true);

    builder.selectAll();

    SelectionListPlan projection = builder.build();

    assertTrue(projection.isSelectAll());
    assertEquals(5, projection.outputCols().size());

    assertEquals(SelectionListPlan.WILDCARD, projection.outputCols().get(0).name());
    assertTrue(((OutputColumn.WildcardColumn) (projection.outputCols().get(0))).includePartitions());
    assertEquals("fqn", projection.outputCols().get(1).name());
    assertEquals("filepath", projection.outputCols().get(2).name());
    assertEquals("filename", projection.outputCols().get(3).name());
    assertEquals("suffix", projection.outputCols().get(4).name());
  }

  /**
   * Verify that partition columns, in any case, work.
   */

  @Test
  public void testPartitionColumnSelection() {
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

    builder.queryCols(selectList("dir2", "DIR1", "dir0", "a"));

    SelectionListPlan projection = builder.build();

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

    // Verify resolving the value for the partition columns

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    OutputColumn.PartitionColumn pCol = (OutputColumn.PartitionColumn) projection.outputCols().get(2);
    assertEquals("x", pCol.value(fileInfo));
    pCol = (OutputColumn.PartitionColumn) projection.outputCols().get(1);
    assertEquals("y", pCol.value(fileInfo));
    pCol = (OutputColumn.PartitionColumn) projection.outputCols().get(0);
    assertNull(pCol.value(fileInfo));
  }

  /**
   * Test the obscure case that the partition column contains two digits:
   * dir11. Also tests the obscure case that the output only has partition
   * columns.
   */

  @Test
  public void testPartitionColumnTwoDigits() {
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

    builder.queryCols(selectList("dir11"));

    SelectionListPlan projection = builder.build();
    assertEquals("dir11", projection.outputCols().get(0).name());

    Path path = new Path("hdfs:///x/0/1/2/3/4/5/6/7/8/9/10/d11/z.csv");
    OutputColumn.ResolvedFileInfo fileInfo = new OutputColumn.ResolvedFileInfo(path, "hdfs:///x");
    OutputColumn.PartitionColumn pCol = (OutputColumn.PartitionColumn) projection.outputCols().get(0);
    assertEquals("d11", pCol.value(fileInfo));
  }

  // TODO: testPartitionColumnStarLegacy
  // TODO: testPartitionColumnStarRevised

  @Test
  public void testErrorStarAndColumns() {
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

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
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

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
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

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
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

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
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

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
    SelectionListPlan.Builder builder = new SelectionListPlan.Builder(fixture.options());

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
