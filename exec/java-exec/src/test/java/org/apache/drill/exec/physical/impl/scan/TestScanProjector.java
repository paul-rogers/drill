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

import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ImplicitColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ImplicitColumnDefn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.SelectColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.StaticColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjector.StaticColumnLoader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.LogicalTupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.TupleSetImpl.TupleLoaderImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.NullColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ScanProjection;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestScanProjector extends SubOperatorTest {

  public SelectColumn buildSelectCol(String name) {
    return new SelectColumn(SchemaPath.getSimplePath(name));
  }

  /**
   * Test the static column loader using one column of each type.
   * The null column is of type int, but the associated value is of
   * type string. This is a bit odd, but works out because we detect that
   * the string value is null and call setNull on the writer, and avoid
   * using the actual data.
   */

  @Test
  public void testStaticColumnLoader() {

    List<StaticColumn> defns = new ArrayList<>();
    defns.add(new NullColumn(buildSelectCol("null"), 0,
        SchemaBuilder.columnSchema("null", MinorType.INT, DataMode.OPTIONAL)));

    ImplicitColumnDefn iDefn = new ImplicitColumnDefn("suffix", ImplicitFileColumns.SUFFIX);
    ImplicitColumn iCol = new ImplicitColumn(buildSelectCol("suffix"), 1, iDefn);
    defns.add(iCol);
    Path path = new Path("hdfs:///w/x/y/z.csv");
    iCol.setValue(path);

    PartitionColumn pCol = new PartitionColumn(buildSelectCol("dir0"), 2, 0);
    defns.add(pCol);
    pCol.setValue("w");

    StaticColumnLoader staticLoader = new StaticColumnLoader(fixture.allocator(), defns);

    // Create a batch

    staticLoader.load(2);

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("null", MinorType.INT)
        .addNullable("suffix", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(null, "csv", "w")
        .add(null, "csv", "w")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(staticLoader.output()));
  }

  @Test
  public void testEarlySchemaSelectStar() {

    // Define the projection plan.
    // SELECT * FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.selectAll();

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the scan projector.

    ScanProjector.Builder scanBuilder = new ScanProjector.Builder(fixture.allocator(), projection);
    ScanProjector scanProj = scanBuilder.build();
    assertTrue(scanProj instanceof ScanProjector.EarlySchemaProjectPlan);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
    ResultSetLoader loader = scanProj.tableLoader();
    loader.startBatch();
    TupleLoader writer = loader.writer();

    // Should be a direct writer, no projection
    assertTrue(writer instanceof TupleLoaderImpl);
    for (int i = 0; i < 2; i++) {
      loader.startRow();
      writer.column(0).setInt((i+1));
      writer.column(1).setString(bValues[i]);
      loader.saveRow();
    }
    scanProj.build();

    // Verify

    SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
        .add(1, "fred")
        .add(2, "wilma")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanProj.output()));
  }

  @Test
  public void testEarlySchemaSelectAll() {
    // Define the projection plan.
    // SELECT a, b FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestProjectionPlanner.selectList("a", "b"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the scan projector.

    ScanProjector.Builder scanBuilder = new ScanProjector.Builder(fixture.allocator(), projection);
    ScanProjector scanProj = scanBuilder.build();
    assertTrue(scanProj instanceof ScanProjector.EarlySchemaProjectPlan);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
    ResultSetLoader loader = scanProj.tableLoader();
    loader.startBatch();

    // Should be a direct writer, no projection
    TupleLoader writer = loader.writer();
    assertTrue(writer instanceof TupleLoaderImpl);
    for (int i = 0; i < 2; i++) {
      loader.startRow();
      writer.column(0).setInt((i+1));
      writer.column(1).setString(bValues[i]);
      loader.saveRow();
    }
    scanProj.build();

    // Verify

    SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
        .add(1, "fred")
        .add(2, "wilma")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanProj.output()));
  }

  @Test
  public void testEarlySchemaSelectAllReorder() {
    // Define the projection plan.
    // SELECT b, a FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestProjectionPlanner.selectList("b", "a"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the scan projector.

    ScanProjector.Builder scanBuilder = new ScanProjector.Builder(fixture.allocator(), projection);
    ScanProjector scanProj = scanBuilder.build();
    assertTrue(scanProj instanceof ScanProjector.EarlySchemaProjectPlan);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
    ResultSetLoader loader = scanProj.tableLoader();
    loader.startBatch();

    // Should be a direct writer, no projection
    TupleLoader writer = loader.writer();
    assertTrue(writer instanceof TupleLoaderImpl);
    for (int i = 0; i < 2; i++) {
      loader.startRow();
      writer.column(0).setInt((i+1));
      writer.column(1).setString(bValues[i]);
      loader.saveRow();
    }
    scanProj.build();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("b", MinorType.VARCHAR)
        .add("a", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add("fred", 1)
        .add("wilma", 2)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanProj.output()));
  }

  @Test
  public void testEarlySchemaSelectExtra() {
    // Define the projection plan.
    // SELECT a, b, c FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestProjectionPlanner.selectList("a", "b", "c"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the scan projector.

    ScanProjector.Builder scanBuilder = new ScanProjector.Builder(fixture.allocator(), projection);
    ScanProjector scanProj = scanBuilder.build();
    assertTrue(scanProj instanceof ScanProjector.EarlySchemaProjectPlan);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
    ResultSetLoader loader = scanProj.tableLoader();
    loader.startBatch();

    // Should be a direct writer, no projection
    TupleLoader writer = loader.writer();
    assertTrue(writer instanceof TupleLoaderImpl);
    for (int i = 0; i < 2; i++) {
      loader.startRow();
      writer.column(0).setInt((i+1));
      writer.column(1).setString(bValues[i]);
      loader.saveRow();
    }
    scanProj.build();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(1, "fred", null)
        .add(2, "wilma", null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanProj.output()));
  }

  @Test
  public void testEarlySchemaSelectSubset() {
    // Define the projection plan.
    // SELECT a FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestProjectionPlanner.selectList("a"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the scan projector.

    ScanProjector.Builder scanBuilder = new ScanProjector.Builder(fixture.allocator(), projection);
    ScanProjector scanProj = scanBuilder.build();
    assertTrue(scanProj instanceof ScanProjector.EarlySchemaProjectPlan);

    // Create a batch of data.

    ResultSetLoader loader = scanProj.tableLoader();
    loader.startBatch();

    // Should be a projection
    TupleLoader writer = loader.writer();
    assertTrue(writer instanceof LogicalTupleLoader);
    for (int i = 0; i < 2; i++) {
      loader.startRow();
      writer.column(0).setInt((i+1));
      assertNull(writer.column(1));
      loader.saveRow();
    }
    scanProj.build();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(1)
        .add(2)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanProj.output()));
  }

  @Test
  public void testEarlySchemaSelectAllAndStatic() {
    // Define the projection plan.
    // SELECT a, b, dir0, suffix FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestProjectionPlanner.selectList("a", "b", "dir0", "suffix"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    // Set file path

    Path path = new Path("hdfs:///w/x/y/z.csv");
    builder.setSource(path, "hdfs:///w");

    ScanProjection projection = builder.build();

    // Create the scan projector.

    ScanProjector.Builder scanBuilder = new ScanProjector.Builder(fixture.allocator(), projection);
    ScanProjector scanProj = scanBuilder.build();
    assertTrue(scanProj instanceof ScanProjector.EarlySchemaProjectPlan);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
    ResultSetLoader loader = scanProj.tableLoader();
    loader.startBatch();

    // Should be a direct writer, no projection
    TupleLoader writer = loader.writer();
    assertTrue(writer instanceof TupleLoaderImpl);
    for (int i = 0; i < 2; i++) {
      loader.startRow();
      writer.column(0).setInt((i+1));
      writer.column(1).setString(bValues[i]);
      loader.saveRow();
    }
    scanProj.build();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .add("suffix", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(1, "fred", "x", "csv")
        .add(2, "wilma", "x", "csv")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanProj.output()));
  }

  @Test
  public void testEarlySchemaSelectNone() {
    // Define the projection plan.
    // SELECT c FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestProjectionPlanner.selectList("c"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the scan projector.

    ScanProjector.Builder scanBuilder = new ScanProjector.Builder(fixture.allocator(), projection);
    ScanProjector scanProj = scanBuilder.build();
    assertTrue(scanProj instanceof ScanProjector.EarlySchemaProjectPlan);

    // Create a batch of data.

    ResultSetLoader loader = scanProj.tableLoader();
    loader.startBatch();

    // Should be a projection
    TupleLoader writer = loader.writer();
    assertTrue(writer instanceof LogicalTupleLoader);
    for (int i = 0; i < 2; i++) {
      loader.startRow();
      assertNull(writer.column(0));
      assertNull(writer.column(1));
      loader.saveRow();
    }
    scanProj.build();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanProj.output()));
  }


}
