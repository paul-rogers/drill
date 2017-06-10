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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.SchemaTracker;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ExpectedTableColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.MetadataColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.NullColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ResolvedFileInfo;
import org.apache.drill.exec.physical.impl.scan.QuerySelectionPlan.FileMetadataColumnDefn;
import org.apache.drill.exec.physical.impl.scan.QuerySelectionPlan.SelectColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.EarlyProjectedColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.TableColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjector.MetadataColumnLoader;
import org.apache.drill.exec.physical.impl.scan.ScanProjector.NullColumnLoader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.LogicalTupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.TupleSetImpl.TupleLoaderImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;
import org.apache.drill.exec.vector.ValueVector;
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

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    List<MetadataColumn> defns = new ArrayList<>();
    FileMetadataColumnDefn iDefn = new FileMetadataColumnDefn("suffix", ImplicitFileColumns.SUFFIX);
    FileMetadataColumn iCol = new FileMetadataColumn(buildSelectCol("suffix"), iDefn, null, fileInfo);
    defns.add(iCol);

    PartitionColumn pCol = new PartitionColumn(buildSelectCol("dir1"), 1, null, fileInfo);
    defns.add(pCol);

    ResultVectorCache cache = new ResultVectorCache(fixture.allocator());
    MetadataColumnLoader staticLoader = new MetadataColumnLoader(fixture.allocator(), defns, cache);

    // Create a batch

    staticLoader.load(2);

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("suffix", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add("csv", "y")
        .add("csv", "y")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(staticLoader.output()));
    staticLoader.close();
  }

  @SuppressWarnings("resource")
  @Test
  public void testNullColumnLoader() {

    List<NullColumn> defns = new ArrayList<>();
    defns.add(new NullColumn(buildSelectCol("req")));
    defns.add(new NullColumn(buildSelectCol("opt")));
    defns.add(new NullColumn(buildSelectCol("rep")));
    defns.add(new NullColumn(buildSelectCol("unk")));

    // Populate the cache with a column of each mode.

    ResultVectorCache cache = new ResultVectorCache(fixture.allocator());
    cache.addOrGet(SchemaBuilder.columnSchema("req", MinorType.FLOAT8, DataMode.REQUIRED));
    ValueVector opt = cache.addOrGet(SchemaBuilder.columnSchema("opt", MinorType.FLOAT8, DataMode.OPTIONAL));
    ValueVector rep = cache.addOrGet(SchemaBuilder.columnSchema("rep", MinorType.FLOAT8, DataMode.REPEATED));

    // Use nullable Varchar for unknown null columns.

    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();
    NullColumnLoader staticLoader = new NullColumnLoader(fixture.allocator(), defns, cache, nullType);

    // Create a batch

    staticLoader.load(2);

    // Verify vectors are reused

    VectorContainer output = staticLoader.output();
    assertSame(opt, output.getValueVector(1).getValueVector());
    assertSame(rep, output.getValueVector(2).getValueVector());

    // Verify values and types

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("req", MinorType.FLOAT8)
        .addNullable("opt", MinorType.FLOAT8)
        .addArray("rep", MinorType.FLOAT8)
        .addNullable("unk", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(null, null, new int[] { }, null)
        .add(null, null, new int[] { }, null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(output));
    staticLoader.close();
  }

  @Test
  public void testEarlySchemaSelectStar() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

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

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(projection);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
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
    projector.publish();

    // Verify

    SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
        .add(1, "fred")
        .add(2, "wilma")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  @Test
  public void testEarlySchemaSelectAll() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    // Define the projection plan.
    // SELECT a, b FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestScanProjectionPlanner.selectList("a", "b"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(projection);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
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
    projector.publish();

    // Verify

    SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
        .add(1, "fred")
        .add(2, "wilma")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  @Test
  public void testEarlySchemaSelectAllReorder() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    // Define the projection plan.
    // SELECT b, a FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestScanProjectionPlanner.selectList("b", "a"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(projection);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
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
    projector.publish();

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
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  @Test
  public void testEarlySchemaSelectExtra() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    // Define the projection plan.
    // SELECT a, b, c FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestScanProjectionPlanner.selectList("a", "b", "c"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(projection);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
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
    projector.publish();

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
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  @Test
  public void testEarlySchemaSelectExtraCustomType() {

    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();
    ScanProjector projector = new ScanProjector(fixture.allocator(), nullType);

    // Define the projection plan.
    // SELECT a, b, c FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestScanProjectionPlanner.selectList("a", "b", "c"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(projection);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
    loader.startBatch();

    TupleLoader writer = loader.writer();
    assertTrue(writer instanceof TupleLoaderImpl);
    for (int i = 0; i < 2; i++) {
      loader.startRow();
      writer.column(0).setInt((i+1));
      writer.column(1).setString(bValues[i]);
      loader.saveRow();
    }
    projector.publish();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addNullable("c", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(1, "fred", null)
        .add(2, "wilma", null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  @Test
  public void testEarlySchemaSelectSubset() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    // Define the projection plan.
    // SELECT a FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestScanProjectionPlanner.selectList("a"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(projection);

    // Create a batch of data.

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
    projector.publish();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(1)
        .add(2)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  @Test
  public void testEarlySchemaSelectAllAndMetadata() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    // Define the projection plan.
    // SELECT a, b, dir0, suffix FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestScanProjectionPlanner.selectList("a", "b", "dir0", "suffix"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    // Set file path

    Path path = new Path("hdfs:///w/x/y/z.csv");
    builder.setSource(path, "hdfs:///w");

    ScanProjection projection = builder.build();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(projection);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
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
    projector.publish();

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
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  @Test
  public void testEarlySchemaSelectNone() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    // Define the projection plan.
    // SELECT c FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestScanProjectionPlanner.selectList("c"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);
    ScanProjection projection = builder.build();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(projection);

    // Create a batch of data.

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
    projector.publish();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  @Test
  public void testMixture() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    // Define the projection plan.
    // SELECT dir0, a, suffix, c FROM table(a, b)

    ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
    builder.queryCols(TestScanProjectionPlanner.selectList("dir0", "b", "suffix", "c"));

    BatchSchema tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();
    builder.tableColumns(tableSchema);

    // Set file path

    Path path = new Path("hdfs:///w/x/y/z.csv");
    builder.setSource(path, "hdfs:///w");

    ScanProjection projection = builder.build();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(projection);

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
    loader.startBatch();

    // Should be a direct writer, no projection
    TupleLoader writer = loader.writer();
    assertTrue(writer instanceof LogicalTupleLoader);
    for (int i = 0; i < 2; i++) {
      loader.startRow();
      assertNull(writer.column(0));
      writer.column(1).setString(bValues[i]);
      loader.saveRow();
    }
    projector.publish();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("dir0", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("suffix", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add("x", "fred", "csv", null)
        .add("x", "wilma", "csv", null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  /**
   * Test the ability of the scan projector to "smooth" out schema changes
   * by reusing the type from a previous reader, if known. That is,
   * given three readers:<br>
   * (a, b)<br>
   * (b)<br>
   * (a, b)<br>
   * Then the type of column a should be preserved for the second reader that
   * does not include a. This works if a is nullable. If so, a's type will
   * be used for the empty column, rather than the usual nullable int.
   * <p>
   * Detailed testing of type matching for "missing" columns is done
   * in {@link #testNullColumnLoader()}.
   * <p>
   * As a side effect, makes sure that two identical tables (in this case,
   * separated by a different table) results in no schema change.
   */

  @Test
  public void testSchemaSmoothing() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    BatchSchema twoColSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();
    BatchSchema oneColSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // Projection of (a, b) to (a, b)

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("a", "b"));
      builder.tableColumns(twoColSchema);
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow(10, "fred")
          .loadRow(20, "wilma");
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(twoColSchema)
          .add(10, "fred")
          .add(20, "wilma")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Projection of (a) to (a, b), reusing b from above.

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("a", "b"));
      builder.tableColumns(oneColSchema);
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow(30)
          .loadRow(40);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(oneColSchema)
          .add(30)
          .add(40)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Projection of (a, b), to (a, b), reusing b yet again

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("a", "b"));
      builder.tableColumns(twoColSchema);
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow(50, "dino")
          .loadRow(60, "barney");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(twoColSchema)
          .add(50, "dino")
          .add(60, "barney")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * Verify that different table column orders are projected into the
   * SELECT order, preserving vectors, so no schema change for column
   * reordering.
   */

  @Test
  public void testColumnReordering() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    BatchSchema schema1 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("c", MinorType.BIGINT)
        .build();
    BatchSchema schema2 = new SchemaBuilder()
        .add("c", MinorType.BIGINT)
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();
    BatchSchema schema3 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("c", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // Projection of (a, b, c) to (a, b, c)

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("a", "b", "c"));
      builder.tableColumns(schema1);
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow(10, "fred", 110L)
          .loadRow(20, "wilma", 110L);
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(schema1)
          .add(10, "fred", 110L)
          .add(20, "wilma", 110L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Projection of (c, a, b) to (a, b, c)

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("a", "b", "c"));
      builder.tableColumns(schema2);
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow(330L, 30, "bambam")
          .loadRow(440L, 40, "betty");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(schema1)
          .add(30, "bambam", 330L)
          .add(40, "betty", 440L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Projection of (a, c, b) to (a, b, c)

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("a", "b", "c"));
      builder.tableColumns(schema3);
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow(50, 550L, "dino")
          .loadRow(60, 660L, "barney");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(schema1)
          .add(50, "dino", 550L)
          .add(60, "barney", 660L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * A SELECT * query uses the schema of the table as the output schema.
   * This is trivial when the scanner has one table. But, if two or more
   * tables occur, then things get interesting. The first table sets the
   * schema. The second table then has:
   * <ul>
   * <li>The same schema, trivial case.</li>
   * <li>A subset of the first table. The type of the "missing" column
   * from the first table is used for a null column in the second table.</li>
   * <li>A superset or disjoint set of the first schema. This triggers a hard schema
   * change.</li>
   * </ul>
   * <p>
   * It is an open question whether previous columns should be preserved on
   * a hard reset. For now, the code implements, and this test verifies, that a
   * hard reset clears the "memory" of prior schemas.
   */

  @Test
  public void testSelectStarSmoothing() {

    ScanProjector projector = new ScanProjector(fixture.allocator(), null);

    BatchSchema firstSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .addNullable("c", MinorType.BIGINT)
        .build();
    BatchSchema subsetSchema = new SchemaBuilder()
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("a", MinorType.INT)
        .build();
    BatchSchema disjointSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("d", MinorType.VARCHAR)
        .build();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // First table, establishes the baseline

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("*"));
      builder.tableColumns(firstSchema);
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow(10, "fred", 110L)
          .loadRow(20, "wilma", 110L);
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(firstSchema)
          .add(10, "fred", 110L)
          .add(20, "wilma", 110L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Second table, same schema, the trivial case

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("*"));
      builder.tableColumns(firstSchema);
      builder.priorSchema(projector.output().getSchema());
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow(70, "pebbles", 770L)
          .loadRow(80, "hoppy", 880L);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(firstSchema)
          .add(70, "pebbles", 770L)
          .add(80, "hoppy", 880L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Third table: subset schema of first two

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("*"));
      builder.tableColumns(subsetSchema);
      builder.priorSchema(projector.output().getSchema());
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow("bambam", 30)
          .loadRow("betty", 40);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(firstSchema)
          .add(30, "bambam", null)
          .add(40, "betty", null)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Fourth table: disjoint schema, cases a schema reset

      ProjectionPlanner builder = new ProjectionPlanner(fixture.options());
      builder.queryCols(TestScanProjectionPlanner.selectList("*"));
      builder.tableColumns(disjointSchema);
      builder.priorSchema(projector.output().getSchema());
      ScanProjection projection = builder.build();
      ResultSetLoader loader = projector.makeTableLoader(projection);

      loader.startBatch();
      loader.writer()
          .loadRow(50, "dino", "supporting")
          .loadRow(60, "barney", "main");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertNotEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(disjointSchema)
          .add(50, "dino", "supporting")
          .add(60, "barney", "main")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  // TODO: Flavors of schema evolution
  // TODO: Late schema testing
  // TODO: Test schema smoothing with repeated
}
