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
import org.apache.drill.exec.physical.impl.scan.ScanProjection.EarlyProjectedColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.FileInfoColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.ImplicitColumnDefn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.LateProjectedColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.OutputColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.SelectColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.StaticColumn;
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

    List<StaticColumn> defns = new ArrayList<>();
    ImplicitColumnDefn iDefn = new ImplicitColumnDefn("suffix", ImplicitFileColumns.SUFFIX);
    FileInfoColumn iCol = new FileInfoColumn(buildSelectCol("suffix"), 0, iDefn);
    defns.add(iCol);
    Path path = new Path("hdfs:///w/x/y/z.csv");
    iCol.setValue(path);

    PartitionColumn pCol = new PartitionColumn(buildSelectCol("dir0"), 1, 0);
    defns.add(pCol);
    pCol.setValue("w");

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
        .add("csv", "w")
        .add("csv", "w")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(staticLoader.output()));
    staticLoader.close();
  }

  @SuppressWarnings("resource")
  @Test
  public void testNullColumnLoader() {

    List<OutputColumn> defns = new ArrayList<>();
    defns.add(new LateProjectedColumn(buildSelectCol("req"), 0));
    defns.add(new LateProjectedColumn(buildSelectCol("opt"), 1));
    defns.add(new LateProjectedColumn(buildSelectCol("rep"), 2));
    defns.add(new EarlyProjectedColumn(buildSelectCol("defreq"), 3,
        new TableColumn(0, SchemaBuilder.columnSchema("defreq", MinorType.BIGINT, DataMode.REQUIRED))));
    defns.add(new EarlyProjectedColumn(buildSelectCol("defopt"), 4,
        new TableColumn(1, SchemaBuilder.columnSchema("defopt", MinorType.BIGINT, DataMode.OPTIONAL))));
    defns.add(new LateProjectedColumn(buildSelectCol("unk"), 5));

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
        .addNullable("defreq", MinorType.BIGINT)
        .addNullable("defopt", MinorType.BIGINT)
        .addNullable("unk", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(null, null, new int[] { }, null, null, null)
        .add(null, null, new int[] { }, null, null, null)
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

  // TODO: Test two identical tables -- vectors are identical
  // TODO: Test columns
  // TODO: Test version tracking
  // TODO: Flavors of schema evolution
  // TODO: Schema smoothing for SELECT *
  // TODO: Schema smoothing for reordered schema
}
