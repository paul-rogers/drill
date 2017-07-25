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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.SchemaTracker;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.MetadataColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.NullColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.ScanOutputColumn.RequestedTableColumn;
import org.apache.drill.exec.physical.impl.scan.ScanLevelProjection.FileMetadata;
import org.apache.drill.exec.physical.impl.scan.ScanLevelProjection.FileMetadataColumnDefn;
import org.apache.drill.exec.physical.impl.scan.ScanLevelProjection.RequestedColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjector.MetadataColumnLoader;
import org.apache.drill.exec.physical.impl.scan.ScanProjector.NullColumnLoader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.physical.rowSet.impl.LogicalTupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.TupleSetImpl.TupleLoaderImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
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

  public RequestedColumn buildSelectCol(String name) {
    return new RequestedColumn(SchemaPath.getSimplePath(name));
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

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    List<MetadataColumn> defns = new ArrayList<>();
    FileMetadataColumnDefn iDefn = new FileMetadataColumnDefn("suffix", ImplicitFileColumns.SUFFIX);
    FileMetadataColumn iCol = FileMetadataColumn.resolved(buildSelectCol("suffix"), iDefn, fileInfo);
    defns.add(iCol);

    PartitionColumn pCol = PartitionColumn.resolved(buildSelectCol("dir1"), 1, fileInfo);
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

  private NullColumn makeNullCol(String name) {
    return NullColumn.fromResolution(RequestedTableColumn.fromSelect(buildSelectCol(name)));
  }

  @SuppressWarnings("resource")
  @Test
  public void testNullColumnLoader() {

    List<NullColumn> defns = new ArrayList<>();
    defns.add(makeNullCol("req"));
    defns.add(makeNullCol("opt"));
    defns.add(makeNullCol("rep"));
    defns.add(makeNullCol("unk"));

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

  /**
   * Test SELECT * from an early-schema table of (a, b)
   */

  @Test
  public void testEarlySchemaWildcard() {

    // SELECT * ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectAll();
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(tableSchema);

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

  /**
   * Test SELECT a, b FROM table(a, b)
   */

  @Test
  public void testEarlySchemaSelectAll() {

    // SELECT a, b ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("a", "b"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(tableSchema);

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

  /**
   * Test SELECT b, a FROM table(a, b)
   */

  @Test
  public void testEarlySchemaSelectAllReorder() {

    // SELECT a, b ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("b", "a"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(tableSchema);

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

  /**
   * Test SELECT a, b, c FROM table(a, b)
   * c will be null
   */

  @Test
  public void testEarlySchemaSelectExtra() {

    // SELECT a, b, c ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("a", "b", "c"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(tableSchema);

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

  /**
   * Test SELECT a, b, c FROM table(a, b)
   * c will be null of type VARCHAR
   */

  @Test
  public void testEarlySchemaSelectExtraCustomType() {

    // SELECT a, b, c ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("a", "b", "c"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    // Null columns of type VARCHAR

    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();
    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, nullType);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(tableSchema);

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

  /**
   * Test SELECT a FROM table(a, b)
   */

  @Test
  public void testEarlySchemaSelectSubset() {

    // SELECT a ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("a"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

     ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(tableSchema);

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

  /**
   * Test SELECT a, b, dir0, suffix FROM table(a, b)
   * dir0, suffix are file metadata columns
   */

  @Test
  public void testEarlySchemaSelectAllAndMetadata() {

    // SELECT a, b, dir0, suffix ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("a", "b", "dir0", "suffix"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    // Null columns of type VARCHAR

    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();
    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, nullType);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(tableSchema);

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

  /**
   * Test SELECT c FROM table(a, b)
   * The result set will be one null column for each record, but
   * no file data.
   */

  @Test
  public void testEarlySchemaSelectNone() {

    // SELECT c ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("c"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

     ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(tableSchema);

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

  /**
   * Test SELECT dir0, b, suffix, c FROM table(a, b)
   * Full combination of metadata, table and null columns
   */

  @Test
  public void testMixture() {

    // SELECT dir0, b, suffix, c ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("dir0", "b", "suffix", "c"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

     ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(tableSchema);

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

    // SELECT a, b ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("a", "b"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

     ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // file schema (a, b)

    TupleMetadata twoColSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .buildSchema();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // ... FROM file a.csv

      projector.startFile(new Path("hdfs:///w/x/y/a.csv"));

      // Projection of (a, b) to (a, b)

      ResultSetLoader loader = projector.makeTableLoader(twoColSchema);

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
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/b.csv"));

      // File schema (a)

      TupleMetadata oneColSchema = new SchemaBuilder()
          .add("a", MinorType.INT)
          .buildSchema();

      // Projection of (a) to (a, b), reusing b from above.

      ResultSetLoader loader = projector.makeTableLoader(oneColSchema);

      loader.startBatch();
      loader.writer()
          .loadRow(30)
          .loadRow(40);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(twoColSchema)
          .add(30, null)
          .add(40, null)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // ... FROM file c.csv

      projector.startFile(new Path("hdfs:///w/x/y/c.csv"));

      // Projection of (a, b), to (a, b), reusing b yet again

      ResultSetLoader loader = projector.makeTableLoader(twoColSchema);

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

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("a", "b", "c"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    TupleMetadata schema1 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("c", MinorType.BIGINT)
        .buildSchema();
    TupleMetadata schema2 = new SchemaBuilder()
        .add("c", MinorType.BIGINT)
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .buildSchema();
    TupleMetadata schema3 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("c", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .buildSchema();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // ... FROM file a.csv

      projector.startFile(new Path("hdfs:///w/x/y/a.csv"));

      // Projection of (a, b, c) to (a, b, c)

      ResultSetLoader loader = projector.makeTableLoader(schema1);

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
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/b.csv"));

      // Projection of (c, a, b) to (a, b, c)

      ResultSetLoader loader = projector.makeTableLoader(schema2);

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
      // ... FROM file c.csv

      projector.startFile(new Path("hdfs:///w/x/y/c.csv"));

      // Projection of (a, c, b) to (a, b, c)

      ResultSetLoader loader = projector.makeTableLoader(schema3);

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
  public void testWildcardSmoothing() {

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectAll();
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    TupleMetadata firstSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .addNullable("c", MinorType.BIGINT)
        .buildSchema();
    TupleMetadata subsetSchema = new SchemaBuilder()
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("a", MinorType.INT)
        .buildSchema();
    TupleMetadata disjointSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("d", MinorType.VARCHAR)
        .buildSchema();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // First table, establishes the baseline
      // ... FROM file a.csv

      projector.startFile(new Path("hdfs:///w/x/y/a.csv"));

      ResultSetLoader loader = projector.makeTableLoader(firstSchema);

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
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/b.csv"));

      ResultSetLoader loader = projector.makeTableLoader(firstSchema);

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
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/c.csv"));

      ResultSetLoader loader = projector.makeTableLoader(subsetSchema);

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
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/c.csv"));

      ResultSetLoader loader = projector.makeTableLoader(disjointSchema);

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

  /**
   * Verify that metadata columns follow distinct files
   * <br>
   * SELECT dir0, filename, b FROM (a.csv, b.csv)
   */

  @Test
  public void testMetadataMulti() {

    // SELECT a, b ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("dir0", "filename", "b"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .buildSchema();
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("dir0", MinorType.VARCHAR)
        .addNullable("filename", MinorType.VARCHAR)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // ... FROM file a.csv

      projector.startFile(new Path("hdfs:///w/x/y/a.csv"));

      ResultSetLoader loader = projector.makeTableLoader(tableSchema);

      loader.startBatch();
      loader.writer()
          .loadRow(10, "fred")
          .loadRow(20, "wilma");
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .add("x", "a.csv", "fred")
          .add("x", "a.csv", "wilma")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/b.csv"));

      ResultSetLoader loader = projector.makeTableLoader(tableSchema);

      loader.startBatch();
      loader.writer()
          .loadRow(30, "bambam")
          .loadRow(40, "betty");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .add("x", "b.csv", "bambam")
          .add("x", "b.csv", "betty")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * Test SELECT * from an early-schema table of (a, b)
   */

  @Test
  public void testLateSchemaWildcard() {

    // SELECT * ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectAll();
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

    ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(null);

    // file schema (a, b)

    TupleSchema schema = loader.writer().schema();
    schema.addColumn(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    schema.addColumn(SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED));

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

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
        .add(1, "fred")
        .add(2, "wilma")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  /**
   * Test SELECT a, c FROM table(a, b)
   */

  @Test
  public void testLateSchemaSelectDisjoint() {

    // SELECT a, c ...

    ScanLevelProjection.Builder scanProjBuilder = new ScanLevelProjection.Builder(fixture.options());
    scanProjBuilder.useLegacyWildcardExpansion(false);
    scanProjBuilder.setScanRootDir("hdfs:///w");
    scanProjBuilder.projectedCols(TestScanLevelProjection.projectList("a", "c"));
    ScanLevelProjection querySelPlan = scanProjBuilder.build();

     ScanProjector projector = new ScanProjector(fixture.allocator(), querySelPlan, null);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(null);

    // file schema (a, b)

    TupleSchema schema = loader.writer().schema();
    schema.addColumn(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    schema.addColumn(SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED));

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
        .addNullable("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(1, null)
        .add(2, null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  // TODO: Test schema smoothing with repeated
  // TODO: Test hard schema change
  // TODO: Typed null column tests (resurrect)
}