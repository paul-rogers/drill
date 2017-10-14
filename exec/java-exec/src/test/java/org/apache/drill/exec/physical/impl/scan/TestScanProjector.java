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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.SchemaTracker;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadata;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumnDefn;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedMetadataColumn.ResolvedFileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedMetadataColumn.ResolvedPartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.NullColumn;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector.MetadataColumnLoader;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector.NullColumnLoader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.ColumnExplorer.ImplicitFileColumns;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestScanProjector extends SubOperatorTest {

  /**
   * Test the static column loader using one column of each type.
   * The null column is of type int, but the associated value is of
   * type string. This is a bit odd, but works out because we detect that
   * the string value is null and call setNull on the writer, and avoid
   * using the actual data.
   */

  @Test
  public void testStaticColumnLoader() {

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), new Path("hdfs:///w"));
    List<ResolvedMetadataColumn> defns = new ArrayList<>();
    FileMetadataColumnDefn iDefn = new FileMetadataColumnDefn(
        ScanTestUtils.SUFFIX_COL, ImplicitFileColumns.SUFFIX);
    ResolvedFileMetadataColumn iCol = new ResolvedFileMetadataColumn(ScanTestUtils.SUFFIX_COL,
        SchemaPath.getSimplePath(ScanTestUtils.SUFFIX_COL),
        iDefn, fileInfo);
    defns.add(iCol);

    String partColName = ScanTestUtils.partitionColName(1);
    ResolvedPartitionColumn pCol = new ResolvedPartitionColumn(partColName,
        SchemaPath.getSimplePath(partColName), 1, fileInfo);
    defns.add(pCol);

    ResultVectorCacheImpl cache = new ResultVectorCacheImpl(fixture.allocator());
    MetadataColumnLoader staticLoader = new MetadataColumnLoader(fixture.allocator(), defns, cache);

    // Create a batch

    staticLoader.load(2);

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable(ScanTestUtils.SUFFIX_COL, MinorType.VARCHAR)
        .addNullable(partColName, MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("csv", "y")
        .addRow("csv", "y")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(staticLoader.output()));
    staticLoader.close();
  }

  private NullColumn makeNullCol(String name, MajorType nullType) {
    return new NullColumn(name, nullType,
        SchemaPath.getSimplePath(name));
  }

  private NullColumn makeNullCol(String name) {
    return makeNullCol(name, null);
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

    ResultVectorCacheImpl cache = new ResultVectorCacheImpl(fixture.allocator());
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
        .addRow(null, null, new int[] { }, null)
        .addRow(null, null, new int[] { }, null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(output));
    staticLoader.close();
  }

  private ProjectionFixture buildFixture(String... cols) {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options())
        .projectedCols(cols);
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    return projFixture;
  }

  private ScanProjector buildProjector(String... cols) {
    ProjectionFixture projFixture = buildFixture(cols);
    return new ScanProjector(fixture.allocator(), projFixture.scanProj, projFixture.metadataProj, null);
  }

  private ScanProjector buildProjectorWithNulls(MajorType nullType, String... cols) {
    ProjectionFixture projFixture = buildFixture(cols);
    return new ScanProjector(fixture.allocator(), projFixture.scanProj, projFixture.metadataProj, nullType);
  }

  /**
   * Test SELECT * from an early-schema table of (a, b)
   */

  @Test
  public void testEarlySchemaWildcard() {

    // SELECT * ...

    ScanProjector projector = buildProjector(SchemaPath.WILDCARD);

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
    RowSetLoader writer = loader.writer();

    // Should be a direct writer, no projection
    for (int i = 0; i < 2; i++) {
      writer.start();
      writer.scalar(0).setInt((i+1));
      writer.scalar(1).setString(bValues[i]);
      writer.save();
    }
    projector.publish();

    // Verify

    SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
        .addRow(1, "fred")
        .addRow(2, "wilma")
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

    ScanProjector projector = buildProjector("a", "b");

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
    RowSetLoader writer = loader.writer();
    for (int i = 0; i < 2; i++) {
      writer.start();
      writer.scalar(0).setInt((i+1));
      writer.scalar(1).setString(bValues[i]);
      writer.save();
    }
    projector.publish();

    // Verify

    SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
        .addRow(1, "fred")
        .addRow(2, "wilma")
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

    // SELECT b, a ...

    ScanProjector projector = buildProjector("b", "a");

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
    RowSetLoader writer = loader.writer();
    for (int i = 0; i < 2; i++) {
      writer.start();
      writer.scalar(0).setInt((i+1));
      writer.scalar(1).setString(bValues[i]);
      writer.save();
    }
    projector.publish();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("b", MinorType.VARCHAR)
        .add("a", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("fred", 1)
        .addRow("wilma", 2)
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

    ScanProjector projector = buildProjector("a", "b", "c");

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
    RowSetLoader writer = loader.writer();
    for (int i = 0; i < 2; i++) {
      writer.start();
      writer.scalar(0).setInt((i+1));
      writer.scalar(1).setString(bValues[i]);
      writer.save();
    }
    projector.publish();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, "fred", null)
        .addRow(2, "wilma", null)
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

    // Null columns of type VARCHAR

    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();

    // SELECT a, b, c ...

    ScanProjector projector = buildProjectorWithNulls(nullType, "a", "b", "c");

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

    RowSetLoader writer = loader.writer();
    for (int i = 0; i < 2; i++) {
      writer.start();
      writer.scalar(0).setInt((i+1));
      writer.scalar(1).setString(bValues[i]);
      writer.save();
    }
    projector.publish();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addNullable("c", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, "fred", null)
        .addRow(2, "wilma", null)
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

    ScanProjector projector = buildProjector("a");

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
    RowSetLoader writer = loader.writer();
    for (int i = 0; i < 2; i++) {
      writer.start();
      assertTrue(writer.column(0).schema().isProjected());
      writer.scalar(0).setInt((i+1));
      assertFalse(writer.column(1).schema().isProjected());
      writer.save();
    }
    projector.publish();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1)
        .addRow(2)
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

    // Null columns of type VARCHAR

    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();

    // SELECT a, b, dir0, suffix ...

    ScanProjector projector = buildProjectorWithNulls(nullType, "a", "b", "dir0", "suffix");

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
    RowSetLoader writer = loader.writer();
    for (int i = 0; i < 2; i++) {
      writer.start();
      writer.scalar(0).setInt((i+1));
      writer.scalar(1).setString(bValues[i]);
      writer.save();
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
        .addRow(1, "fred", "x", "csv")
        .addRow(2, "wilma", "x", "csv")
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

    ScanProjector projector = buildProjector("c");

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
    RowSetLoader writer = loader.writer();
    for (int i = 0; i < 2; i++) {
      writer.start();
      assertFalse(writer.column(0).schema().isProjected());
      assertFalse(writer.column(1).schema().isProjected());
      writer.save();
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

    ScanProjector projector = buildProjector("dir0", "b", "suffix", "c");

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
    RowSetLoader writer = loader.writer();
    for (int i = 0; i < 2; i++) {
      writer.start();
      assertFalse(writer.column(0).schema().isProjected());
      assertTrue(writer.column(1).schema().isProjected());
      writer.scalar(1).setString(bValues[i]);
      writer.save();
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
        .addRow("x", "fred", "csv", null)
        .addRow("x", "wilma", "csv", null)
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

    ScanProjector projector = buildProjector("a", "b");

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
          .addRow(10, "fred")
          .addRow(20, "wilma");
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(twoColSchema)
          .addRow(10, "fred")
          .addRow(20, "wilma")
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
          .addRow(30)
          .addRow(40);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(twoColSchema)
          .addRow(30, null)
          .addRow(40, null)
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
          .addRow(50, "dino")
          .addRow(60, "barney");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(twoColSchema)
          .addRow(50, "dino")
          .addRow(60, "barney")
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

    ScanProjector projector = buildProjector("a", "b", "c");

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
          .addRow(10, "fred", 110L)
          .addRow(20, "wilma", 110L);
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(schema1)
          .addRow(10, "fred", 110L)
          .addRow(20, "wilma", 110L)
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
          .addRow(330L, 30, "bambam")
          .addRow(440L, 40, "betty");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(schema1)
          .addRow(30, "bambam", 330L)
          .addRow(40, "betty", 440L)
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
          .addRow(50, 550L, "dino")
          .addRow(60, 660L, "barney");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(schema1)
          .addRow(50, "dino", 550L)
          .addRow(60, "barney", 660L)
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

    ScanProjector projector = buildProjector(SchemaPath.WILDCARD);

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
          .addRow(10, "fred", 110L)
          .addRow(20, "wilma", 110L);
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(firstSchema)
          .addRow(10, "fred", 110L)
          .addRow(20, "wilma", 110L)
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
          .addRow(70, "pebbles", 770L)
          .addRow(80, "hoppy", 880L);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(firstSchema)
          .addRow(70, "pebbles", 770L)
          .addRow(80, "hoppy", 880L)
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
          .addRow("bambam", 30)
          .addRow("betty", 40);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(firstSchema)
          .addRow(30, "bambam", null)
          .addRow(40, "betty", null)
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
          .addRow(50, "dino", "supporting")
          .addRow(60, "barney", "main");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertNotEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(disjointSchema)
          .addRow(50, "dino", "supporting")
          .addRow(60, "barney", "main")
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

    // SELECT dir0, filename, b ...

    ScanProjector projector = buildProjector(
        ScanTestUtils.partitionColName(0),
        ScanTestUtils.FILE_NAME_COL, "b");

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
          .addRow(10, "fred")
          .addRow(20, "wilma");
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .addRow("x", "a.csv", "fred")
          .addRow("x", "a.csv", "wilma")
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
          .addRow(30, "bambam")
          .addRow(40, "betty");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .addRow("x", "b.csv", "bambam")
          .addRow("x", "b.csv", "betty")
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

    ScanProjector projector = buildProjector(SchemaPath.WILDCARD);

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(null);

    // file schema (a, b)

    RowSetLoader rowSet = loader.writer();
    rowSet.addColumn(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    rowSet.addColumn(SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED));

    // Create a batch of data.

    String bValues[] = new String[] { "fred", "wilma" };
    loader.startBatch();
    RowSetLoader writer = loader.writer();

    // Should be a direct writer, no projection
    for (int i = 0; i < 2; i++) {
      writer.start();
      writer.scalar(0).setInt((i+1));
      writer.scalar(1).setString(bValues[i]);
      writer.save();
    }
    projector.publish();

    // Verify

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
        .addRow(1, "fred")
        .addRow(2, "wilma")
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

    ScanProjector projector = buildProjector("a", "c");

    // ... FROM file

    projector.startFile(new Path("hdfs:///w/x/y/z.csv"));

    // Create the table loader

    ResultSetLoader loader = projector.makeTableLoader(null);

    // file schema (a, b)

    RowSetLoader rowSet = loader.writer();
    rowSet.addColumn(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
    rowSet.addColumn(SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED));

    // Create a batch of data.

    loader.startBatch();

    // Should be a projection
    RowSetLoader writer = loader.writer();
    for (int i = 0; i < 2; i++) {
      writer.start();
      assertTrue(writer.column(0).schema().isProjected());
      writer.scalar(0).setInt((i+1));
      assertFalse(writer.column(1).schema().isProjected());
      writer.save();
    }
    projector.publish();

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("c", MinorType.INT)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, null)
        .addRow(2, null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));

    projector.close();
  }

  // TODO: Test schema smoothing with repeated
  // TODO: Test hard schema change
  // TODO: Typed null column tests (resurrect)
  // TODO: Test maps and arrays of maps
}
