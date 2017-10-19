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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.SchemaTracker;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator.ReaderSchemaOrchestrator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Test the early-schema support of the scan orchestrator. "Early schema"
 * refers to the case in which the reader can provide a schema when the
 * reader is opened. Examples: CSV, HBase, MapR-DB binary, JDBC.
 * <p>
 * The tests here focus on the scan orchestrator itself; the tests assume
 * that tests for lower-level components have already passed.
 */

public class TestScanOrchestratorEarlySchema extends SubOperatorTest {

  /**
   * Test SELECT * from an early-schema table of (a, b)
   */

  @Test
  public void testEarlySchemaWildcard() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT * ...

    scanner.build(ScanTestUtils.projectAll());

    // ... FROM table

    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

    // Schema provided, so an empty batch is available to
    // send downstream.

    {
      SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
          .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(1, "fred")
      .addRow(2, "wilma");

    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
          .addRow(1, "fred")
          .addRow(2, "wilma")
          .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Second batch.

    reader.startBatch();
    loader.writer()
      .addRow(3, "barney")
      .addRow(4, "betty");

    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
          .addRow(3, "barney")
          .addRow(4, "betty")
          .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Explicit reader close. (All other tests are lazy, they
    // use an implicit close.)

    scanner.closeReader();

    scanner.close();
  }

  /**
   * Test SELECT a, b FROM table(a, b)
   */

  @Test
  public void testEarlySchemaSelectAll() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT a, b ...

    scanner.build(ScanTestUtils.projectList("a", "b"));

    // ... FROM table

    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

    // Verify empty batch.

    {
      SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
          .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(1, "fred")
      .addRow(2, "wilma");
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
          .addRow(1, "fred")
          .addRow(2, "wilma")
          .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
  }

  /**
   * Test SELECT b, a FROM table(a, b)
   */

  @Test
  public void testEarlySchemaSelectAllReorder() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT b, a ...

    scanner.build(ScanTestUtils.projectList("b", "a"));

    // ... FROM table

    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

    // Verify empty batch.

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("b", MinorType.VARCHAR)
        .add("a", MinorType.INT)
        .build();
   {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
   }

    // Create a batch of data.

   reader.startBatch();
   loader.writer()
     .addRow(1, "fred")
     .addRow(2, "wilma");
   reader.endBatch();

    // Verify

   {
     SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("fred", 1)
        .addRow("wilma", 2)
        .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
   }

    scanner.close();
  }

  /**
   * Test SELECT a, b, c FROM table(a, b)
   * c will be null
   */

  @Test
  public void testEarlySchemaSelectExtra() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT a, b, c ...

    scanner.build(ScanTestUtils.projectList("a", "b", "c"));

    // ... FROM table

    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

    // Verify empty batch

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .build();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
   }

   // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(1, "fred")
      .addRow(2, "wilma");
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, "fred", null)
        .addRow(2, "wilma", null)
        .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
  }

  /**
   * Test SELECT a, b, c FROM table(a, b)
   * c will be null of type VARCHAR
   */

  @Test
  public void testEarlySchemaSelectExtraCustomType() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());

    // Null columns of type VARCHAR

    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();
    scanner.setNullType(nullType);

    // SELECT a, b, c ...

    scanner.build(ScanTestUtils.projectList("a", "b", "c"));

    // ... FROM tab;e

    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

    // Verify empty batch

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addNullable("c", MinorType.VARCHAR)
        .build();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
   }

    // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(1, "fred")
      .addRow(2, "wilma");
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, "fred", null)
        .addRow(2, "wilma", null)
        .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
  }

  /**
   * Test SELECT a FROM table(a, b)
   */

  @Test
  public void testEarlySchemaSelectSubset() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT a ...

    scanner.build(ScanTestUtils.projectList("a"));

    // ... FROM table

    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

    // Verify that unprojected column is unprojected in the
    // table loader.

    assertFalse(loader.writer().column("b").schema().isProjected());

    // Verify empty batch.

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(1, "fred")
      .addRow(2, "wilma");
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1)
        .addRow(2)
        .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
  }


  /**
   * Test SELECT * from an early-schema table of () (that is,
   * a schema that consists of zero columns.
   */

  @Test
  public void testEmptySchema() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT * ...

    scanner.build(ScanTestUtils.projectAll());

    // ... FROM table

    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema ()

    TupleMetadata tableSchema = new SchemaBuilder()
        .buildSchema();

    // Create the table loader

    reader.makeResultSetLoader(tableSchema);

    // Schema provided, so an empty batch is available to
    // send downstream.

    {
      SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
          .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Create a batch of data. Because there are no columns, it does
    // not make sense to ready any rows.

    reader.startBatch();
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(tableSchema)
          .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
  }


  /**
   * Test SELECT a from an early-schema table of () (that is,
   * a schema that consists of zero columns.
   */

  @Test
  public void testEmptySchemaExtra() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT * ...

    scanner.build(ScanTestUtils.projectList("a"));

    // ... FROM table

    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema ()

    TupleMetadata tableSchema = new SchemaBuilder()
        .buildSchema();

    // Create the table loader

    reader.makeResultSetLoader(tableSchema);

    // Verify initial empty batch.

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.INT)
        .build();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Create a batch of data. Because there are no columns, it does
    // not make sense to ready any rows.

    reader.startBatch();
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
  }

  /**
   * Test SELECT c FROM table(a, b)
   * The result set will be one null column for each record, but
   * no file data.
   */

  @Test
  public void testSelectNone() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));
    scanner.withMetadata(metadataManager);

    // SELECT c ...

    scanner.build(ScanTestUtils.projectList("c"));

    // ... FROM file

    metadataManager.startFile(filePath);
    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

    // Verify empty batch.

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("c", MinorType.INT)
        .build();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
         .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
         .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(1, "fred")
      .addRow(2, "wilma");
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
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

    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());
    scanner.setNullType(nullType);
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));
    scanner.withMetadata(metadataManager);

    // SELECT a, b, dir0, suffix ...

    scanner.build(ScanTestUtils.projectList("a", "b", "dir0", "suffix"));

    // ... FROM file

    metadataManager.startFile(filePath);
    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

    // Verify empty batch.

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .add("suffix", MinorType.VARCHAR)
        .build();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
         .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
         .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(1, "fred")
      .addRow(2, "wilma");
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, "fred", "x", "csv")
        .addRow(2, "wilma", "x", "csv")
        .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
  }

  /**
   * Test SELECT dir0, b, suffix, c FROM table(a, b)
   * Full combination of metadata, table and null columns
   */

  @Test
  public void testMixture() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));
    scanner.withMetadata(metadataManager);

    // SELECT dir0, b, suffix, c ...

    scanner.build(ScanTestUtils.projectList("dir0", "b", "suffix", "c"));

    // ... FROM file

    metadataManager.startFile(filePath);
    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    // Create the table loader

    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

    // Verify empty batch.

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("dir0", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("suffix", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .build();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
         .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
         .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(1, "fred")
      .addRow(2, "wilma");
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("x", "fred", "csv", null)
        .addRow("x", "wilma", "csv", null)
        .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
  }

  /**
   * Verify that metadata columns follow distinct files
   * <br>
   * SELECT dir0, filename, b FROM (a.csv, b.csv)
   */

  @Test
  public void testMetadataMulti() {
    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());
    Path filePathA = new Path("hdfs:///w/x/y/a.csv");
    Path filePathB = new Path("hdfs:///w/x/b.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePathA, filePathB));
    scanner.withMetadata(metadataManager);

    // SELECT dir0, dir1, filename, b ...

    scanner.build(ScanTestUtils.projectList(
        ScanTestUtils.partitionColName(0),
        ScanTestUtils.partitionColName(1),
        ScanTestUtils.FILE_NAME_COL,
        "b"));

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .buildSchema();
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable(ScanTestUtils.partitionColName(0), MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(1), MinorType.VARCHAR)
        .add(ScanTestUtils.FILE_NAME_COL, MinorType.VARCHAR)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // ... FROM file a.csv

      metadataManager.startFile(filePathA);

      ReaderSchemaOrchestrator reader = scanner.startReader();
      ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

      reader.startBatch();
      loader.writer()
        .addRow(10, "fred")
        .addRow(20, "wilma");
      reader.endBatch();

      tracker.trackSchema(scanner.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .addRow("x", "y", "a.csv", "fred")
          .addRow("x", "y", "a.csv", "wilma")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanner.output()));

      // Do explicit close (as in real code) to avoid an implicit
      // close which will blow away the current file info...

      scanner.closeReader();
    }
    {
      // ... FROM file b.csv

      metadataManager.startFile(filePathB);
      ReaderSchemaOrchestrator reader = scanner.startReader();
      ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);

      reader.startBatch();
      loader.writer()
          .addRow(30, "bambam")
          .addRow(40, "betty");
      reader.endBatch();

      tracker.trackSchema(scanner.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
          .addRow("x", null, "b.csv", "bambam")
          .addRow("x", null, "b.csv", "betty")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanner.output()));

      scanner.closeReader();
    }

    scanner.close();
  }

  /**
   * Resolve a selection list using SELECT *.
   */

  @Test
  public void testWildcardWithMetadata() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());
    scanner.withMetadata(metadataManager);

    // SELECT * ...

    scanner.build(ScanTestUtils.projectAll());

    // ... FROM file

    metadataManager.startFile(filePath);
    ReaderSchemaOrchestrator reader = scanner.startReader();

    // file schema (a, b)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();
    ResultSetLoader loader = reader.makeResultSetLoader(tableSchema);
    reader.container().zeroVectors();

    // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(1, "fred")
      .addRow(2, "wilma");
    reader.endBatch();

    // Verify

    TupleMetadata expectedSchema = ScanTestUtils.expandMetadata(tableSchema, metadataManager, 2);

    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1, "fred", "/w/x/y/z.csv", "/w/x/y", "z.csv", "csv", "x", "y")
        .addRow(2, "wilma", "/w/x/y/z.csv", "/w/x/y", "z.csv", "csv", "x", "y")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scanner.output()));

    scanner.close();
  }

  // TODO: Start with early schema, but add columns

}
