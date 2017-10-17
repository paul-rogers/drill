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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
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

public class TestScanOrchestrator extends SubOperatorTest {

  /**
   * Test SELECT * from an early-schema table of (a, b)
   */

  @Test
  public void testEarlySchemaWildcard() {
    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT * ...

    projector.build(ScanTestUtils.projectAll());

    // ... FROM table

    ReaderSchemaOrchestrator reader = projector.startReader();

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

      assertNotNull(projector.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * Test SELECT a, b FROM table(a, b)
   */

  @Test
  public void testEarlySchemaSelectAll() {
    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT a, b ...

    projector.build(ScanTestUtils.projectList("a", "b"));

    // ... FROM table

    ReaderSchemaOrchestrator reader = projector.startReader();

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

      assertNotNull(projector.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * Test SELECT b, a FROM table(a, b)
   */

  @Test
  public void testEarlySchemaSelectAllReorder() {
    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT b, a ...

    projector.build(ScanTestUtils.projectList("b", "a"));

    // ... FROM table

    ReaderSchemaOrchestrator reader = projector.startReader();

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

      assertNotNull(projector.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
   }

    projector.close();
  }

  /**
   * Test SELECT a, b, c FROM table(a, b)
   * c will be null
   */

  @Test
  public void testEarlySchemaSelectExtra() {
    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT a, b, c ...

    projector.build(ScanTestUtils.projectList("a", "b", "c"));

    // ... FROM table

    ReaderSchemaOrchestrator reader = projector.startReader();

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

      assertNotNull(projector.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * Test SELECT a, b, c FROM table(a, b)
   * c will be null of type VARCHAR
   */

  @Test
  public void testEarlySchemaSelectExtraCustomType() {
    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());

    // Null columns of type VARCHAR

    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();
    projector.setNullType(nullType);

    // SELECT a, b, c ...

    projector.build(ScanTestUtils.projectList("a", "b", "c"));

    // ... FROM tab;e

    ReaderSchemaOrchestrator reader = projector.startReader();

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

      assertNotNull(projector.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * Test SELECT a FROM table(a, b)
   */

  @Test
  public void testEarlySchemaSelectSubset() {
    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());

    // SELECT a ...

    projector.build(ScanTestUtils.projectList("a"));

    // ... FROM table

    ReaderSchemaOrchestrator reader = projector.startReader();

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

      assertNotNull(projector.output());
      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
    }

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

    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());
    projector.setNullType(nullType);
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), false, new Path("hdfs:///w"));
    projector.withMetadata(metadataManager);

    // SELECT a, b, dir0, suffix ...

    projector.build(ScanTestUtils.projectList("a", "b", "dir0", "suffix"));

    // ... FROM file

    metadataManager.startFile(new Path("hdfs:///w/x/y/z.csv"));
    ReaderSchemaOrchestrator reader = projector.startReader();

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

      assertNotNull(projector.output());
      new RowSetComparison(expected)
         .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * Test SELECT c FROM table(a, b)
   * The result set will be one null column for each record, but
   * no file data.
   */

  @Test
  public void testEarlySchemaSelectNone() {
    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), false, new Path("hdfs:///w"));
    projector.withMetadata(metadataManager);

    // SELECT c ...

    projector.build(ScanTestUtils.projectList("c"));

    // ... FROM file

    metadataManager.startFile(new Path("hdfs:///w/x/y/z.csv"));
    ReaderSchemaOrchestrator reader = projector.startReader();

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

      assertNotNull(projector.output());
      new RowSetComparison(expected)
         .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }


  /**
   * Test SELECT dir0, b, suffix, c FROM table(a, b)
   * Full combination of metadata, table and null columns
   */

  @Test
  public void testMixture() {
    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.options(), false, new Path("hdfs:///w"));
    projector.withMetadata(metadataManager);

    // SELECT dir0, b, suffix, c ...

    projector.build(ScanTestUtils.projectList("dir0", "b", "suffix", "c"));

    // ... FROM file

    metadataManager.startFile(new Path("hdfs:///w/x/y/z.csv"));
    ReaderSchemaOrchestrator reader = projector.startReader();

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

      assertNotNull(projector.output());
      new RowSetComparison(expected)
         .verifyAndClearAll(fixture.wrap(projector.output()));
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
          .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }
}
