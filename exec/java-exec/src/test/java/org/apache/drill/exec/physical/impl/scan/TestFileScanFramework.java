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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.TestScanOperatorExec.BaseMockBatchReader;
import org.apache.drill.exec.physical.impl.scan.TestScanOperatorExec.LegacyManagerBuilder;
import org.apache.drill.exec.physical.impl.scan.TestScanOperatorExec.MockBatch;
import org.apache.drill.exec.physical.impl.scan.TestScanOperatorExec.MockEarlySchemaReader;
import org.apache.drill.exec.physical.impl.scan.TestScanOperatorExec.MockLateSchemaReader;
import org.apache.drill.exec.physical.impl.scan.managed.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.managed.SchemaNegotiator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFileScanFramework {

  private static final String MOCK_FILE_NAME = "foo.csv";
  private static final String MOCK_FILE_PATH = "/w/x/y";
  private static final String MOCK_FILE_FQN = MOCK_FILE_PATH + "/" + MOCK_FILE_NAME;
  private static final String MOCK_FILE_SYSTEM_NAME = "hdfs:" + MOCK_FILE_FQN;
  private static final String MOCK_ROOT_PATH = "hdfs:///w";
  private static final String MOCK_SUFFIX = "csv";
  private static final String MOCK_DIR0 = "x";
  private static final String MOCK_DIR1 = "y";

  public static class LegacyManagerBuilder {

    public BasicScanConfig scanConfig = new BasicScanConfig();

    public void setSelectionRoot(String rootPath) {
      setSelectionRoot(new Path(rootPath));
    }
  }



  /**
   * Base class for the "mock" readers used in this test. The mock readers
   * follow the normal (enhanced) reader API, but instead of actually reading
   * from a data source, they just generate data with a known schema.
   * They also expose internal state such as identifying which methods
   * were actually called.
   */

  private static abstract class BaseMockBatchReader implements ManagedReader {
    public boolean openCalled;
    public boolean closeCalled;
    public int startIndex;
    public int batchCount;
    public int batchLimit;
    protected ResultSetLoader tableLoader;
    protected Path filePath = new Path(MOCK_FILE_SYSTEM_NAME);

    public BaseMockBatchReader setFilePath(String filePath) {
      this.filePath = new Path(filePath);
      return this;
    }

    protected void buildFilePath(SchemaNegotiator schemaNegotiator) {
      if (filePath != null) {
        schemaNegotiator.setFilePath(filePath);
      }
    }

    protected void makeBatch() {
      RowSetLoader writer = tableLoader.writer();
      int offset = (batchCount - 1) * 20 + startIndex;
      writeRow(writer, offset + 10, "fred");
      writeRow(writer, offset + 20, "wilma");
    }

    protected void writeRow(RowSetLoader writer, int col1, String col2) {
      writer.start();
      if (writer.column(0) != null) {
        writer.scalar(0).setInt(col1);
      }
      if (writer.column(1) != null) {
        writer.scalar(1).setString(col2);
      }
      writer.save();
    }

    @Override
    public void close() {
      closeCalled = true;
    }
  }

  /**
   * "Late schema" reader, meaning that the reader does not know the schema on
   * open, but must "discover" it when reading data.
   */

  private static class MockLateSchemaReader extends BaseMockBatchReader {

    public boolean returnDataOnFirst;

    @Override
    public boolean open(SchemaNegotiator schemaNegotiator) {

      // No schema or file, just build the table loader.

      buildFilePath(schemaNegotiator);
      tableLoader = schemaNegotiator.build();
      openCalled = true;
      return true;
    }

    @Override
    public boolean next() {
      batchCount++;
      if (batchCount > batchLimit) {
        return false;
      } else if (batchCount == 1) {

        // On first batch, pretend to discover the schema.

        RowSetLoader rowSet = tableLoader.writer();
        MaterializedField a = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
        rowSet.addColumn(a);
        MaterializedField b = new SchemaBuilder.ColumnBuilder("b", MinorType.VARCHAR)
            .setMode(DataMode.OPTIONAL)
            .setWidth(10)
            .build();
        rowSet.addColumn(b);
        if ( ! returnDataOnFirst) {
          return true;
        }
      }

      makeBatch();
      return true;
    }
  }

  private static class MockNullEarlySchemaReader extends BaseMockBatchReader {

    @Override
    public boolean open(SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      return false;
    }

    @Override
    public boolean next() {
      return false;
    }
  }

  private static class MockEarlySchemaReader extends BaseMockBatchReader {

    @Override
    public boolean open(SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      buildFilePath(schemaNegotiator);
      TupleMetadata schema = new SchemaBuilder()
          .add("a", MinorType.INT)
          .addNullable("b", MinorType.VARCHAR, 10)
          .buildSchema();
      schemaNegotiator.setTableSchema(schema);
      tableLoader = schemaNegotiator.build();
      return true;
    }

    @Override
    public boolean next() {
      batchCount++;
      if (batchCount > batchLimit) {
        return false;
      }

      makeBatch();
      return true;
    }
  }


  @Test
  public void testLateSchemaFileWildcards() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 2;
    reader.returnDataOnFirst = false;

    // Create the scan operator

    LegacyManagerBuilder builder = new LegacyManagerBuilder();
    builder.projectAll();
    builder.setSelectionRoot(MOCK_ROOT_PATH);
    builder.addReader(reader);
    MockBatch mockBatch = new MockBatch(builder);
    ScanOperatorExec scan = mockBatch.scanOp;

    // Standard startup

    assertFalse(reader.openCalled);

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    assertTrue(reader.openCalled);
    assertEquals(1, reader.batchCount);
    assertEquals(0, scan.batchAccessor().getRowCount());

    // Create the expected result.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add(ScanTestUtils.FULLY_QUALIFIED_NAME_COL, MinorType.VARCHAR)
        .add(ScanTestUtils.FILE_PATH_COL, MinorType.VARCHAR)
        .add(ScanTestUtils.FILE_NAME_COL, MinorType.VARCHAR)
        .add(ScanTestUtils.SUFFIX_COL, MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(0), MinorType.VARCHAR)
        .addNullable(ScanTestUtils.partitionColName(1), MinorType.VARCHAR)
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(30, "fred", MOCK_FILE_FQN, MOCK_FILE_PATH, MOCK_FILE_NAME, MOCK_SUFFIX, MOCK_DIR0, MOCK_DIR1)
        .addRow(40, "wilma", MOCK_FILE_FQN, MOCK_FILE_PATH, MOCK_FILE_NAME, MOCK_SUFFIX, MOCK_DIR0, MOCK_DIR1)
        .build();
    RowSetComparison verifier = new RowSetComparison(expected);
    assertEquals(expected.batchSchema(), scan.batchAccessor().getSchema());

    // Next call, return with data.

    assertTrue(scan.next());
    verifier.verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    assertTrue(reader.closeCalled);
    assertEquals(0, scan.batchAccessor().getRowCount());

    mockBatch.close();
  }


  /**
   * Basic sanity test of a couple of implicit columns, along
   * with all table columns in table order. Full testing of implicit
   * columns is done on lower-level components.
   */

  @Test
  public void testMetadataColumns() {

    MockEarlySchemaReader reader = new MockEarlySchemaReader();
    reader.batchLimit = 1;

    // Select table and implicit columns.

    LegacyManagerBuilder builder = new LegacyManagerBuilder();
    builder.setProjection(new String[] {"a", "b", "filename", "suffix"});
    builder.addReader(reader);
    MockBatch mockBatch = new MockBatch(builder);
    ScanOperatorExec scan = mockBatch.scanOp;

    // Expect data and implicit columns

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("filename", MinorType.VARCHAR)
        .add("suffix", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(10, "fred", MOCK_FILE_NAME, MOCK_SUFFIX)
        .addRow(20, "wilma", MOCK_FILE_NAME, MOCK_SUFFIX)
        .build();
    RowSetComparison verifier = new RowSetComparison(expected);

    // Schema should include implicit columns.

    assertTrue(scan.buildSchema());
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    scan.batchAccessor().release();

    // Read one batch, should contain implicit columns

    assertTrue(scan.next());
    verifier.verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    assertEquals(0, scan.batchAccessor().getRowCount());
    mockBatch.close();
  }

  /**
   * Exercise the major project operations: subset of table
   * columns, implicit, partition, missing columns, and output
   * order (and positions) different than table. These cases
   * are more fully test on lower level components; here we verify
   * that the components are wired up correctly.
   */
  @Test
  public void testFullProject() {

    MockEarlySchemaReader reader = new MockEarlySchemaReader();
    reader.batchLimit = 1;

    // Select table and implicit columns.

    LegacyManagerBuilder builder = new LegacyManagerBuilder();
    builder.setSelectionRoot(MOCK_ROOT_PATH);
    builder.setProjection(new String[] {"dir0", "b", "filename", "c", "suffix"});
    builder.addReader(reader);
    MockBatch mockBatch = new MockBatch(builder);
    ScanOperatorExec scan = mockBatch.scanOp;

    // Expect data and implicit columns

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("dir0", MinorType.VARCHAR)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("filename", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .add("suffix", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(MOCK_DIR0, "fred", MOCK_FILE_NAME, null, MOCK_SUFFIX)
        .addRow(MOCK_DIR0, "wilma", MOCK_FILE_NAME, null, MOCK_SUFFIX)
        .build();
    RowSetComparison verifier = new RowSetComparison(expected);

    // Schema should include implicit columns.

    assertTrue(scan.buildSchema());
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    scan.batchAccessor().release();

    // Read one batch, should contain implicit columns

    assertTrue(scan.next());
    verifier.verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    assertEquals(0, scan.batchAccessor().getRowCount());
    mockBatch.close();
  }

}
