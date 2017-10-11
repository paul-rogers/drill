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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.impl.scan.managed.LegacyReaderFactory;
import org.apache.drill.exec.physical.impl.scan.managed.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.managed.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.managed.SchemaNegotiatorImpl;
import org.apache.drill.exec.physical.impl.scan.managed.LegacyReaderFactory.LegacyManagerBuilder;
import org.apache.drill.exec.physical.impl.scan.metadata.FileMetadataColumnsParser;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test of the scan operator framework. Here the focus is on the
 * implementation of the scan operator itself. This operator is
 * based on a number of lower-level abstractions, each of which has
 * its own unit tests. To make this more concrete: review the scan
 * operator code paths. Each path should be exercised by one or more
 * of the tests here. If, however, the code path depends on the
 * details of another, supporting class, then tests for that class
 * appear elsewhere.
 */

public class TestScanOperatorExec extends SubOperatorTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestScanOperatorExec.class);

  private static final String MOCK_FILE_NAME = "foo.csv";
  private static final String MOCK_FILE_PATH = "/w/x/y";
  private static final String MOCK_FILE_FQN = MOCK_FILE_PATH + "/" + MOCK_FILE_NAME;
  private static final String MOCK_FILE_SYSTEM_NAME = "hdfs:" + MOCK_FILE_FQN;
  private static final String MOCK_ROOT_PATH = "hdfs:///w";
  private static final String MOCK_SUFFIX = "csv";
  private static final String MOCK_DIR0 = "x";
  private static final String MOCK_DIR1 = "y";
  private static final String STAR = SchemaPath.WILDCARD;
  private static final String[] SELECT_STAR = new String[] { STAR };

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

  private static class MockEarlySchemaReader2 extends MockEarlySchemaReader {

    @Override
    public boolean open(SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      buildFilePath(schemaNegotiator);
      TupleMetadata schema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .addNullable("b", MinorType.VARCHAR, 10)
          .buildSchema();
      schemaNegotiator.setTableSchema(schema);
      schemaNegotiator.build();
      tableLoader = schemaNegotiator.build();
      return true;
    }

    @Override
    protected void writeRow(RowSetLoader writer, int col1, String col2) {
      writer.start();
      if (writer.column(0) != null) {
        writer.scalar(0).setString(Integer.toString(col1));
      }
      if (writer.column(1) != null) {
        writer.scalar(1).setString(col2);
      }
      writer.save();
    }
  }

  private SingleRowSet makeExpected() {
    return makeExpected(0);
  }

  private SingleRowSet makeExpected(int offset) {
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(offset + 10, "fred")
        .addRow(offset + 20, "wilma")
        .build();
    return expected;
  }

  private void verifyBatch(int offset, VectorContainer output) {
    SingleRowSet expected = makeExpected(offset);
    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(output));
  }

  private static class MockBatch {

    private OperatorContext services;
    public ScanOperatorExec scanOp;

    public MockBatch(LegacyReaderFactory.LegacyManagerBuilder builder) {
      scanOp = new ScanOperatorExec(builder.build());
      Scan scanConfig = new AbstractSubScan("bob") {

        @Override
        public int getOperatorType() {
          return 0;
        } };
      services = fixture.operatorContext(scanConfig);
      scanOp.bind(services);
    }

    public void close() {
      try {
        scanOp.close();
      } finally {
        services.close();
      }
    }
  }

  @Test
  public void testLateSchemaLifecycle() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 2;
    reader.returnDataOnFirst = false;

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(new LegacyReaderFactory.LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .useLegacyWildcardExpansion(false)
        .addReader(reader)
         );
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

    SingleRowSet expected = makeExpected(20);
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
   * Test a late-schema source that has no file information.
   * (Like a Hive or JDBC data source.)
   */

  @Test
  public void testLateSchemaLifecycleNoFile() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 2;
    reader.returnDataOnFirst = false;
    reader.filePath = null;

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .useLegacyWildcardExpansion(false)
        .addReader(reader)
         );
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

    SingleRowSet expected = makeExpected(20);
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

  @Test
  public void testLateSchemaNoData() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 0;
    reader.returnDataOnFirst = false;

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .useLegacyWildcardExpansion(false)
        .addReader(reader)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    // Standard startup

    assertFalse(reader.openCalled);

    // First batch: EOF.

    assertFalse(scan.buildSchema());
    assertTrue(reader.openCalled);
    assertTrue(reader.closeCalled);
    mockBatch.close();
  }

  @Test
  public void testLateSchemaDataOnFirst() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 1;
    reader.returnDataOnFirst = true;

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .useLegacyWildcardExpansion(false)
        .addReader(reader)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    // Standard startup

    assertFalse(reader.openCalled);

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    assertTrue(reader.openCalled);
    assertEquals(1, reader.batchCount);
    assertEquals(0, scan.batchAccessor().getRowCount());

    SingleRowSet expected = makeExpected();
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

  @Test
  public void testLateSchemaFileWildcards() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 2;
    reader.returnDataOnFirst = false;

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .setSelectionRoot(MOCK_ROOT_PATH)
        .addReader(reader)
         );
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
        .add(FileMetadataColumnsParser.FULLY_QUALIFIED_NAME_COL, MinorType.VARCHAR)
        .add(FileMetadataColumnsParser.FILE_PATH_COL, MinorType.VARCHAR)
        .add(FileMetadataColumnsParser.FILE_NAME_COL, MinorType.VARCHAR)
        .add(FileMetadataColumnsParser.SUFFIX_COL, MinorType.VARCHAR)
        .addNullable(FileMetadataColumnsParser.partitionColName(0), MinorType.VARCHAR)
        .addNullable(FileMetadataColumnsParser.partitionColName(1), MinorType.VARCHAR)
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

  @Test
  public void testEarlySchemaLifecycle() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockEarlySchemaReader reader = new MockEarlySchemaReader();
    reader.batchLimit = 1;

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .projectAll()
        .useLegacyWildcardExpansion(false)
        .addReader(reader)
        );
    ScanOperatorExec scan = mockBatch.scanOp;

    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    // First batch: return schema.

    assertTrue(scan.buildSchema());
    assertEquals(0, reader.batchCount);
    assertEquals(expected.batchSchema(), scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());

    // Next call, return with data.

    assertTrue(scan.next());
    verifier.verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    assertEquals(0, scan.batchAccessor().getRowCount());

    // Next again: no-op

    assertFalse(scan.next());
    mockBatch.close();

    // Close again: no-op

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

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(new String[] {"a", "b", "filename", "suffix"})
        .addReader(reader)
         );
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
  public void testFullProject() { // Stopped here

    MockEarlySchemaReader reader = new MockEarlySchemaReader();
    reader.batchLimit = 1;

    // Select table and implicit columns.

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setSelectionRoot(MOCK_ROOT_PATH)
        .setProjection(new String[] {"dir0", "b", "filename", "c", "suffix"})
        .addReader(reader)
         );
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

  /**
   * Test the case where the reader does not play the "first batch contains
   * only schema" game, and instead returns data. The Scan operator will
   * split the first batch into two: one with schema only, another with
   * data.
   */

  @Test
  // TODO
  @Ignore("Needs late schema - not yet")
  public void testNonEmptyFirstBatch() {
    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 2;
    reader.returnDataOnFirst = true;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader)
        );
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch. The reader returns a non-empty batch. The scan
    // operator strips off the schema and returns just that.

    assertTrue(scan.buildSchema());
    assertEquals(1, reader.batchCount);
    assertEquals(expected.batchSchema(), scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());
    scan.batchAccessor().release();

    // Second batch. Returns the "look-ahead" batch returned by
    // the reader earlier.

    assertTrue(scan.next());
    assertEquals(1, reader.batchCount);
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Third batch, normal case.

    assertTrue(scan.next());
    assertEquals(2, reader.batchCount);
    verifier.verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    assertTrue(reader.closeCalled);
    assertEquals(0, scan.batchAccessor().getRowCount());

    mockBatch.close();
  }

  /**
   * Test EOF on the first batch. Is allowed, but will result in the scan operator
   * passing a null batch to the parent.
   */

  @Test
  public void testEOFOnSchema() {
    MockNullEarlySchemaReader reader = new MockNullEarlySchemaReader();

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    // EOF

    assertFalse(scan.buildSchema());
    assertTrue(reader.closeCalled);
    assertEquals(0, scan.batchAccessor().getRowCount());

    mockBatch.close();
  }

  @Test
  public void testEOFOnFirstBatch() {
    MockEarlySchemaReader reader = new MockEarlySchemaReader();
    reader.batchLimit = 0;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader)
         );
    ScanOperatorExec scan = mockBatch.scanOp;
    assertTrue(scan.buildSchema());

    // EOF

    assertFalse(scan.next());
    assertTrue(reader.closeCalled);
    assertEquals(0, scan.batchAccessor().getRowCount());

    mockBatch.close();
  }

  /**
   * Test normal case with multiple readers. These return
   * the same schema, so no schema change.
   */

  @Test
  public void testMultipleReaders() {
    MockNullEarlySchemaReader nullReader = new MockNullEarlySchemaReader();

    MockEarlySchemaReader reader1 = new MockEarlySchemaReader();
    reader1.batchLimit = 2;

    MockEarlySchemaReader reader2 = new MockEarlySchemaReader();
    reader2.batchLimit = 2;
    reader2.startIndex = 100;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(nullReader)
        .addReader(reader1)
        .addReader(reader2)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch, schema only.

    assertTrue(scan.buildSchema());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // Second batch.

    assertTrue(scan.next());
    assertEquals(1, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifyBatch(0, scan.batchAccessor().getOutgoingContainer());

    // Third batch.

    assertTrue(scan.next());
    assertEquals(2, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifyBatch(20, scan.batchAccessor().getOutgoingContainer());

    // Second reader. First batch includes data, no special first-batch
    // handling for the second reader.

    assertFalse(reader1.closeCalled);
    assertFalse(reader2.openCalled);
    assertTrue(scan.next());
    assertTrue(reader1.closeCalled);
    assertTrue(reader2.openCalled);
    assertEquals(1, reader2.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifyBatch(100, scan.batchAccessor().getOutgoingContainer());

    // Second batch from second reader.

    assertTrue(scan.next());
    assertEquals(2, reader2.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifyBatch(120, scan.batchAccessor().getOutgoingContainer());

    // EOF

    assertFalse(scan.next());
    assertTrue(reader2.closeCalled);
    assertEquals(0, scan.batchAccessor().getRowCount());

    mockBatch.close();
  }

  /**
   * Multiple readers with a schema change between them.
   */

  @Test
  public void testSchemaChange() {
    MockEarlySchemaReader reader1 = new MockEarlySchemaReader();
    reader1.batchLimit = 2;
    MockEarlySchemaReader reader2 = new MockEarlySchemaReader2();
    reader2.batchLimit = 2;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .useLegacyWildcardExpansion(false)
        .addReader(reader1)
        .addReader(reader2)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    // Build schema

    assertTrue(scan.buildSchema());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // First batch

    assertTrue(scan.next());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // Second batch

    assertTrue(scan.next());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // Second reader.

    BatchSchema expectedSchema2 = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();

    assertTrue(scan.next());
    assertEquals(2, scan.batchAccessor().schemaVersion());
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema2)
        .addRow("10", "fred")
        .addRow("20", "wilma")
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Second batch from second reader.

    assertTrue(scan.next());
    assertEquals(2, scan.batchAccessor().schemaVersion());
    expected = fixture.rowSetBuilder(expectedSchema2)
        .addRow("30", "fred")
        .addRow("40", "wilma")
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    assertTrue(reader2.closeCalled);
    assertEquals(0, scan.batchAccessor().getRowCount());

    mockBatch.close();
  }

  /**
   * Test multiple readers, all EOF on first batch.
   */

  @Test
  public void testMultiEOFOnFirstBatch() {
    MockEarlySchemaReader reader1 = new MockEarlySchemaReader();
    reader1.batchLimit = 0;
    MockEarlySchemaReader reader2 = new MockEarlySchemaReader();
    reader2.batchLimit = 0;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader1)
        .addReader(reader2)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    // EOF

    assertTrue(scan.buildSchema());
    assertFalse(scan.next());
    assertTrue(reader1.closeCalled);
    assertTrue(reader2.closeCalled);
    assertEquals(0, scan.batchAccessor().getRowCount());

    mockBatch.close();
  }

  public final String ERROR_MSG = "My Bad!";

  @Test
  public void testExceptionOnOpen() {
    MockEarlySchemaReader reader = new MockEarlySchemaReader() {
      @Override
      public boolean open(SchemaNegotiator schemaNegotiator) {
        openCalled = true;
        throw new IllegalStateException(ERROR_MSG);
      }

    };
    reader.batchLimit = 0;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    try {
      scan.buildSchema();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertTrue(e.getCause() instanceof IllegalStateException);
    }
    assertTrue(reader.openCalled);

    assertEquals(0, scan.batchAccessor().getRowCount());
    mockBatch.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testUserExceptionOnOpen() {
    MockEarlySchemaReader reader = new MockEarlySchemaReader() {
      @Override
      public boolean open(SchemaNegotiator schemaNegotiator) {
        openCalled = true;
        throw UserException.dataReadError()
            .addContext(ERROR_MSG)
            .build(logger);
      }

    };
    reader.batchLimit = 2;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    try {
      scan.buildSchema();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertNull(e.getCause());
    }
    assertTrue(reader.openCalled);

    assertEquals(0, scan.batchAccessor().getRowCount());
    mockBatch.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testExceptionOnFirstNext() {
    MockEarlySchemaReader reader = new MockEarlySchemaReader() {
      @Override
      public boolean next() {
        super.next(); // Load some data
        throw new IllegalStateException(ERROR_MSG);
      }
    };
    reader.batchLimit = 2;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader)
          );
    ScanOperatorExec scan = mockBatch.scanOp;
    scan.buildSchema();

    try {
      scan.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertTrue(e.getCause() instanceof IllegalStateException);
    }
    assertTrue(reader.openCalled);

    assertEquals(0, scan.batchAccessor().getRowCount());
    mockBatch.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testUserExceptionOnFirstNext() {
    MockEarlySchemaReader reader = new MockEarlySchemaReader() {
      @Override
      public boolean next() {
        super.next(); // Load some data
        throw UserException.dataReadError()
            .addContext(ERROR_MSG)
            .build(logger);
      }
    };
    reader.batchLimit = 2;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    assertTrue(scan.buildSchema());

    // EOF

    try {
      scan.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertNull(e.getCause());
    }
    assertTrue(reader.openCalled);

    assertEquals(0, scan.batchAccessor().getRowCount());
    mockBatch.close();
    assertTrue(reader.closeCalled);
  }

  /**
   * Test throwing an exception after the first batch, but while
   * "reading" the second. Note that the first batch returns data
   * and is spread over two next() calls, so the error is on the
   * third call to the scan operator next().
   */

  @Test
  public void testExceptionOnSecondNext() {
    MockEarlySchemaReader reader = new MockEarlySchemaReader() {
      @Override
      public boolean next() {
        if (batchCount == 1) {
          super.next(); // Load some data
          throw new IllegalStateException(ERROR_MSG);
        }
        return super.next();
      }
    };
    reader.batchLimit = 2;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    // Schema

    assertTrue(scan.buildSchema());

    // First batch

    assertTrue(scan.next());
    scan.batchAccessor().release();

    // Fail

    try {
      scan.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertTrue(e.getCause() instanceof IllegalStateException);
    }

    mockBatch.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testUserExceptionOnSecondNext() {
    MockEarlySchemaReader reader = new MockEarlySchemaReader() {
      @Override
      public boolean next() {
        if (batchCount == 1) {
          super.next(); // Load some data
          throw UserException.dataReadError()
              .addContext(ERROR_MSG)
              .build(logger);
        }
        return super.next();
      }
    };
    reader.batchLimit = 2;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    // Schema

    assertTrue(scan.buildSchema());

    // First batch

    assertTrue(scan.next());
    scan.batchAccessor().release();

    // Fail

    try {
      scan.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertNull(e.getCause());
    }

    mockBatch.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testExceptionOnClose() {
    MockEarlySchemaReader reader1 = new MockEarlySchemaReader() {
      @Override
      public void close() {
        super.close();
        throw new IllegalStateException(ERROR_MSG);
       }
    };
    reader1.batchLimit = 2;

    MockEarlySchemaReader reader2 = new MockEarlySchemaReader();
    reader2.batchLimit = 2;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader1)
        .addReader(reader2)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    assertTrue(scan.buildSchema());

    assertTrue(scan.next());
    scan.batchAccessor().release();

    assertTrue(scan.next());
    scan.batchAccessor().release();

    // Fail on close of first reader

    try {
      scan.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertTrue(e.getCause() instanceof IllegalStateException);
    }
    assertTrue(reader1.closeCalled);
    assertFalse(reader2.openCalled);

    mockBatch.close();
  }


  @Test
  public void testUserExceptionOnClose() {
    MockEarlySchemaReader reader1 = new MockEarlySchemaReader() {
      @Override
      public void close() {
        super.close();
        throw UserException.dataReadError()
            .addContext(ERROR_MSG)
            .build(logger);
       }
    };
    reader1.batchLimit = 2;

    MockEarlySchemaReader reader2 = new MockEarlySchemaReader();
    reader2.batchLimit = 2;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setProjection(SELECT_STAR)
        .addReader(reader1)
        .addReader(reader2)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    assertTrue(scan.buildSchema());

    assertTrue(scan.next());
    scan.batchAccessor().release();

    assertTrue(scan.next());
    scan.batchAccessor().release();

    // Fail on close of first reader

    try {
      scan.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertNull(e.getCause());
    }
    assertTrue(reader1.closeCalled);
    assertFalse(reader2.openCalled);

    mockBatch.close();
  }

  /**
   * Mock reader that produces "jumbo" batches that cause a vector to
   * fill and a row to overflow from one batch to the next.
   */

  private static class OverflowReader extends BaseMockBatchReader {

    private final String value;
    public int rowCount;

    public OverflowReader() {
      char buf[] = new char[512];
      Arrays.fill(buf, 'x');
      value = new String(buf);
    }

    @Override
    public boolean open(SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      TupleMetadata schema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .buildSchema();
      schemaNegotiator.setTableSchema(schema);
      buildFilePath(schemaNegotiator);
      tableLoader = schemaNegotiator.build();
      return true;
    }

    @Override
    public boolean next() {
      batchCount++;
      if (batchCount > batchLimit) {
        return false;
      }

      RowSetLoader writer = tableLoader.writer();
      while (! writer.isFull()) {
        writer.start();
        writer.scalar(0).setString(value);
        writer.save();
        rowCount++;
      }

      // The vector overflowed on the last row. But, we still had to write the row.
      // The row is tucked away in the mutator to appear as the first row in
      // the next batch.

      return true;
    }
  }

  /**
   * Test multiple readers, with one of them creating "jumbo" batches
   * that overflow. Specifically, test a corner case. A batch ends right
   * at file EOF, but that last batch overflowed.
   */

  @Test
  public void testMultipleReadersWithOverflow() {
    OverflowReader reader1 = new OverflowReader();
    reader1.batchLimit = 2;
    reader1.filePath = new Path("hdfs:///w/x/y/a.csv");
    MockEarlySchemaReader reader2 = new MockEarlySchemaReader();
    reader2.batchLimit = 2;

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setSelectionRoot(MOCK_ROOT_PATH)
        .useLegacyWildcardExpansion(false)
        .setProjection(SELECT_STAR)
        .addReader(reader1)
        .addReader(reader2)
         );
    ScanOperatorExec scan = mockBatch.scanOp;

    assertTrue(scan.buildSchema());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // Second batch. Should be 1 less than the reader's row
    // count because the mutator has its own one-row lookahead batch.

    assertTrue(scan.next());
    assertEquals(1, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    int prevRowCount = scan.batchAccessor().getRowCount();
    assertEquals(reader1.rowCount - 1, prevRowCount);
    scan.batchAccessor().release();

    // Third batch, adds more data to the lookahead batch. Also overflows
    // so returned records is one less than total produced so far minus
    // those returned earlier.

    assertTrue(scan.next());
    assertEquals(2, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    assertEquals(reader1.rowCount - prevRowCount - 1, scan.batchAccessor().getRowCount());
    scan.batchAccessor().release();
    int prevReaderRowCount = reader1.rowCount;

    // Third batch. Returns the overflow row from the second batch of
    // the first reader.

    assertTrue(scan.next());
    assertEquals(3, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    assertEquals(1, scan.batchAccessor().getRowCount());
    assertEquals(prevReaderRowCount, reader1.rowCount);
    scan.batchAccessor().release();

    // Second reader.

    assertTrue(scan.next());
    assertEquals(2, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // Second batch from second reader.

    assertTrue(scan.next());
    assertEquals(2, reader2.batchCount);
    assertEquals(2, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // EOF

    assertFalse(scan.next());
    assertEquals(0, scan.batchAccessor().getRowCount());
    mockBatch.close();
  }

  private static class MockOneColEarlySchemaReader extends BaseMockBatchReader {

    @Override
    public boolean open(SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      buildFilePath(schemaNegotiator);
      TupleMetadata schema = new SchemaBuilder()
          .add("a", MinorType.INT)
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

    @Override
    protected void writeRow(RowSetLoader writer, int col1, String col2) {
      writer.start();
      if (writer.column(0) != null) {
        writer.scalar(0).setInt(col1 + 1);
      }
      writer.save();
    }
  }

  /**
   * Test the ability of the scan operator to "smooth" out schema changes
   * by reusing the type from a previous reader, if known. That is,
   * given three readers:<br>
   * (a, b)<br>
   * (b)<br>
   * (a, b)<br>
   * Then the type of column a should be preserved for the second reader that
   * does not include a. This works if a is nullable. If so, a's type will
   * be used for the empty column, rather than the usual nullable int.
   * <p>
   * Full testing of smoothing is done in
   * {#link TestScanProjector}. Here we just make sure that the
   * smoothing logic is available via the scan operator.
   */

  @Test
  public void testSchemaSmoothing() {

    // Reader returns (a, b)
    MockEarlySchemaReader reader1 = new MockEarlySchemaReader();
    reader1.batchLimit = 1;
    reader1.setFilePath("hdfs:///w/x/y/a.csv");

    // Reader returns (a)
    MockOneColEarlySchemaReader reader2 = new MockOneColEarlySchemaReader();
    reader2.batchLimit = 1;
    reader2.setFilePath("hdfs:///w/x/y/b.csv");
    reader2.startIndex = 100;

    // Reader returns (a, b)
    MockEarlySchemaReader reader3 = new MockEarlySchemaReader();
    reader3.batchLimit = 1;
    reader3.startIndex = 200;
    reader3.setFilePath("hdfs:///w/x/y/c.csv");

    MockBatch mockBatch = new MockBatch(new LegacyManagerBuilder()
        .setSelectionRoot(MOCK_ROOT_PATH)
        .setProjection(new String[]{"a", "b"})
        .addReader(reader1)
        .addReader(reader2)
        .addReader(reader3)
        );
    ScanOperatorExec scan = mockBatch.scanOp;

    // Schema based on (a, b)

    assertTrue(scan.buildSchema());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // Batch from (a, b) reader 1

    assertTrue(scan.next());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifyBatch(0, scan.batchAccessor().getOutgoingContainer());

    // Batch from (a) reader 2
    // Due to schema smoothing, b vector type is left unchanged,
    // but is null filled.

    assertTrue(scan.next());
    assertEquals(1, scan.batchAccessor().schemaVersion());

    SingleRowSet expected = fixture.rowSetBuilder(scan.batchAccessor().getSchema())
        .addRow(111, null)
        .addRow(121, null)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Batch from (a, b) reader 3
    // Recycles b again, back to being a table column.

    assertTrue(scan.next());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifyBatch(200, scan.batchAccessor().getOutgoingContainer());

    assertFalse(scan.next());
    mockBatch.close();
  }

  // Early schema without file info

  // TODO: Schema change in late reader
}