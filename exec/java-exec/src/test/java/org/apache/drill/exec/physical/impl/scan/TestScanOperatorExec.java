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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.OperatorExecServices;
import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch.OperatorExecServicesImpl;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec.ScanOptions;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestScanOperatorExec extends SubOperatorTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestScanOperatorExec.class);

  public static final String MOCK_FILE_PATH = "hdfs:///w/x/y/foo.csv";
  public static final String MOCK_ROOT_PATH = "hdfs:///w";
  public static final String MOCK_FILE_NAME = "foo.csv";
  public static final String MOCK_SUFFIX = "csv";
  public static final String MOCK_DIR0 = "x";

  private static abstract class BaseMockBatchReader implements RowBatchReader {
    public boolean openCalled;
    public boolean closeCalled;
    public int startIndex;
    public int batchCount;
    public int batchLimit;
    protected ResultSetLoader tableLoader;
    protected String[] select = SELECT_STAR;
    protected Path filePath;
    protected Path rootPath;

    public void setSelect(String[] select) {
      this.select = select;
    }

    public void setFilePath(String filePath, String rootPath) {
      this.filePath = new Path(filePath);
      if (rootPath == null) {
        this.rootPath = null;
      } else {
        this.rootPath = new Path(rootPath);
      }
    }

    public void setDefaultFilePath() {
      setFilePath(MOCK_FILE_PATH, MOCK_ROOT_PATH);
    }

    protected void buildSelect(SchemaNegotiator schemaNegotiator) {
      if (select == null) {
        return;
      }
      for (String col : select) {
        schemaNegotiator.addSelectColumn(col);
      }
    }

    protected void buildFilePath(SchemaNegotiator schemaNegotiator) {
      schemaNegotiator.setFilePath(filePath);
      schemaNegotiator.setSelectionRoot(rootPath);
    }

    protected void makeBatch() {
      TupleLoader writer = tableLoader.writer();
      int offset = (batchCount - 1) * 20 + startIndex;
      writeRow(writer, offset + 10, "fred");
      writeRow(writer, offset + 20, "wilma");
    }

    protected void writeRow(TupleLoader writer, int col1, String col2) {
      tableLoader.startRow();
      if (writer.column(0) != null) {
        writer.column(0).setInt(col1);
      }
      if (writer.column(1) != null) {
        writer.column(1).setString(col2);
      }
      tableLoader.saveRow();
    }

    @Override
    public void close() {
      closeCalled = true;
    }
  }

  private static class MockLateSchemaReader extends BaseMockBatchReader {

    public boolean returnDataOnFirst;

    @Override
    public boolean open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
      this.tableLoader = schemaNegotiator.build();
      openCalled = true;
      return true;
    }

    @Override
    public boolean next() {
      batchCount++;
      if (batchCount > batchLimit) {
        return false;
      } else if (batchCount == 1) {
        TupleSchema schema = tableLoader.writer().schema();
        MaterializedField a = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
        schema.addColumn(a);
        MaterializedField b = new SchemaBuilder.ColumnBuilder("b", MinorType.VARCHAR)
            .setMode(DataMode.OPTIONAL)
            .setWidth(10)
            .build();
        schema.addColumn(b);
        if ( ! returnDataOnFirst) {
          return true;
        }
      }

      makeBatch();
      return true;
    }

    @Override
    public void close() {
      closeCalled = true;
    }
  }

  private static class MockNullEarlySchemaReader extends BaseMockBatchReader {

    @Override
    public boolean open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
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
    public boolean open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      buildSelect(schemaNegotiator);
      buildFilePath(schemaNegotiator);
      MaterializedField a = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
      schemaNegotiator.addTableColumn(a);
      MaterializedField b = new SchemaBuilder.ColumnBuilder("b", MinorType.VARCHAR)
          .setMode(DataMode.OPTIONAL)
          .setWidth(10)
          .build();
      schemaNegotiator.addTableColumn(b);
      this.tableLoader = schemaNegotiator.build();
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
    public boolean open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      buildSelect(schemaNegotiator);
      buildFilePath(schemaNegotiator);
      MaterializedField a = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
      schemaNegotiator.addTableColumn(a);
      MaterializedField b = new SchemaBuilder.ColumnBuilder("b", MinorType.VARCHAR)
          .setMode(DataMode.OPTIONAL)
          .setWidth(10)
          .build();
      schemaNegotiator.addTableColumn(b);
      this.tableLoader = schemaNegotiator.build();
      return true;
    }

    @Override
    protected void writeRow(TupleLoader writer, int col1, String col2) {
      tableLoader.startRow();
      if (writer.column(0) != null) {
        writer.column(0).setString(Integer.toString(col1));
      }
      if (writer.column(1) != null) {
        writer.column(1).setString(col2);
      }
      tableLoader.saveRow();
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
        .add(offset + 10, "fred")
        .add(offset + 20, "wilma")
        .build();
    return expected;
  }

  private void verifyBatch(int offset, VectorContainer output) {
    SingleRowSet expected = makeExpected(offset);
    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(output));
  }

  private static final String STAR = "*";
  private static final String[] SELECT_STAR = new String[] { STAR };

  private static class MockBatch {

    private OperatorExecServicesImpl services;
    public ScanOperatorExec scanOp;

    public MockBatch(List<RowBatchReader> readers) {
      this(readers, (ScanOptions) null);
    }

    public MockBatch(List<RowBatchReader> readers, ScanOptions options) {
      scanOp = new ScanOperatorExec(readers.iterator(), options);
      services = new OperatorExecServicesImpl(fixture.codeGenContext(), null, scanOp);
      scanOp.bind(services);
    }

    public MockBatch(List<RowBatchReader> readers, String select[]) {
      this(readers, toOptions(select));
    }

    private static ScanOptions toOptions(String select[]) {
      ScanOptions options = null;
      if (select != null) {
        options = new ScanOptions();
        options.selection = new ArrayList<>();
        for (String col : select) {
          options.selection.add(SchemaPath.getSimplePath(col));
        }
      }
      return options;
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
  @Ignore("Not yet")
  public void testLateSchemaLifecycle() {
    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 2;
    List<RowBatchReader> readers = Lists.newArrayList(reader);

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(readers, SELECT_STAR);
    ScanOperatorExec scan = mockBatch.scanOp;

    // Standard startup

    assertFalse(reader.openCalled);

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    assertTrue(reader.openCalled);
    assertEquals(0, reader.batchCount);
    assertEquals(expected.batchSchema(), scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());

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
    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockEarlySchemaReader reader = new MockEarlySchemaReader();
    reader.setSelect(null);
    reader.batchLimit = 1;
    List<RowBatchReader> readers = Lists.newArrayList(reader);

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(readers, SELECT_STAR);
    ScanOperatorExec scan = mockBatch.scanOp;

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

    mockBatch.close();
  }

  /**
   * Basic sanity test of a couple of implicit columns, along
   * with all table columns in table order. Full testing of implicit
   * columns is done on lower-level components.
   */
  @Test
  public void testImplicitColumns() {

    MockEarlySchemaReader reader = new MockEarlySchemaReader();

    // Select table and implicit columns.

    reader.setSelect(new String[] {"a", "b", "filename", "suffix"});

    // Use the test file path

    reader.setDefaultFilePath();
    reader.batchLimit = 1;
    List<RowBatchReader> readers = Lists.newArrayList(reader);

    // Initialize the scan operator

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // Expect data and implicit columns

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("filename", MinorType.VARCHAR)
        .add("suffix", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(10, "fred", MOCK_FILE_NAME, MOCK_SUFFIX)
        .add(20, "wilma", MOCK_FILE_NAME, MOCK_SUFFIX)
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

    // Select table and implicit columns.

    reader.setSelect(new String[] {"dir0", "b", "filename", "c", "suffix"});

    // Use the test file path

    reader.setDefaultFilePath();
    reader.batchLimit = 1;
    List<RowBatchReader> readers = Lists.newArrayList(reader);

    // Initialize the scan operator

    MockBatch mockBatch = new MockBatch(readers);
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
        .add(MOCK_DIR0, "fred", MOCK_FILE_NAME, null, MOCK_SUFFIX)
        .add(MOCK_DIR0, "wilma", MOCK_FILE_NAME, null, MOCK_SUFFIX)
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
  @Ignore("Needs late schema - not yet")
  public void testNonEmptyFirstBatch() {
    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 2;
    reader.returnDataOnFirst = true;
    List<RowBatchReader> readers = Lists.newArrayList(reader);

    MockBatch mockBatch = new MockBatch(readers);
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
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
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
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
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

    List<RowBatchReader> readers = Lists.newArrayList(nullReader, reader1, reader2);
    MockBatch mockBatch = new MockBatch(readers);
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
    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2);

    MockBatch mockBatch = new MockBatch(readers);
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
        .add("10", "fred")
        .add("20", "wilma")
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Second batch from second reader.

    assertTrue(scan.next());
    assertEquals(2, scan.batchAccessor().schemaVersion());
    expected = fixture.rowSetBuilder(expectedSchema2)
        .add("30", "fred")
        .add("40", "wilma")
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
    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2);

    MockBatch mockBatch = new MockBatch(readers);
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
      public boolean open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
        openCalled = true;
        throw new IllegalStateException(ERROR_MSG);
      }

    };
    reader.batchLimit = 0;
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
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
      public boolean open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
        openCalled = true;
        throw UserException.dataReadError()
            .addContext(ERROR_MSG)
            .build(logger);
      }

    };
    reader.batchLimit = 2;
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
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
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
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
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;
    scan.buildSchema();

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
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
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
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
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

    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2);
    MockBatch mockBatch = new MockBatch(readers);
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
    public boolean open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      buildSelect(schemaNegotiator);
      MaterializedField a = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
      schemaNegotiator.addTableColumn(a);
      this.tableLoader = schemaNegotiator.build();
      return true;
    }

    @Override
    public boolean next() {
      batchCount++;
      if (batchCount > batchLimit) {
        return false;
      }

      TupleLoader writer = tableLoader.writer();
      while (! tableLoader.isFull()) {
        tableLoader.startRow();
        writer.column(0).setString(value);
        tableLoader.saveRow();
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
    MockEarlySchemaReader reader2 = new MockEarlySchemaReader();
    reader2.batchLimit = 2;
    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2);

    MockBatch mockBatch = new MockBatch(readers);
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
    public boolean open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      buildSelect(schemaNegotiator);
      MaterializedField a = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
      schemaNegotiator.addTableColumn(a);
      this.tableLoader = schemaNegotiator.build();
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
    protected void writeRow(TupleLoader writer, int col1, String col2) {
      tableLoader.startRow();
      if (writer.column(0) != null) {
        writer.column(0).setInt(col1 + 1);
      }
      tableLoader.saveRow();
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
   * This trick works only for an explicit select list. Does not work for
   * SELECT *.
   */

  @Test
  public void testSchemaSmoothing() {
    String selectList[] = new String[]{"a", "b"};
    // Reader returns (a, b)
    MockEarlySchemaReader reader1 = new MockEarlySchemaReader();
    reader1.batchLimit = 1;
    reader1.setSelect(selectList);
    // Reader returns (a)
    MockOneColEarlySchemaReader reader2 = new MockOneColEarlySchemaReader();
    reader2.batchLimit = 1;
    reader2.setSelect(selectList);
    reader2.startIndex = 100;
    // Reader returns (a, b)
    MockEarlySchemaReader reader3 = new MockEarlySchemaReader();
    reader3.batchLimit = 1;
    reader3.startIndex = 200;
    reader3.setSelect(selectList);
    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2, reader3);

    MockBatch mockBatch = new MockBatch(readers);
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
        .addSingleCol(111)
        .addSingleCol(121)
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

  // TODO: Test schema "smoothing" limits: required
  // TODO: Test schema smoothing with repeated
  // TODO: Test schema negotiation
  // TODO: Test set missing vector type
}