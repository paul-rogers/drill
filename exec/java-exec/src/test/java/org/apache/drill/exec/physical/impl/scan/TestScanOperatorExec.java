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
import org.apache.drill.exec.physical.impl.scan.RowBatchReader.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec.ScanOptions;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestScanOperatorExec extends SubOperatorTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestScanOperatorExec.class);

  private static abstract class BaseMockBatchReader implements RowBatchReader {
    protected ResultSetLoader mutator;

    protected void makeBatch() {
      TupleLoader writer = mutator.writer();
      mutator.startRow();
      writer.column(0).setInt(10);
      writer.column(1).setString("fred");
      mutator.saveRow();
      mutator.startRow();
      writer.column(0).setInt(20);
      writer.column(1).setString("wilma");
      mutator.saveRow();
    }

    @Override
    public void close() {
    }
  }

  private static class MockLateSchemaReader extends BaseMockBatchReader {

    public boolean openCalled;
    public boolean closeCalled;
    public int batchLimit;
    public int batchCount;
    public boolean returnDataOnFirst;

    @Override
    public void open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
      this.mutator = schemaNegotiator.build();
      openCalled = true;
    }

    @Override
    public boolean next() {
      batchCount++;
      if (batchCount > batchLimit) {
        return false;
      } else if (batchCount == 1) {
        TupleSchema schema = mutator.writer().schema();
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

  private static class MockLateSchemaReader2 extends MockLateSchemaReader {

    @Override
    public boolean next() {
      batchCount++;
      if (batchCount > batchLimit) {
        return false;
      } else if (batchCount == 1) {
        TupleSchema schema = mutator.writer().schema();
        MaterializedField a = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
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

      TupleLoader writer = mutator.writer();
      mutator.startRow();
      writer.column(0).setString("10");
      writer.column(1).setString("fred");
      mutator.saveRow();
      mutator.startRow();
      writer.column(0).setString("20");
      writer.column(1).setString("wilma");
      mutator.saveRow();
      return true;
    }
  }

  private static class MockEarlySchemaReader extends BaseMockBatchReader {

    public int batchLimit;
    public int batchCount;

    @Override
    public void open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
      MaterializedField a = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
      schemaNegotiator.addTableColumn(a);
      MaterializedField b = new SchemaBuilder.ColumnBuilder("b", MinorType.VARCHAR)
          .setMode(DataMode.OPTIONAL)
          .setWidth(10)
          .build();
      schemaNegotiator.addTableColumn(b);
      this.mutator = schemaNegotiator.build();
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

  private SingleRowSet makeExpected() {
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(10, "fred")
        .add(20, "wilma")
        .build();
    return expected;
  }

  private class MockBatch {

    private OperatorExecServicesImpl services;
    public ScanOperatorExec scanOp;

    public MockBatch(List<RowBatchReader> readers) {
      this(readers, null);
    }

    public MockBatch(List<RowBatchReader> readers, ScanOptions options) {
      if (options == null) {
        options = new ScanOptions();
      }
      if (options.selection == null) {
        options.selection = Lists.newArrayList(new SchemaPath[] {SchemaPath.getSimplePath("*")});
      }
      scanOp = new ScanOperatorExec(readers.iterator(), options);
      services = new OperatorExecServicesImpl(fixture.codeGenContext(), null, scanOp);
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
    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 2;
    List<RowBatchReader> readers = Lists.newArrayList(reader);

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(readers);
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
    reader.batchLimit = 1;
    List<RowBatchReader> readers = Lists.newArrayList(reader);

    // Create the scan operator

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: return schema.

    assertTrue(scan.buildSchema());
    assertEquals(0, reader.batchCount);
    // TODO: Failing here. Need to associate the merged container with
    // the scan operator. Need to preserve that container across readers
    // as part of the inventory.
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

  public static final String MOCK_FILE_NAME = "foo.csv";
  public static final String MOCK_SUFFIX = "csv";

  @Test
  public void testImplicitColumns() {

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

    // Reader with two batches: 1) schema only, 2) with data.

    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 2;
    List<RowBatchReader> readers = Lists.newArrayList(reader);

    // Set up implicit columns.

    ScanOptions options = new ScanOptions();
    options.implicitColumns = new ArrayList<>();
    options.implicitColumns.add(new ImplicitColumnDefn("filename", MOCK_FILE_NAME));
    options.implicitColumns.add(new ImplicitColumnDefn("suffix", MOCK_SUFFIX));

    // Initialize the scan operator

    MockBatch mockBatch = new MockBatch(readers, options);
    ScanOperatorExec scan = mockBatch.scanOp;

    // Schema should include implicit columns.

    assertTrue(scan.buildSchema());
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

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
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

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
  public void testEOFOnFirstBatch() {
    MockLateSchemaReader reader = new MockLateSchemaReader();
    reader.batchLimit = 0;
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // EOF

    assertFalse(scan.buildSchema());
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
    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    MockLateSchemaReader reader1 = new MockLateSchemaReader();
    reader1.batchLimit = 2;
    reader1.returnDataOnFirst = true;
    MockLateSchemaReader reader2 = new MockLateSchemaReader();
    reader2.batchLimit = 2;
    reader2.returnDataOnFirst = true;
    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2);

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch. The reader returns a non-empty batch. The scan
    // operator strips off the schema and returns just that.

    assertTrue(scan.buildSchema());
    assertEquals(1, reader1.batchCount);
    assertEquals(expected.batchSchema(), scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

    // Second batch. Returns the "look-ahead" batch returned by
    // the reader earlier.

    assertTrue(scan.next());
    assertEquals(1, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Third batch, normal case.

    assertTrue(scan.next());
    assertEquals(2, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Second reader. First batch includes data, no special first-batch
    // handling for the second reader.

    assertFalse(reader1.closeCalled);
    assertFalse(reader2.openCalled);
    assertTrue(scan.next());
    assertTrue(reader1.closeCalled);
    assertTrue(reader2.openCalled);
    assertEquals(1, reader2.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Second batch from second reader.

    assertTrue(scan.next());
    assertEquals(2, reader2.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifier.verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

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
    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    MockLateSchemaReader reader1 = new MockLateSchemaReader();
    reader1.batchLimit = 2;
    reader1.returnDataOnFirst = true;
    MockLateSchemaReader reader2 = new MockLateSchemaReader2();
    reader2.batchLimit = 2;
    reader2.returnDataOnFirst = true;
    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2);

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch. The reader returns a non-empty batch. The scan
    // operator strips off the schema and returns just that.

    assertTrue(scan.buildSchema());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

    // Second batch. Returns the "look-ahead" batch returned by
    // the reader earlier.

    assertTrue(scan.next());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Third batch, normal case.

    assertTrue(scan.next());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    verifier.verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Second reader. First batch includes data, no special first-batch
    // handling for the second reader.

    BatchSchema expectedSchema2 = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();
    SingleRowSet expected2 = fixture.rowSetBuilder(expectedSchema2)
        .add("10", "fred")
        .add("20", "wilma")
        .build();
    verifier = new RowSetComparison(expected2);

    assertTrue(scan.next());
    assertEquals(2, scan.batchAccessor().schemaVersion());
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Second batch from second reader.

    assertTrue(scan.next());
    assertEquals(2, scan.batchAccessor().schemaVersion());
    verifier.verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

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
    MockLateSchemaReader reader1 = new MockLateSchemaReader();
    reader1.batchLimit = 0;
    reader1.returnDataOnFirst = true;
    MockLateSchemaReader reader2 = new MockLateSchemaReader();
    reader2.batchLimit = 0;
    reader2.returnDataOnFirst = true;
    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2);

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // EOF

    assertFalse(scan.buildSchema());
    assertTrue(reader1.closeCalled);
    assertTrue(reader2.closeCalled);
    assertEquals(0, scan.batchAccessor().getRowCount());

    mockBatch.close();
  }

  public final String ERROR_MSG = "My Bad!";

  @Test
  public void testExceptionOnOpen() {
    MockLateSchemaReader reader = new MockLateSchemaReader() {
      @Override
      public void open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
        super.open(context, schemaNegotiator);
        throw new IllegalStateException(ERROR_MSG);
      }

    };
    reader.batchLimit = 0;
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // EOF

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
    MockLateSchemaReader reader = new MockLateSchemaReader() {
      @Override
      public void open(OperatorExecServices context, SchemaNegotiator schemaNegotiator) {
        super.open(context, schemaNegotiator);
        throw UserException.dataReadError()
            .addContext(ERROR_MSG)
            .build(logger);
      }

    };
    reader.batchLimit = 2;
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // EOF

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
    MockLateSchemaReader reader = new MockLateSchemaReader() {
      @Override
      public boolean next() {
        super.next(); // Load some data
        throw new IllegalStateException(ERROR_MSG);
      }
    };
    reader.batchLimit = 2;
    reader.returnDataOnFirst = true;
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // EOF

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
  public void testUserExceptionOnFirstNext() {
    MockLateSchemaReader reader = new MockLateSchemaReader() {
      @Override
      public boolean next() {
        super.next(); // Load some data
        throw UserException.dataReadError()
            .addContext(ERROR_MSG)
            .build(logger);
      }
    };
    reader.batchLimit = 2;
    reader.returnDataOnFirst = true;
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // EOF

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

  /**
   * Test throwing an exception after the first batch, but while
   * "reading" the second. Note that the first batch returns data
   * and is spread over two next() calls, so the error is on the
   * third call to the scan operator next().
   */

  @Test
  public void testExceptionOnSecondNext() {
    MockLateSchemaReader reader = new MockLateSchemaReader() {
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
    reader.returnDataOnFirst = true;
    List<RowBatchReader> readers = Lists.newArrayList(reader);
    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // Schema

    assertTrue(scan.buildSchema());

    // Cached batch

    assertTrue(scan.next());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

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
    MockLateSchemaReader reader = new MockLateSchemaReader() {
      @Override
      public boolean next() {
        if (batchCount == 2) {
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

    // Cached batch

    assertTrue(scan.next());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

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
    MockLateSchemaReader reader1 = new MockLateSchemaReader() {
      @Override
      public void close() {
        super.close(); // Load some data
        throw new IllegalStateException(ERROR_MSG);
       }
    };
    reader1.batchLimit = 2;
    reader1.returnDataOnFirst = true;

    MockLateSchemaReader reader2 = new MockLateSchemaReader();
    reader2.batchLimit = 2;
    reader2.returnDataOnFirst = true;

    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2);
    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // Schema

    assertTrue(scan.buildSchema());

    // Cached batch

    assertTrue(scan.next());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

    // Second batch for first reader

    assertTrue(scan.next());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

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

  private static class OverflowReader extends MockLateSchemaReader {

    private final String value;
    public int rowCount;

    public OverflowReader() {
      char buf[] = new char[512];
      Arrays.fill(buf, 'x');
      value = new String(buf);
    }

    @Override
    public boolean next() {
      batchCount++;
      if (batchCount > batchLimit) {
        return false;
      } else if (batchCount == 1) {
        TupleSchema schema = mutator.writer().schema();
        MaterializedField a = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
        schema.addColumn(a);
        if ( ! returnDataOnFirst) {
          return true;
        }
      }

      TupleLoader writer = mutator.writer();
      while (! mutator.isFull()) {
        mutator.startRow();
        writer.column(0).setString(value);
        mutator.saveRow();
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
    reader1.returnDataOnFirst = true;
    MockLateSchemaReader reader2 = new MockLateSchemaReader();
    reader2.batchLimit = 2;
    reader2.returnDataOnFirst = true;
    List<RowBatchReader> readers = Lists.newArrayList(reader1, reader2);

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch. The reader returns a non-empty batch. The scan
    // operator strips off the schema and returns just that.

    assertTrue(scan.buildSchema());
    assertEquals(1, reader1.batchCount);
    int prevReaderRowCount = reader1.rowCount;
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

    // Second batch. Returns the "look-ahead" batch returned by
    // the reader earlier. Should be 1 less than the reader's row
    // count because the mutator has its own one-row lookahead batch.

    assertTrue(scan.next());
    assertEquals(1, reader1.batchCount);
    assertEquals(prevReaderRowCount, reader1.rowCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    assertEquals(prevReaderRowCount - 1, scan.batchAccessor().getRowCount());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();
    int prevRowCount = scan.batchAccessor().getRowCount();

    // Third batch, adds more data to the lookahead batch. Also overflows
    // so returned records is one less than total produced so far minus
    // those returned earlier.

    assertTrue(scan.next());
    assertEquals(2, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    assertEquals(reader1.rowCount - prevRowCount - 1, scan.batchAccessor().getRowCount());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();
    prevReaderRowCount = reader1.rowCount;

    // Third batch. Returns the overflow row from the second batch of
    // the first reader.

    assertTrue(scan.next());
    assertEquals(3, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    assertEquals(1, scan.batchAccessor().getRowCount());
    assertEquals(prevReaderRowCount, reader1.rowCount);
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

    // Second reader.

    assertTrue(scan.next());
    assertEquals(2, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

    // Second batch from second reader.

    assertTrue(scan.next());
    assertEquals(2, reader2.batchCount);
    assertEquals(2, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().getOutgoingContainer().zeroVectors();

    // EOF

    assertFalse(scan.next());
    assertEquals(0, scan.batchAccessor().getRowCount());
    mockBatch.close();
  }
}