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
package org.apache.drill.exec.physical.impl;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.physical.impl.ScanOperatorExec.ImplicitColumn;
import org.apache.drill.exec.physical.impl.ScanOperatorExec.ScanOptions;
import org.apache.drill.exec.physical.rowSet.RowSetMutator;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RowReader;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestScanOperatorExec extends SubOperatorTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestScanOperatorExec.class);

  private static class MockRowReader implements RowReader {

    public boolean openCalled;
    public boolean closeCalled;
    private RowSetMutator mutator;
    public int batchLimit;
    public int batchCount;
    public boolean returnDataOnFirst;

    @Override
    public void open(OperatorExecContext context, RowSetMutator mutator) {
      this.mutator = mutator;
      openCalled = true;
    }

    @Override
    public boolean next() {
      batchCount++;
      if (batchLimit < 0 || batchCount > batchLimit) {
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

      TupleLoader writer = mutator.writer();
      mutator.startRow();
      writer.column(0).setInt(10);
      writer.column(1).setString("fred");
      mutator.saveRow();
      mutator.startRow();
      writer.column(0).setInt(20);
      writer.column(1).setString("wilma");
      mutator.saveRow();
      return true;
    }

    @Override
    public void close() {
      mutator.close();
      closeCalled = true;
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

  @Test
  public void testNormalLifecycle() {
    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    // Create a mock reader, return two batches: one schema-only, another with data.

    MockRowReader reader = new MockRowReader();
    reader.batchLimit = 2;
    List<RowReader> readers = Lists.newArrayList(reader);

    // Create options and the scan operator

    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);

    // Standard startup

    scan.bind();
    scan.start();
    assertFalse(reader.openCalled);

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    assertTrue(reader.openCalled);
    assertEquals(expected.batchSchema(), scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());

    // Next call, return with data.

    assertTrue(scan.next());
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    assertTrue(reader.closeCalled);

    scan.close();
    expected.clear();
  }

  public static final String MOCK_FILE_NAME = "foo.csv";
  public static final String MOCK_SUFFIX = "csv";

  @Test
  public void testImplicitColumns() {

    // Expect data and implicit columns

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("filename", MinorType.VARCHAR, MOCK_FILE_NAME.length())
        .add("suffix", MinorType.VARCHAR, MOCK_SUFFIX.length())
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(10, "fred", MOCK_FILE_NAME, MOCK_SUFFIX)
        .add(20, "wilma", MOCK_FILE_NAME, MOCK_SUFFIX)
        .build();
    RowSetComparison verifier = new RowSetComparison(expected);

    // Reader with two batches: 1) schema only, 2) with data.

    MockRowReader reader = new MockRowReader();
    reader.batchLimit = 2;
    List<RowReader> readers = Lists.newArrayList(reader);

    // Set up implicit columns.

    ScanOptions options = new ScanOptions();
    options.implicitColumns = new ArrayList<>();
    options.implicitColumns.add(new ImplicitColumn("filename", MOCK_FILE_NAME));
    options.implicitColumns.add(new ImplicitColumn("suffix", MOCK_SUFFIX));

    // Initialize the scan operator

    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // Schema should include implicit columns.

    assertTrue(scan.buildSchema());
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    scan.batchAccessor().getOutgoingContainer().clear();

    // Read one batch, should contain implicit columns

    assertTrue(scan.next());
    verifier.verifyAndClearAll(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    scan.close();
    expected.clear();
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

    MockRowReader reader = new MockRowReader();
    reader.batchLimit = 2;
    reader.returnDataOnFirst = true;
    List<RowReader> readers = Lists.newArrayList(reader);
    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // First batch. The reader returns a non-empty batch. The scan
    // operator strips off the schema and returns just that.

    assertTrue(scan.buildSchema());
    assertEquals(1, reader.batchCount);
    assertEquals(expected.batchSchema(), scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());
    scan.batchAccessor().getOutgoingContainer().clear();

    // Second batch. Returns the "look-ahead" batch returned by
    // the reader earlier.

    assertTrue(scan.next());
    assertEquals(1, reader.batchCount);
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Third batch, normal case.

    assertTrue(scan.next());
    assertEquals(2, reader.batchCount);
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    assertTrue(reader.closeCalled);

    scan.close();
    expected.clear();
  }

  /**
   * Test EOF on the first batch. Is allowed, but will result in the scan operator
   * passing a null batch to the parent.
   */

  @Test
  public void testEOFOnFirstBatch() {
    MockRowReader reader = new MockRowReader();
    reader.batchLimit = 0;
    List<RowReader> readers = Lists.newArrayList(reader);
    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // EOF

    assertFalse(scan.buildSchema());
    assertTrue(reader.closeCalled);

    scan.close();
  }

  /**
   * Test normal case with multiple readers.
   */

  @Test
  public void testMultipleReaders() {
    SingleRowSet expected = makeExpected();
    RowSetComparison verifier = new RowSetComparison(expected);

    MockRowReader reader1 = new MockRowReader();
    reader1.batchLimit = 2;
    reader1.returnDataOnFirst = true;
    MockRowReader reader2 = new MockRowReader();
    reader2.batchLimit = 2;
    reader2.returnDataOnFirst = true;
    List<RowReader> readers = Lists.newArrayList(reader1, reader2);

    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // First batch. The reader returns a non-empty batch. The scan
    // operator strips off the schema and returns just that.

    assertTrue(scan.buildSchema());
    assertEquals(1, reader1.batchCount);
    assertEquals(expected.batchSchema(), scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());
    scan.batchAccessor().getOutgoingContainer().clear();

    // Second batch. Returns the "look-ahead" batch returned by
    // the reader earlier.

    assertTrue(scan.next());
    assertEquals(1, reader1.batchCount);
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Third batch, normal case.

    assertTrue(scan.next());
    assertEquals(2, reader1.batchCount);
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Second reader. First batch includes data, no special first-batch
    // handling for the second reader.

    assertFalse(reader1.closeCalled);
    assertFalse(reader2.openCalled);
    assertTrue(scan.next());
    assertTrue(reader1.closeCalled);
    assertTrue(reader2.openCalled);
    assertEquals(1, reader2.batchCount);
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // Second batch from second reader.

    assertTrue(scan.next());
    assertEquals(2, reader2.batchCount);
    verifier.verifyAndClear(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));

    // EOF

    assertFalse(scan.next());
    assertTrue(reader2.closeCalled);

    scan.close();
    expected.clear();
  }

  /**
   * Test multiple readers, all EOF on first batch.
   */

  @Test
  public void testMultiEOFOnFirstBatch() {
    MockRowReader reader1 = new MockRowReader();
    reader1.batchLimit = 0;
    reader1.returnDataOnFirst = true;
    MockRowReader reader2 = new MockRowReader();
    reader2.batchLimit = 0;
    reader2.returnDataOnFirst = true;
    List<RowReader> readers = Lists.newArrayList(reader1, reader2);

    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // EOF

    assertFalse(scan.buildSchema());
    assertTrue(reader1.closeCalled);
    assertTrue(reader2.closeCalled);

    scan.close();
  }

  public final String ERROR_MSG = "My Bad!";

  @Test
  public void testExceptionOnOpen() {
    MockRowReader reader = new MockRowReader() {
      @Override
      public void open(OperatorExecContext context, RowSetMutator mutator) {
        super.open(context, mutator);
        throw new IllegalStateException(ERROR_MSG);
      }

    };
    reader.batchLimit = 0;
    List<RowReader> readers = Lists.newArrayList(reader);
    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // EOF

    try {
      scan.buildSchema();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertTrue(e.getCause() instanceof IllegalStateException);
    }
    assertTrue(reader.openCalled);

    scan.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testUserExceptionOnOpen() {
    MockRowReader reader = new MockRowReader() {
      @Override
      public void open(OperatorExecContext context, RowSetMutator mutator) {
        super.open(context, mutator);
        throw UserException.dataReadError()
            .addContext(ERROR_MSG)
            .build(logger);
      }

    };
    reader.batchLimit = 2;
    List<RowReader> readers = Lists.newArrayList(reader);
    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // EOF

    try {
      scan.buildSchema();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertNull(e.getCause());
    }
    assertTrue(reader.openCalled);

    scan.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testExceptionOnFirstNext() {
    MockRowReader reader = new MockRowReader() {
      @Override
      public boolean next() {
        super.next(); // Load some data
        throw new IllegalStateException(ERROR_MSG);
      }
    };
    reader.batchLimit = 2;
    reader.returnDataOnFirst = true;
    List<RowReader> readers = Lists.newArrayList(reader);
    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // EOF

    try {
      scan.buildSchema();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertTrue(e.getCause() instanceof IllegalStateException);
    }
    assertTrue(reader.openCalled);

    scan.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testUserExceptionOnFirstNext() {
    MockRowReader reader = new MockRowReader() {
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
    List<RowReader> readers = Lists.newArrayList(reader);
    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // EOF

    try {
      scan.buildSchema();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertNull(e.getCause());
    }
    assertTrue(reader.openCalled);

    scan.close();
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
    MockRowReader reader = new MockRowReader() {
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
    List<RowReader> readers = Lists.newArrayList(reader);
    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // Schema

    assertTrue(scan.buildSchema());

    // Cached batch

    assertTrue(scan.next());
    scan.batchAccessor().getOutgoingContainer().clear();

    // Fail

    try {
      scan.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertTrue(e.getCause() instanceof IllegalStateException);
    }

    scan.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testUserExceptionOnSecondNext() {
    MockRowReader reader = new MockRowReader() {
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
    List<RowReader> readers = Lists.newArrayList(reader);
    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // Schema

    assertTrue(scan.buildSchema());

    // Cached batch

    assertTrue(scan.next());
    scan.batchAccessor().getOutgoingContainer().clear();

    // Fail

    try {
      scan.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains(ERROR_MSG));
      assertNull(e.getCause());
    }

    scan.close();
    assertTrue(reader.closeCalled);
  }

  @Test
  public void testExceptionOnClose() {
    MockRowReader reader1 = new MockRowReader() {
      @Override
      public void close() {
        super.close(); // Load some data
        throw new IllegalStateException(ERROR_MSG);
       }
    };
    reader1.batchLimit = 2;
    reader1.returnDataOnFirst = true;
    
    MockRowReader reader2 = new MockRowReader();
    reader2.batchLimit = 2;
    reader2.returnDataOnFirst = true;
    
    List<RowReader> readers = Lists.newArrayList(reader1, reader2);
    ScanOptions options = new ScanOptions();
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();

    // Schema

    assertTrue(scan.buildSchema());

    // Cached batch

    assertTrue(scan.next());
    scan.batchAccessor().getOutgoingContainer().clear();
    
    // Second batch for first reader

    assertTrue(scan.next());
    scan.batchAccessor().getOutgoingContainer().clear();

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

    scan.close();
  }

  // Test exceptions on each method

  // Test overflow row on last batch

  // Test same schema across readers: same schema version.

  // Test distinct schemas across batches: new schema version.

}
