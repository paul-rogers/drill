package org.apache.drill.exec.store.mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.OperatorRecordBatch.OperatorExecServicesImpl;
import org.apache.drill.exec.physical.impl.ScanOperatorExec;
import org.apache.drill.exec.physical.impl.ScanOperatorExec.ScanOptions;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.RowReader;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestMockRowReader extends SubOperatorTest {

  private class MockBatch {

    private OperatorExecServicesImpl services;
    public ScanOperatorExec scanOp;

    public MockBatch(List<RowReader> readers) {
      this(readers, null);
    }

    public MockBatch(List<RowReader> readers, ScanOptions options) {
      if (options == null) {
        scanOp = new ScanOperatorExec(readers.iterator());
      } else {
        scanOp = new ScanOperatorExec(readers.iterator(), options);
      }
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

  /**
   * Test the most basic case: required integers and strings.
   */

  @Test
  public void testBasics() {
    int rowCount = 10;
    MockTableDef.MockColumn cols[] = new MockTableDef.MockColumn[] {
        new MockTableDef.MockColumn("a", MinorType.INT, DataMode.REQUIRED, null, null, null, null, null, null ),
        new MockTableDef.MockColumn("b", MinorType.VARCHAR, DataMode.REQUIRED, 10, null, null, null, null, null )
    };
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, null, null, cols);
    RowReader reader = new ExtendedMockRecordReader(entry);
    List<RowReader> readers = Lists.newArrayList(reader);

    // Create options and the scan operator

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR, 10) // Width is reflected in meta-data
        .build();
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());

    // Next call, return with data.

    assertTrue(scan.next());
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    assertEquals(rowCount, scan.batchAccessor().getRowCount());
    scan.batchAccessor().release();

    // EOF

    assertFalse(scan.next());
    mockBatch.close();
  }

  /**
   * Verify that the mock reader can generate nullalble (optional) columns,
   * including filling values with nulls at some percentage, 25% by
   * default.
   */

  @Test
  public void testOptional() {
    int rowCount = 10;
    Map<String,Object> props = new HashMap<>();
    props.put("nulls", 50);
    MockTableDef.MockColumn cols[] = new MockTableDef.MockColumn[] {
        new MockTableDef.MockColumn("a", MinorType.INT, DataMode.OPTIONAL, null, null, null, null, null, null ),
        new MockTableDef.MockColumn("b", MinorType.VARCHAR, DataMode.OPTIONAL, 10, null, null, null, null, props )
    };
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, null, null, cols);
    RowReader reader = new ExtendedMockRecordReader(entry);
    List<RowReader> readers = Lists.newArrayList(reader);

    // Create options and the scan operator

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());

    // Next call, return with data.

    assertTrue(scan.next());
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    assertEquals(rowCount, scan.batchAccessor().getRowCount());
    scan.batchAccessor().release();

    // EOF

    assertFalse(scan.next());
    mockBatch.close();
  }

  /**
   * Test a repeated column.
   */

  @Test
  public void testColumnRepeat() {
    int rowCount = 10;
    MockTableDef.MockColumn cols[] = new MockTableDef.MockColumn[] {
        new MockTableDef.MockColumn("a", MinorType.INT, DataMode.REQUIRED, null, null, null, null, 3, null ),
        new MockTableDef.MockColumn("b", MinorType.VARCHAR, DataMode.REQUIRED, 10, null, null, null, null, null )
    };
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, null, null, cols);
    RowReader reader = new ExtendedMockRecordReader(entry);
    List<RowReader> readers = Lists.newArrayList(reader);

    // Create options and the scan operator

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a1", MinorType.INT)
        .add("a2", MinorType.INT)
        .add("a3", MinorType.INT)
        .add("b", MinorType.VARCHAR, 10)
        .build();
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());

    // Next call, return with data.

    assertTrue(scan.next());
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    assertEquals(rowCount, scan.batchAccessor().getRowCount());
    scan.batchAccessor().release();

    // EOF

    assertFalse(scan.next());
    mockBatch.close();
  }

  /**
   * Verify limit on individual batch size (limiting row count per batch).
   */

  @Test
  public void testBatchSize() {
    int rowCount = 20;
    int batchSize = 10;
    MockTableDef.MockColumn cols[] = new MockTableDef.MockColumn[] {
        new MockTableDef.MockColumn("a", MinorType.INT, DataMode.REQUIRED, null, null, null, null, null, null ),
        new MockTableDef.MockColumn("b", MinorType.VARCHAR, DataMode.REQUIRED, 10, null, null, null, null, null )
    };
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, batchSize, null, cols);
    RowReader reader = new ExtendedMockRecordReader(entry);
    List<RowReader> readers = Lists.newArrayList(reader);

    // Create options and the scan operator

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());

    // Next call, return with data, limited by batch size.

    assertTrue(scan.next());
    assertEquals(batchSize, scan.batchAccessor().getRowCount());
    scan.batchAccessor().release();

    assertTrue(scan.next());
    assertEquals(batchSize, scan.batchAccessor().getRowCount());
    scan.batchAccessor().release();

    // EOF

    assertFalse(scan.next());
    mockBatch.close();
  }

  /**
   * Test a mock varchar column large enough to cause vector overflow.
   */

  @Test
  public void testOverflow() {
    int rowCount = ValueVector.MAX_ROW_COUNT;
    MockTableDef.MockColumn cols[] = new MockTableDef.MockColumn[] {
        new MockTableDef.MockColumn("a", MinorType.INT, DataMode.REQUIRED, null, null, null, null, null, null ),
        new MockTableDef.MockColumn("b", MinorType.VARCHAR, DataMode.REQUIRED, 1000, null, null, null, null, null )
    };
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, null, null, cols);
    RowReader reader = new ExtendedMockRecordReader(entry);
    List<RowReader> readers = Lists.newArrayList(reader);

    // Create options and the scan operator

    MockBatch mockBatch = new MockBatch(readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());

    // Next call, return with data, limited by batch size.

    while(scan.next()) {
      assertTrue(scan.batchAccessor().getRowCount() < ValueVector.MAX_ROW_COUNT);
      scan.batchAccessor().release();
    }

    assertEquals(ValueVector.MAX_ROW_COUNT, scan.getMutator().totalRowCount());
    assertTrue(scan.getMutator().batchCount() > 1);

    mockBatch.close();
  }
}
