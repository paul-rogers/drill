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
package org.apache.drill.exec.store.mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.BatchAccessor;
import org.apache.drill.exec.physical.impl.scan.BaseScanOperatorExecTest.BaseScanFixtureBuilder;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ScanFixture;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.metadata.ColumnBuilder;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the mock data source directly by wrapping it in a mock
 * scan operator, without the rest of Drill.
 */

@Category({RowSetTests.class, UnlikelyTest.class})
public class TestMockRowReader extends SubOperatorTest {

  private static ScanFixture buildScan(MockSubScan config, List<ManagedReader<SchemaNegotiator>> readers) {
    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    List<SchemaPath> projList = new ArrayList<>();
    projList.add(SchemaPath.STAR_COLUMN);
    builder.setProjection(projList);
    builder.addReaders(readers);
    return builder.build();
  }

  /**
   * Test the most basic case: required integers and strings.
   */

  @Test
  public void testBasics() {
    int rowCount = 10;
    ColumnMetadata cols[] = new ColumnMetadata[] {
        ColumnBuilder.required("a", MinorType.INT),
        ColumnBuilder.builder("b", MinorType.VARCHAR, DataMode.REQUIRED).precision(10).build()
    };
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, null, null, cols);
    MockSubScan config = new MockSubScan(true, Collections.singletonList(entry));

    ManagedReader<SchemaNegotiator> reader = new ExtendedMockBatchReader(entry);
    List<ManagedReader<SchemaNegotiator>> readers = Collections.singletonList(reader);

    // Create options and the scan operator

    ScanFixture mockBatch = buildScan(config, readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR, 10) // Width is reflected in meta-data
        .buildSchema();
    BatchSchema expectedBatchSchema = new BatchSchema(SelectionVectorMode.NONE, expectedSchema.toFieldList());
    assertTrue(expectedBatchSchema.isEquivalent(scan.batchAccessor().schema()));
    assertEquals(0, scan.batchAccessor().rowCount());
    scan.batchAccessor().release();

    // Next call, return with data.

    assertTrue(scan.next());
    assertTrue(expectedBatchSchema.isEquivalent(scan.batchAccessor().schema()));
    assertEquals(rowCount, scan.batchAccessor().rowCount());
    scan.batchAccessor().release();

    // EOF

    assertFalse(scan.next());
    mockBatch.close();
  }

  /**
   * Verify that the mock reader can generate nullable (optional) columns,
   * including filling values with nulls at some percentage, 25% by
   * default.
   */

  @Test
  public void testOptional() {
    int rowCount = 10;
    ColumnMetadata cols[] = new ColumnMetadata[] {
        ColumnBuilder.nullable("a", MinorType.INT),
        ColumnBuilder.builder("b", MinorType.VARCHAR, DataMode.OPTIONAL).precision(10).build()
    };
    cols[1].setIntProperty(MockTableDef.NULL_RATE_PROP, 50);
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, null, null, cols);
    MockSubScan config = new MockSubScan(true, Collections.singletonList(entry));
    ManagedReader<SchemaNegotiator> reader = new ExtendedMockBatchReader(entry);
    List<ManagedReader<SchemaNegotiator>> readers = Collections.singletonList(reader);

    // Create options and the scan operator

    ScanFixture mockBatch = buildScan(config, readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .build();
    BatchSchema expectedBatchSchema = new BatchSchema(SelectionVectorMode.NONE, expectedSchema.toFieldList());
    assertTrue(expectedBatchSchema.isEquivalent(scan.batchAccessor().schema()));
    assertEquals(0, scan.batchAccessor().rowCount());

    // Next call, return with data.

    assertTrue(scan.next());
    assertTrue(expectedBatchSchema.isEquivalent(scan.batchAccessor().schema()));
    assertEquals(rowCount, scan.batchAccessor().rowCount());
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
    ColumnMetadata cols[] = new ColumnMetadata[] {
        ColumnBuilder.required("a", MinorType.INT),
        ColumnBuilder.builder("b", MinorType.VARCHAR, DataMode.REQUIRED).precision(10).build()
    };
    cols[0].setIntProperty(MockTableDef.REPEAT_PROP, 3);
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, null, null, cols);
    MockSubScan config = new MockSubScan(true, Collections.singletonList(entry));

    ManagedReader<SchemaNegotiator> reader = new ExtendedMockBatchReader(entry);
     List<ManagedReader<SchemaNegotiator>> readers = Collections.singletonList(reader);

    // Create options and the scan operator

    ScanFixture mockBatch = buildScan(config, readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a1", MinorType.INT)
        .add("a2", MinorType.INT)
        .add("a3", MinorType.INT)
        .add("b", MinorType.VARCHAR, 10)
        .build();
    BatchSchema expectedBatchSchema = new BatchSchema(SelectionVectorMode.NONE, expectedSchema.toFieldList());
    assertTrue(expectedBatchSchema.isEquivalent(scan.batchAccessor().schema()));
    assertEquals(0, scan.batchAccessor().rowCount());

    // Next call, return with data.

    assertTrue(scan.next());
    assertTrue(expectedBatchSchema.isEquivalent(scan.batchAccessor().schema()));
    assertEquals(rowCount, scan.batchAccessor().rowCount());
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
    ColumnMetadata cols[] = new ColumnMetadata[] {
        ColumnBuilder.required("a", MinorType.INT),
        ColumnBuilder.builder("b", MinorType.VARCHAR, DataMode.REQUIRED).precision(10).build()
    };
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, batchSize, null, cols);
    MockSubScan config = new MockSubScan(true, Collections.singletonList(entry));

    ManagedReader<SchemaNegotiator> reader = new ExtendedMockBatchReader(entry);
    List<ManagedReader<SchemaNegotiator>> readers = Collections.singletonList(reader);

    // Create options and the scan operator

    ScanFixture mockBatch = buildScan(config, readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    assertEquals(0, scan.batchAccessor().rowCount());

    // Next call, return with data, limited by batch size.

    assertTrue(scan.next());
    assertEquals(batchSize, scan.batchAccessor().rowCount());
    scan.batchAccessor().release();

    assertTrue(scan.next());
    assertEquals(batchSize, scan.batchAccessor().rowCount());
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
    ColumnMetadata cols[] = new ColumnMetadata[] {
        ColumnBuilder.required("a", MinorType.INT),
        ColumnBuilder.builder("b", MinorType.VARCHAR, DataMode.REQUIRED).precision(10).build()
    };
    MockTableDef.MockScanEntry entry = new MockTableDef.MockScanEntry(rowCount, true, null, null, cols);
    MockSubScan config = new MockSubScan(true, Collections.singletonList(entry));

    ManagedReader<SchemaNegotiator> reader = new ExtendedMockBatchReader(entry);
    List<ManagedReader<SchemaNegotiator>> readers = Collections.singletonList(reader);

    // Create options and the scan operator

    ScanFixture mockBatch = buildScan(config, readers);
    ScanOperatorExec scan = mockBatch.scanOp;

    // First batch: build schema. The reader helps: it returns an
    // empty first batch.

    assertTrue(scan.buildSchema());
    assertEquals(0, scan.batchAccessor().rowCount());

    // Next call, return with data, limited by batch size.

    int totalRowCount = 0;
    int batchCount = 0;
    while(scan.next()) {
      assertTrue(scan.batchAccessor().rowCount() < ValueVector.MAX_ROW_COUNT);
      BatchAccessor batchAccessor = scan.batchAccessor();
      totalRowCount += batchAccessor.rowCount();
      batchCount++;
      batchAccessor.release();
    }

    assertEquals(ValueVector.MAX_ROW_COUNT, totalRowCount);
    assertTrue(batchCount > 1);

    mockBatch.close();
  }
}
