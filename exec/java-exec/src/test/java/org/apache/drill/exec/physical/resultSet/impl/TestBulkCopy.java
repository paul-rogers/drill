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
package org.apache.drill.exec.physical.resultSet.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.IndirectOutgoingAccessor;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.selection.SelectionVector2Builder;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

public class TestBulkCopy extends SubOperatorTest {

  private static final TupleMetadata TEST_SCHEMA =
      new SchemaBuilder()
        .add("id", MinorType.INT)
        .add("name", MinorType.VARCHAR)
        .build();
  private static class SparseFilterDataGen {
    protected final ResultSetLoader rsLoader;
    protected final IndirectOutgoingAccessor batch;
    private int rowCount;

    public SparseFilterDataGen() {
      ResultSetOptions options = new OptionBuilder()
          .setSchema(TEST_SCHEMA)
          .setVectorCache(new ResultVectorCacheImpl(fixture.allocator()))
          .build();
      rsLoader = new ResultSetLoaderImpl(options);
      batch = new IndirectOutgoingAccessor(rsLoader.outputContainer());
    }

    // Get a set of non-overflowed batches that will, however,
    // cause the copier to overflow due to no row limit, and
    // odd row sizes.

    public void next(int batchSize, boolean full) {
      rsLoader.startBatch();
      for (int i = 0; i < batchSize; i++, rowCount++) {
        rsLoader.writer().addRow(rowCount, "Row " + rowCount);
      }
      rsLoader.harvestOutput();
      VectorContainer container = rsLoader.outputContainer();
      SelectionVector2Builder sv2Builder =
          new SelectionVector2Builder(container);
      if (full) {
        for (int i = 0; i < batchSize; i++) {
          sv2Builder.setNext(i);
        }
      } else {
        for (int i = 0; i < batchSize/2; i++) {
          sv2Builder.setNext(i * 2);
        }
      }
      container.buildSchema(SelectionVectorMode.TWO_BYTE);
      batch.registerBatch(sv2Builder.build());
    }
  }

  final int bigInputSize = 100;
  final int outputSize = 60;
  final int minBulkCopySize = outputSize / 2 + 1;

  /**
   * Test an initial bulk copy batch larger than the
   * target row size. Immediately fills the output batch.
   */

  @Test
  public void testLargeBulkCopyOnFirst() {

    SparseFilterDataGen dataGen = new SparseFilterDataGen();
    OptionBuilder options = new OptionBuilder()
        .setRowCountLimit(outputSize);
    ResultSetCopierImpl copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batch, options);
    copier.allowDirectTransfer(true);

    // Start: do a bulk copy: no SV and batch size > target/2

    copier.startOutputBatch();
    dataGen.next(bigInputSize, true);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertTrue(copier.isOutputFull());
    copier.harvestOutput();
    assertEquals(1, copier.bulkCopyCount());
    VectorContainer container = copier.outputContainer();
    assertEquals(bigInputSize, container.getRecordCount());
    container.zeroVectors();

    // Filtered batch. Regular copy.

    copier.startOutputBatch();
    copier.releaseInputBatch();
    dataGen.next(bigInputSize, false);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());
    assertEquals(1, copier.bulkCopyCount());

    copier.harvestOutput();
    container = copier.outputContainer();
    assertEquals(bigInputSize / 2, container.getRecordCount());
    container.zeroVectors();

    copier.close();
  }

  /**
   * Test that a bulk copy leaves the result set loader in
   * a sane state to append more data in the second batch.
   */

  @Test
  public void testBulkCopyAndAppend() {

    SparseFilterDataGen dataGen = new SparseFilterDataGen();
    OptionBuilder options = new OptionBuilder()
        .setRowCountLimit(outputSize);
    ResultSetCopierImpl copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batch, options);
    copier.allowDirectTransfer(true);

    // Large enough for bulk copy, smaller than full batch

    copier.startOutputBatch();
    dataGen.next(minBulkCopySize, true);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());
    assertEquals(1, copier.bulkCopyCount());
    assertFalse(copier.isCopyPending());

    // We can now append a small batch on top of the bulk
    // copy. Use a filtered batch. Actual rows are half
    // the batch size, which is half the copier output
    // batch size. So, can add rows to those above.

    copier.releaseInputBatch();
    dataGen.next(minBulkCopySize, false);
    copier.registerInputBatch();
    int secondBatchSize = minBulkCopySize / 2;
    assertEquals(secondBatchSize, dataGen.batch.rowCount());
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());
    assertFalse(copier.isCopyPending());

    // Single batch, with second appended.

    copier.harvestOutput();
    VectorContainer container = copier.outputContainer();
    assertEquals(minBulkCopySize * 3 / 2, container.getRecordCount());

    // Sanity check of data. If something goes wonky with internal
    // writer state after the bulk copy, the following will likely
    // fail.

    RowSet result = fixture.wrap(container);
    RowSetReader reader = result.reader();
    int index = 0;
    for (int i = 0; i < minBulkCopySize; i++, index++) {
      reader.next();
      assertEquals(index, reader.scalar(0).getInt());
      assertEquals("Row " + index, reader.scalar(1).getString());
    }
    for (int i = 0; i < secondBatchSize; i++, index += 2) {
      reader.next();
      assertEquals(index, reader.scalar(0).getInt());
      assertEquals("Row " + index, reader.scalar(1).getString());
    }
    result.clear();

    copier.close();
  }

  /**
   * Test multiple bulk copies in a row.
   */
  @Test
  public void testMultipleBulkCopy() {

    SparseFilterDataGen dataGen = new SparseFilterDataGen();
    OptionBuilder options = new OptionBuilder()
        .setRowCountLimit(outputSize);
    ResultSetCopierImpl copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batch, options);
    copier.allowDirectTransfer(true);

    // Large enough for bulk copy, smaller than full batch

    copier.startOutputBatch();
    dataGen.next(minBulkCopySize, true);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());
    assertEquals(1, copier.bulkCopyCount());

    // Another. Forces flush of the above.

    copier.releaseInputBatch();
    // Change size, to catch missing updates
    int secondBatchSize = minBulkCopySize + 2;
    dataGen.next(secondBatchSize, true);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertTrue(copier.isOutputFull());
    assertEquals(2, copier.bulkCopyCount());
    copier.harvestOutput();
    VectorContainer container = copier.outputContainer();
    assertEquals(minBulkCopySize, container.getRecordCount());
    container.zeroVectors();
    assertTrue(copier.isCopyPending());

    // Another batch, flushes the above.

    copier.startOutputBatch();
    copier.releaseInputBatch();
    assertFalse(copier.isOutputFull());
    assertEquals(2, copier.bulkCopyCount());
    copier.harvestOutput();
    container = copier.outputContainer();
    assertEquals(secondBatchSize, container.getRecordCount());
    container.zeroVectors();
    assertFalse(copier.isCopyPending());

    copier.close();
  }

  /**
   * Bulk copy enabled, but batches are too small,
   * they are consolidated instead.
   */

  @Test
  public void testConsolidation() {

    SparseFilterDataGen dataGen = new SparseFilterDataGen();
    OptionBuilder options = new OptionBuilder()
        .setRowCountLimit(outputSize);
    ResultSetCopierImpl copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batch, options);
    copier.allowDirectTransfer(true);

    // Smaller than half output size. Bulk copy anyway because
    // this is the first batch.

    int smallBatchSize = minBulkCopySize - 2;
    copier.startOutputBatch();
    dataGen.next(smallBatchSize, true);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());
    assertEquals(1, copier.bulkCopyCount());

    // Another. Consolidated with the above.

    copier.releaseInputBatch();
    dataGen.next(smallBatchSize, true);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());
    assertEquals(1, copier.bulkCopyCount());

    // Another. Consolidated with above, causes
    // output batch.

    copier.releaseInputBatch();
    dataGen.next(smallBatchSize, true);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertTrue(copier.isOutputFull());
    assertEquals(1, copier.bulkCopyCount());
    assertTrue(copier.isCopyPending());
    copier.harvestOutput();
    VectorContainer container = copier.outputContainer();
    assertEquals(outputSize, container.getRecordCount());
    container.zeroVectors();
    assertTrue(copier.isCopyPending());

    // Finish the above.

    copier.startOutputBatch();
    copier.releaseInputBatch();
    assertFalse(copier.isOutputFull());
    copier.harvestOutput();
    container = copier.outputContainer();
    assertEquals(3 * smallBatchSize - outputSize, container.getRecordCount());
    container.zeroVectors();
    assertFalse(copier.isCopyPending());

    // Output is again empty. Bulk copy again on small batch.

    copier.startOutputBatch();
    dataGen.next(smallBatchSize, true);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());
    assertEquals(2, copier.bulkCopyCount());
    copier.harvestOutput();
    container = copier.outputContainer();
    assertEquals(smallBatchSize, container.getRecordCount());
    container.zeroVectors();

    copier.close();
  }

  /**
   * Bulk copy enabled, small batch. A large batch
   * comes along. Causes small in-flight batch to be
   * flushed.
   */

  @Test
  public void testBulkFlush() {

    SparseFilterDataGen dataGen = new SparseFilterDataGen();
    OptionBuilder options = new OptionBuilder()
        .setRowCountLimit(outputSize);
    ResultSetCopierImpl copier = new ResultSetCopierImpl(
        fixture.allocator(), dataGen.batch, options);
    copier.allowDirectTransfer(true);

    // No bulk copy because of incomplete selection.

    copier.startOutputBatch();
    dataGen.next(minBulkCopySize, false);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertFalse(copier.isOutputFull());
    assertEquals(0, copier.bulkCopyCount());

    // Large second batch: flushes the above.

    copier.releaseInputBatch();
    dataGen.next(minBulkCopySize, true);
    copier.registerInputBatch();
    copier.copyAllRows();
    assertTrue(copier.isOutputFull());
    assertEquals(1, copier.bulkCopyCount());

    // Harvest output: is just the first input

    copier.harvestOutput();
    VectorContainer container = copier.outputContainer();
    assertEquals(minBulkCopySize / 2, container.getRecordCount());
    container.zeroVectors();

    // Start and finish an output: is the second input batch

    assertTrue(copier.isCopyPending());
    copier.startOutputBatch();
    copier.harvestOutput();
    container = copier.outputContainer();
    assertEquals(minBulkCopySize, container.getRecordCount());
    container.zeroVectors();

    copier.close();
  }
}
