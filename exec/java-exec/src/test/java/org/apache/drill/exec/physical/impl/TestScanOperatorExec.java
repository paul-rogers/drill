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

import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperExecContext;
import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.physical.impl.ScanOperatorExec.ScanOptions;
import org.apache.drill.exec.physical.rowSet.RowSetMutator;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RowReader;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import jersey.repackaged.com.google.common.collect.Lists;

public class TestScanOperatorExec extends SubOperatorTest {

  private static class MockRowReader implements RowReader {

    public boolean openCalled;
    public boolean closeCalled;
    private RowSetMutator mutator;
    public int batchLimit;
    public int batchCount;

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
        return true;
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

  @Test
  public void testNormalLifecycle() {
    MaterializedField b = new SchemaBuilder.ColumnBuilder("b", MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .setWidth(10)
        .build();
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add(b)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(10, "fred")
        .add(20, "wilma")
        .build();
    RowSetComparison verifier = new RowSetComparison(expected);

    MockRowReader reader = new MockRowReader();
    reader.batchLimit = 2;
    List<RowReader> readers = Lists.newArrayList(reader);
    ScanOptions options = new ScanOptions();
    options.implicitColumns = null;
    ScanOperatorExec scan = new ScanOperatorExec(fixture.operatorContext(null), readers.iterator(), options);
    scan.bind();
    scan.start();
    assertFalse(reader.openCalled);

    assertTrue(scan.buildSchema());
    assertTrue(reader.openCalled);
    assertEquals(expectedSchema, scan.batchAccessor().getSchema());
    assertEquals(0, scan.batchAccessor().getRowCount());

    assertTrue(scan.next());
    verifier.verify(fixture.wrap(scan.batchAccessor().getOutgoingContainer()));
    scan.batchAccessor().getOutgoingContainer().clear();

    assertFalse(scan.next());
    assertTrue(reader.closeCalled);

    scan.close();
    expected.clear();
  }

}
