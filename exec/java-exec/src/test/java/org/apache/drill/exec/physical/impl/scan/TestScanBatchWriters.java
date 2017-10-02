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
 ******************************************************************************/
package org.apache.drill.exec.physical.impl.scan;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import io.netty.buffer.DrillBuf;

public class TestScanBatchWriters extends SubOperatorTest {

  @Test
  public void sanityTest() throws Exception {
    OperatorExecContext oContext = fixture.operatorContext(null);

    // Setup: normally done by ScanBatch

    VectorContainer container = new VectorContainer(fixture.allocator());
    OutputMutator output = new ScanBatch.Mutator(oContext, fixture.allocator(), container);
    @SuppressWarnings("resource")
    DrillBuf buffer = oContext.getManagedBuffer();

    // One-time setup

    try (VectorContainerWriter writer = new VectorContainerWriter(output)) {

      // Per-batch

      writer.allocate();
      writer.reset();
      @SuppressWarnings("resource")
      BaseWriter.MapWriter map = writer.rootAsMap();

      // Write one record (10, "Fred", [100, 110, 120] )

      map.integer("a").writeInt(10);
      byte[] bytes = "Fred".getBytes("UTF-8");
      buffer.setBytes(0, bytes, 0, bytes.length);
      map.varChar("b").writeVarChar(0, bytes.length, buffer);
      try (ListWriter list = map.list("c")) {
        list.startList();
        list.integer().writeInt(100);
        list.integer().writeInt(110);
        list.integer().writeInt(120);
        list.endList();

        // Write another record: (20, "Wilma", [])

        writer.setPosition(1);
        map.integer("a").writeInt(20);
        bytes = "Wilma".getBytes("UTF-8");
        buffer.setBytes(0, bytes, 0, bytes.length);
        map.varChar("b").writeVarChar(0, bytes.length, buffer);
        writer.setValueCount(2);

        // Wrap-up done by ScanBatch

        container.setRecordCount(2);
        container.buildSchema(SelectionVectorMode.NONE);

        RowSet rowSet = fixture.wrap(container);

        // Expected

        BatchSchema schema = new SchemaBuilder()
            .add("a", MinorType.INT)
            .add("b", MinorType.VARCHAR)
            .addArray("c", MinorType.INT)
            .build();
        RowSet expected = fixture.rowSetBuilder(schema)
            .addRow(10, "Fred", new int[] { 100, 110, 120 } )
            .addRow(20, "Wilma", null)
            .build();

        new RowSetComparison(expected)
          .verifyAndClearAll(rowSet);
      }
    }

    oContext.close();
  }
}
