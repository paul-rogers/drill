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
package org.apache.drill.exec.physical.rowSet;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.rowSet.impl.RowSetMutatorImpl;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.SchemaBuilder;

import io.netty.buffer.DrillBuf;

public class CheckLoaderPerformance {

  public static class TestTimer {

    private long start;
    private long end;

    public void start() {
      start = System.currentTimeMillis();
    }

    public void end() {
      end = System.currentTimeMillis();
    }

    public long elapsed() {
      return end - start;
    }

    public void report(String label) {
      System.out.println(label + ": " + elapsed() + " ms.");
    }
  }

  public static final int BATCH_COUNT = 1000;
  public static final int BATCH_SIZE = ValueVector.MAX_ROW_COUNT;

  protected static OperatorFixture fixture;
  public static int targetBatches = BATCH_COUNT;
  public static int targetRows = BATCH_SIZE;

  public static void main(String[] args) {
    fixture = OperatorFixture.standardFixture();
    // Warm the JVM

    System.out.println( "Warm-up phase");
    timeOriginalWriter(targetBatches, targetRows);
    timeRevisedWriter(targetBatches, targetRows);
    timeRevisedWriterRequired(targetBatches, targetRows);

    // Do the test
    System.out.println( "Test phase");
    timeOriginalWriter(targetBatches, targetRows);
    timeRevisedWriter(targetBatches, targetRows);
    timeRevisedWriterRequired(targetBatches, targetRows);
    try {
      fixture.close();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public abstract static class BaseWriterFixture {

    public int batchTarget;
    public int rowTarget;
    public int batchCount;
    public int rowCount;
    public int totalRowCount;
    public String label;

    public BaseWriterFixture(int batchCount, int rowCount, String label) {
      this.batchTarget = batchCount;
      this.rowTarget = rowCount;
      this.label = label;
    }

    public void runTest() {
      TestTimer timer = new TestTimer();
      timer.start();
      for (int i = 0; i < batchTarget; i++) {
        loadBatch();
        batchCount++;
      }
      timer.end();
      timer.report(label + ", " + totalRowCount + " rows");
    }

    protected abstract void loadBatch();

  }

  public abstract static class OriginalWriterFixture extends BaseWriterFixture {

    VectorContainer container;
    OutputMutator output;
    DrillBuf buffer;
    VectorContainerWriter writer;

    public OriginalWriterFixture(int batchCount, int rowCount) {
      super(batchCount, rowCount, "Original writer");
      OperatorExecContext oContext = fixture.operatorContext(null);

      // Setup: normally done by ScanBatch

      container = new VectorContainer(fixture.allocator());
      output = new ScanBatch.Mutator(oContext, fixture.allocator(), container);
      buffer = oContext.getManagedBuffer();
      writer = new VectorContainerWriter(output);
    }

    @Override
    public void runTest() {
      super.runTest();
      buffer.release();
    }

    @Override
    public void loadBatch() {
//      System.out.println("Before alloc: " + fixture.allocator().getAllocatedMemory());
      writer.allocate();
//      System.out.println("After alloc: " + fixture.allocator().getAllocatedMemory());
      writer.reset();
//      System.out.println("After reset: " + fixture.allocator().getAllocatedMemory());
      @SuppressWarnings("resource")
      BaseWriter.MapWriter map = writer.rootAsMap();

      rowCount = 0;
      for (int i = 0; i < rowTarget; i++) {
        loadRow(map);
        rowCount++;
        totalRowCount++;
      }

      // Wrap-up done by ScanBatch

//      System.out.println("After load: " + fixture.allocator().getAllocatedMemory());
      container.setRecordCount(rowCount);
      container.buildSchema(SelectionVectorMode.NONE);

      // Discard the container just created.

      container.zeroVectors();
//      System.out.println("After clear: " + fixture.allocator().getAllocatedMemory());
    }

    protected abstract void loadRow(MapWriter map);
  }

  private static void timeOriginalWriter(int batchTarget, int rowTarget) {

    OriginalWriterFixture fixture = new OriginalWriterFixture(batchTarget, rowTarget) {

      @Override
      protected void loadRow(MapWriter map) {
        map.integer("a").writeInt(totalRowCount);
      }

    };
    fixture.runTest();
  }

  public abstract static class RevisedWriterFixture extends BaseWriterFixture {

    RowSetMutator rsMutator;

    public RevisedWriterFixture(int batchCount, int rowCount) {
      super(batchCount, rowCount, "Revised writer");
      rsMutator = new RowSetMutatorImpl(fixture.allocator());
      TupleLoader rootWriter = rsMutator.writer();
      TupleSchema schema = rootWriter.schema();
      defineSchema(schema);
    }

    protected abstract void defineSchema(TupleSchema schema);

    @Override
    public void runTest() {
      super.runTest();
      rsMutator.close();
    }

    @Override
    protected void loadBatch() {
      rsMutator.start();
      TupleLoader rootWriter = rsMutator.writer();
      rowCount = 0;
      for (int i = 0; i < rowTarget; i++) {
        loadRow(rootWriter);
        rowCount++;
        totalRowCount++;
      }
      rsMutator.harvest().clear();
    }

    protected abstract void loadRow(TupleLoader rootWriter);
  }

  private static void timeRevisedWriter(int batchTarget, int rowTarget) {
    RevisedWriterFixture fixture = new RevisedWriterFixture(batchTarget, rowTarget) {

      @Override
      protected void defineSchema(TupleSchema schema) {
        // Use optional to be same as original writer
        schema.addColumn(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.OPTIONAL));
      }

      @Override
      protected void loadRow(TupleLoader rootWriter) {
        rootWriter.column(0).setInt(totalRowCount);
      }

    };
    fixture.runTest();
  }

  private static void timeRevisedWriterRequired(int batchTarget, int rowTarget) {
    RevisedWriterFixture fixture = new RevisedWriterFixture(batchTarget, rowTarget) {

      @Override
      protected void defineSchema(TupleSchema schema) {
        // Use optional to be same as original writer
        schema.addColumn(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED));
      }

      @Override
      protected void loadRow(TupleLoader rootWriter) {
        rootWriter.column(0).setInt(totalRowCount);
      }

    };
    System.out.println("Using required mode");
    fixture.runTest();
  }
}
