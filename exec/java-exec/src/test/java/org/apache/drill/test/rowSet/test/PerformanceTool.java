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
package org.apache.drill.test.rowSet.test;

import java.util.concurrent.TimeUnit;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessors.IntColumnWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.writer.NullableScalarWriter;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSetWriter;
import org.apache.drill.test.rowSet.SchemaBuilder;

import com.google.common.base.Stopwatch;

public class PerformanceTool {

  public static final int ROW_COUNT = 16 * 1024 * 1024 / 4;
  public static final int ITERATIONS = 300;

  public static void main(String args[]) {
    MaterializedField field = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    TupleMetadata rowSchema = new SchemaBuilder()
        .add(field)
        .buildSchema();
    try (OperatorFixture fixture = OperatorFixture.standardFixture();) {
      for (int i = 0; i < 2; i++) {
        timeVector(field, fixture);
        timeWriter(rowSchema, fixture);
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private static void timeVector(MaterializedField field, OperatorFixture fixture) {
    Stopwatch timer = Stopwatch.createUnstarted();
    for (int i = 0; i < ITERATIONS; i++) {
      testNullableVector(field, fixture, timer);
    }
    System.out.println("Vector: " + timer.elapsed(TimeUnit.MILLISECONDS));
  }

  private static void testVector(MaterializedField field, OperatorFixture fixture, Stopwatch timer) {
    try (IntVector vector = new IntVector(field, fixture.allocator());) {
      vector.allocateNew(4096);
      IntVector.Mutator mutator = vector.getMutator();
      timer.start();
      for (int i = 0; i < ROW_COUNT; i++) {
        mutator.setSafe(i, 1234);
      }
      timer.stop();
    }
  }

  private static void testNullableVector(MaterializedField field, OperatorFixture fixture, Stopwatch timer) {
    try (NullableIntVector vector = new NullableIntVector(field, fixture.allocator());) {
      vector.allocateNew(4096);
      NullableIntVector.Mutator mutator = vector.getMutator();
      timer.start();
      for (int i = 0; i < ROW_COUNT; i++) {
        mutator.setSafe(i, 1234);
      }
      timer.stop();
    }
  }

  private static void timeWriter(TupleMetadata rowSchema,
      OperatorFixture fixture) {
    Stopwatch timer = Stopwatch.createUnstarted();
    for (int i = 0; i < ITERATIONS; i++) {
      testNullableWriter(rowSchema, fixture, timer);
    }
    System.out.println("Writer: " + timer.elapsed(TimeUnit.MILLISECONDS));
  }

  private static class TestWriterIndex implements ColumnWriterIndex {

    public int index;

    @Override
    public int vectorIndex() { return index; }

    @Override
    public void overflowed() {
      throw new IllegalStateException();
    }

    @Override
    public boolean legal() { return true; }

    @Override
    public void nextElement() { index++; }
  }

  private static void testWriter(TupleMetadata rowSchema,
      OperatorFixture fixture, Stopwatch timer) {
    try (IntVector vector = new IntVector(rowSchema.column(0), fixture.allocator());) {
      vector.allocateNew(4096);
      IntColumnWriter colWriter = new IntColumnWriter();
      colWriter.bindVector(vector);
      TestWriterIndex index = new TestWriterIndex();
      colWriter.bindIndex(index);
      timer.start();
      while (index.index < ROW_COUNT) {
        colWriter.setInt(1234);
      }
      timer.stop();
    }
  }

  private static void testNullableWriter(TupleMetadata rowSchema,
      OperatorFixture fixture, Stopwatch timer) {
    try (NullableIntVector vector = new NullableIntVector(rowSchema.column(0), fixture.allocator());) {
      vector.allocateNew(4096);
      NullableScalarWriter colWriter = new NullableScalarWriter(new IntColumnWriter());
      colWriter.bindVector(vector);
      TestWriterIndex index = new TestWriterIndex();
      colWriter.bindIndex(index);
      timer.start();
      while (index.index < ROW_COUNT) {
        colWriter.setInt(1234);
      }
      timer.stop();
    }
  }

  private static void testWriter2(TupleMetadata rowSchema,
      OperatorFixture fixture, Stopwatch timer) {
    ExtendableRowSet rs = fixture.rowSet(rowSchema);
    RowSetWriter writer = rs.writer(4096);
    ScalarWriter colWriter = writer.scalar(0);
    timer.start();
    for (int i = 0; i < ROW_COUNT; i++) {
      colWriter.setInt(i);
      writer.save();
    }
    timer.stop();
    writer.done().clear();
  }
}
