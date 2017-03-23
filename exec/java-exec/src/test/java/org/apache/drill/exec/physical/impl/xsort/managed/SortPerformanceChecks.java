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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.util.concurrent.TimeUnit;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.xsort.managed.PriorityQueueCopierWrapper.BatchMerger;
import org.apache.drill.exec.physical.impl.xsort.managed.SortTestUtilities.CopierTester;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.OperatorFixture.OperatorFixtureBuilder;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.apache.drill.test.rowSet.SchemaBuilder;

import com.google.common.base.Stopwatch;

/**
 * Runs a variety of performance checks on the external sort operator.
 * Implemented as a Java executable because these tests do not fit
 * well into the Maven unit test framework.
 */
public class SortPerformanceChecks {

  public static void main(String[] args) {
    new SortPerformanceChecks().run();
  }

  private void run() {
    try {
      timeWideRowsGeneratedCopier();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      timeWideRowsGenericCopier();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      timeLargeBatch();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      timeWideRows();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public SingleRowSet makeWideRowSet(OperatorFixture fixture, int colCount, int rowCount) {
    SchemaBuilder builder = RowSetSchema.builder()
        .add("key", MinorType.INT);
    for (int i = 0; i < colCount; i++) {
      builder.add("col" + (i+1), MinorType.INT);
    }
    RowSetSchema schema = builder.build();
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer(rowCount);
    for (int i = 0; i < rowCount; i++) {
      writer.next();
      writer.set(0, i);
      for (int j = 0; j < colCount; j++) {
        writer.set(j + 1, i * 100_000 + j);
      }
    }
    writer.done();
    return rowSet;
  }

  Stopwatch timer = Stopwatch.createUnstarted();

  public long runWideRowsTest(OperatorFixture fixture, int colCount, int rowCount) throws Exception {

    CopierTester tester = new CopierTester(fixture) {
      @Override
      protected void verifyResults(BatchMerger merger, VectorContainer dest) {
        while (merger.next()) {
          ;
        }
      }
    };
    tester.addInput(makeWideRowSet(fixture, colCount, rowCount).toIndirect());
    timer.reset();
    timer.start();
    tester.run();
    timer.stop();
    return timer.elapsed(TimeUnit.MILLISECONDS);
  }

  public void timeWideRowsGeneratedCopier() throws Exception {
    OperatorFixtureBuilder builder = OperatorFixture.builder();
    builder.configBuilder()
      .put(ExecConstants.EXTERNAL_SORT_GENERIC_COPIER, false);
    try (OperatorFixture fixture = builder.build()) {
      long timeMs = runWideRowsTest(fixture, 1000, Character.MAX_VALUE);
      System.out.print("Generated copier, 1000 columns, time (ms): ");
      System.out.println(timeMs);
    }
  }

  public void timeWideRowsGenericCopier() throws Exception {
    OperatorFixtureBuilder builder = OperatorFixture.builder();
    builder.configBuilder()
      .put(ExecConstants.EXTERNAL_SORT_GENERIC_COPIER, true);
    try (OperatorFixture fixture = builder.build()) {
      long timeMs = runWideRowsTest(fixture, 1000, Character.MAX_VALUE);
      System.out.print("Generic copier, 1000 columns, time (ms): ");
      System.out.println(timeMs);
    }
  }

  // Run this to time batches. Best when run stand-alone as part
  // of a performance investigation.

  public void timeWideRows() throws Exception {
    TestSortImpl test = new TestSortImpl();
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      for (int i = 0; i < 5; i++) {
        test.runWideRowsTest(fixture, 1000, Character.MAX_VALUE);
      }
      long total = 0;
      for (int i = 0; i < 5; i++) {
        test.runWideRowsTest(fixture, 1000, Character.MAX_VALUE);
        total += test.timer.elapsed(TimeUnit.MILLISECONDS);
      }
      System.out.print("Wide rows, sort impl time (ms): ");
      System.out.println(total / 5);
    }
  }

  // Run this to time batches. Best when run stand-alone as part
  // of a performance investigation.

  public void timeLargeBatch() throws Exception {
    TestSortImpl test = new TestSortImpl();
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      for (int i = 0; i < 5; i++) {
        test.runJumboBatchTest(fixture, Character.MAX_VALUE);
      }
      long total = 0;
      for (int i = 0; i < 5; i++) {
        test.runJumboBatchTest(fixture, Character.MAX_VALUE);
        total += test.timer.elapsed(TimeUnit.MILLISECONDS);
      }
      System.out.print("Large batch time (ms): ");
      System.out.println(total / 5);
    }
  }
}
