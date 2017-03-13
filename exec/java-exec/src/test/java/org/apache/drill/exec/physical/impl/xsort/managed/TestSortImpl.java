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

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.ops.OperExecContext;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.physical.impl.xsort.managed.SortImpl.SortResults;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.HyperRowSetImpl;
import org.apache.drill.test.rowSet.IndirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class TestSortImpl extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
  }

  public SortImpl makeSortImpl(String sortOrder, String nullOrder, VectorContainer outputBatch) {
    FieldReference expr = FieldReference.getWithQuotedRef("key");
    Ordering ordering = new Ordering(sortOrder, expr, nullOrder);
    Sort popConfig = new Sort(null, Lists.newArrayList(ordering), false);
    OperExecContext opContext = fixture.newOperExecContext(popConfig);
    QueryId queryId = QueryId.newBuilder()
        .setPart1(1234)
        .setPart2(5678)
        .build();
    FragmentHandle handle = FragmentHandle.newBuilder()
          .setMajorFragmentId(2)
          .setMinorFragmentId(3)
          .setQueryId(queryId)
          .build();
    SpillSet spillSet = new SpillSet(opContext.getConfig(), handle,
                                     popConfig, "sort", "run");
    SpilledRuns spilledRuns = new SpilledRuns(opContext, spillSet);
    return new SortImpl(opContext, spilledRuns, outputBatch);
  }

  public void runSortTest(List<RowSet> rowSets, List<RowSet> expected) {
    runSortTest(rowSets, expected, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED);
  }

  public void runSortTest(List<RowSet> inputSets, List<RowSet> expected,
                            String sortOrder, String nullOrder) {
    VectorContainer dest = new VectorContainer();
    SortImpl sort = makeSortImpl(sortOrder, nullOrder, dest);

    // Simulates a NEW_SCHEMA event

    if (! inputSets.isEmpty()) {
      sort.setSchema(inputSets.get(0).getContainer().getSchema());
    }

    // Simulates an OK event

    for (RowSet input : inputSets) {
      sort.addBatch(input.getVectorAccessible());
    }

    // Simulate returning results

    SortResults results = sort.startMerge();
    if (results.getContainer() != dest) {
      dest.clear();
      dest = results.getContainer();
    }
    for (RowSet expectedSet : expected) {
      assertTrue(results.next());
      RowSet rowSet = toRowSet(results, dest);
      System.out.println("Expected:");
      expectedSet.print();
      System.out.println("Actual:");
      rowSet.print();
      new RowSetComparison(expectedSet)
            .verify(rowSet);
      expectedSet.clear();
    }
    assertFalse(results.next());
    results.close();
    dest.clear();
    sort.close();
  }

  private RowSet toRowSet(SortResults results, VectorContainer dest) {
    if (results.getSv4() != null) {
      return new HyperRowSetImpl(fixture.allocator(), dest, results.getSv4());
    } else if (results.getSv2() != null) {
      return new IndirectRowSet(fixture.allocator(), dest, results.getSv2());
    } else {
      return new DirectRowSet(fixture.allocator(), dest);
    }
  }

  /**
   * Test for null input (no input batches). Note that, in this case,
   * we never see a schema.
   * @throws Exception
   */

  @Test
  public void testNullInput() {
    runSortTest(Lists.newArrayList(), Lists.newArrayList());
  }

  /**
   * Test for an input with a schema, but only an empty input batch.
   */

  @Test
  public void testEmptyInput() {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    RowSet input = fixture.rowSetBuilder(schema)
        .build();
    runSortTest(Lists.newArrayList(input), Lists.newArrayList());
  }

  /**
   * Degenerate case: single row in single batch.
   */

  @Test
  public void testSingleRow() {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    RowSet input = fixture.rowSetBuilder(schema)
        .add(1, "first")
        .build();
    RowSet expected = fixture.rowSetBuilder(schema)
        .add(1, "first")
        .build();
    runSortTest(Lists.newArrayList(input), Lists.newArrayList(expected));
  }

  /**
   * Degenerate case: two (unsorted) rows in single batch
   */

  @Test
  public void testSingleBatch() {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    RowSet input1 = fixture.rowSetBuilder(schema)
        .add(2, "second")
        .add(1, "first")
        .build();
    RowSet expected = fixture.rowSetBuilder(schema)
        .add(1, "first")
        .add(2, "second")
        .build();
    runSortTest(Lists.newArrayList(input1), Lists.newArrayList(expected));
  }

  /**
   * Degenerate case, one row in each of two
   * (unsorted) batches.
   */

  @Test
  public void testTwoBatches() {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    RowSet input1 = fixture.rowSetBuilder(schema)
        .add(2, "second")
        .build();
    RowSet input2 = fixture.rowSetBuilder(schema)
        .add(1, "first")
        .build();
    RowSet expected = fixture.rowSetBuilder(schema)
        .add(1, "first")
        .add(2, "second")
        .build();
    runSortTest(Lists.newArrayList(input1, input2), Lists.newArrayList(expected));
  }

  public static class DataGenerator {
    private final OperatorFixture fixture;
    private final RowSetSchema schema;
    private final int targetCount;
    private final int batchSize;
    private final int step;
    private int rowCount;
    private int currentValue;

    public DataGenerator(OperatorFixture fixture, int targetCount, int batchSize) {
      this(fixture, targetCount, batchSize, 0, guessStep(targetCount));
    }

    public DataGenerator(OperatorFixture fixture, int targetCount, int batchSize, int seed, int step) {
      this.fixture = fixture;
      this.targetCount = targetCount;
      this.batchSize = Math.min(batchSize, Character.MAX_VALUE);
      this.step = step;
      schema = SortTestUtilities.nonNullSchema();
      currentValue = seed;
    }

    private static int guessStep(int target) {
      if (target < 10) {
        return 7;
      } else if (target < 200) {
        return 71;
      } else if (target < 2000) {
        return 701;
      } else if (target < 20000) {
        return 7001;
      } else {
        return 17011;
      }
    }

    public RowSet nextRowSet() {
      if (rowCount == targetCount) {
        return null;
      }
      RowSetBuilder builder = fixture.rowSetBuilder(schema);
      int end = Math.min(batchSize, targetCount - rowCount);
      for (int i = 0; i < end; i++) {
        builder.add(currentValue, i + ", " + currentValue);
        currentValue = (currentValue + step) % targetCount;
        rowCount++;
      }
      return builder.build();
    }
  }

  public static class DataValidator {
    private final int targetCount;
    private final int batchSize;
    private int batchCount;
    private int rowCount;

    public DataValidator(int targetCount, int batchSize) {
      this.targetCount = targetCount;
      this.batchSize = Math.min(batchSize, Character.MAX_VALUE);
    }

    public void validate(RowSet output) {
      batchCount++;
      int expectedSize = Math.min(batchSize, targetCount - rowCount);
      assertEquals("Size of batch " + batchCount, expectedSize, output.rowCount());
      RowSetReader reader = output.reader();
      while (reader.next()) {
        assertEquals("Value of " + batchCount + ":" + rowCount,
            rowCount, reader.column(0).getInt());
        rowCount++;
      }
    }

    public void validateDone() {
      assertEquals("Wrong row count", targetCount, rowCount);
    }
  }

  Stopwatch timer = Stopwatch.createUnstarted();

  public void runLargeSortTest(DataGenerator dataGen, DataValidator validator) {
    VectorContainer dest = new VectorContainer();
    SortImpl sort = makeSortImpl(Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED, dest);

    int batchCount = 0;
    RowSet input;
    while ((input = dataGen.nextRowSet()) != null) {
      batchCount++;
      if (batchCount == 1) {
        // Simulates a NEW_SCHEMA event

        timer.start();
        sort.setSchema(input.getContainer().getSchema());
        timer.stop();
      }

      // Simulates an OK event

      timer.start();
      sort.addBatch(input.getVectorAccessible());
      timer.stop();
    }

    // Simulate returning results

    timer.start();
    SortResults results = sort.startMerge();
    if (results.getContainer() != dest) {
      dest.clear();
      dest = results.getContainer();
    }
    while (results.next()) {
      timer.stop();
      RowSet output = toRowSet(results, dest);
      validator.validate(output);
      timer.start();
    }
    timer.stop();
    validator.validateDone();
    results.close();
    dest.clear();
    sort.close();
  }

  @Test
  public void testModerateBatch() {
    timer.reset();
    DataGenerator dataGen = new DataGenerator(fixture, 1000, Character.MAX_VALUE);
    DataValidator validator = new DataValidator(1000, Character.MAX_VALUE);
    runLargeSortTest(dataGen, validator);
    System.out.println(timer.elapsed(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLargeBatch() {
    timer.reset();
    DataGenerator dataGen = new DataGenerator(fixture, Character.MAX_VALUE, Character.MAX_VALUE);
    DataValidator validator = new DataValidator(Character.MAX_VALUE, Character.MAX_VALUE);
    runLargeSortTest(dataGen, validator);
    System.out.println(timer.elapsed(TimeUnit.MILLISECONDS));
  }

  @Test
  public void timeLargeBatch() {
    for (int i = 0; i < 5; i++) {
      testLargeBatch();
    }
    long total = 0;
    for (int i = 0; i < 5; i++) {
      testLargeBatch();
      total += timer.elapsed(TimeUnit.MILLISECONDS);
    }
    System.out.print("Average: ");
    System.out.println(total / 5);
  }
}
