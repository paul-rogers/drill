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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

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
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
    for (RowSet expectedSet : expected) {
      assertTrue(results.next());
      RowSet rowSet;
      if (results.getSv4() == null) {
        rowSet = new DirectRowSet(fixture.allocator(), dest);
      } else {
        rowSet = new HyperRowSetImpl(fixture.allocator(), dest, results.getSv4());
      }
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

  @Test
  public void testSingleBatch() {
    fail("Not yet implemented");
  }

}
