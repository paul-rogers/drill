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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperExecContext;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.xsort.managed.PriorityQueueCopierWrapper.BatchMerger;
import org.apache.drill.exec.physical.impl.xsort.managed.SortImpl.SortResults;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.accessor.AccessorUtilities;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetSchema.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Light-weight sanity test of the copier class. The implementation has
 * been used in production, so the tests here just check for the obvious
 * cases.
 * <p>
 * Note, however, that if significant changes are made to the copier,
 * then additional tests should be added to re-validate the code.
 */

public class TestCopier extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setup() {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    fixture.close();
  }

  @Test
  public void testEmptyInput() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    List<BatchGroup> batches = new ArrayList<>();
    PriorityQueueCopierWrapper copier = makeCopier(Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED);
    VectorContainer dest = new VectorContainer();
    try {
      @SuppressWarnings({ "resource", "unused" })
      BatchMerger merger = copier.startMerge(schema.toBatchSchema(SelectionVectorMode.NONE),
                                             batches, dest, 10);
      fail();
    } catch (AssertionError e) {
      // Expected
    }
  }

  public PriorityQueueCopierWrapper makeCopier(String sortOrder, String nullOrder) {
    FieldReference expr = FieldReference.getWithQuotedRef("key");
    Ordering ordering = new Ordering(sortOrder, expr, nullOrder);
    Sort popConfig = new Sort(null, Lists.newArrayList(ordering), false);
    OperExecContext opContext = fixture.newOperExecContext(popConfig);
    return new PriorityQueueCopierWrapper(opContext);
  }

  public void runCopierTest(List<SingleRowSet> rowSets, List<SingleRowSet> expected) throws Exception {
    runCopierTest(rowSets, expected, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED);
  }

  public void runCopierTest(List<SingleRowSet> rowSets, List<SingleRowSet> expected,
                            String sortOrder, String nullOrder) throws Exception {
    PriorityQueueCopierWrapper copier = makeCopier(sortOrder, nullOrder);
    List<BatchGroup> batches = new ArrayList<>();
    RowSetSchema schema = null;
    for (SingleRowSet rowSet : rowSets) {
      batches.add(new BatchGroup.InputBatch(rowSet.getContainer(), rowSet.getSv2(),
                  fixture.allocator(), rowSet.getSize()));
      if (schema == null) {
        schema = rowSet.schema();
      }
    }
    int rowCount = expected.get(0).rowCount();
    if (rowCount == 0) { rowCount = 10; }
    VectorContainer dest = new VectorContainer();
    @SuppressWarnings("resource")
    BatchMerger merger = copier.startMerge(schema.toBatchSchema(SelectionVectorMode.NONE),
                                           batches, dest, rowCount);

    for (RowSet expectedSet : expected) {
      assertTrue(merger.next());
      RowSet rowSet = new DirectRowSet(fixture.allocator(), dest);
      new RowSetComparison(expectedSet)
            .verifyAndClear(rowSet);
    }
    assertFalse(merger.next());
    dest.clear();
    merger.close();
  }

  @Test
  public void testEmptyBatch() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    SingleRowSet input = fixture.rowSetBuilder(schema)
          .withSv2()
          .build();

    runCopierTest(Lists.newArrayList(input), Lists.newArrayList());
  }

  @Test
  public void testSingleRow() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    SingleRowSet input = fixture.rowSetBuilder(schema)
          .add(10, "10")
          .withSv2()
          .build();

    SingleRowSet expected = fixture.rowSetBuilder(schema)
          .add(10, "10")
          .build();
    runCopierTest(Lists.newArrayList(input), Lists.newArrayList(expected));
  }

  @Test
  public void testTwoBatchesSingleRow() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    SingleRowSet input1 = fixture.rowSetBuilder(schema)
          .add(10, "10")
          .withSv2()
          .build();
    SingleRowSet input2 = fixture.rowSetBuilder(schema)
          .add(20, "20")
          .withSv2()
          .build();

    SingleRowSet expected = fixture.rowSetBuilder(schema)
          .add(10, "10")
          .add(20, "20")
          .build();
    runCopierTest(Lists.newArrayList(input1, input2), Lists.newArrayList(expected));
  }

  public SingleRowSet makeDataSet(RowSetSchema schema, int first, int step, int count) {
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer(count);
    int value = first;
    for (int i = 0; i < count; i++, value += step) {
      AccessorUtilities.setFromInt(writer.column(0), value);
      writer.column(1).setString(Integer.toString(value));
      writer.next();
    }
    writer.done();
    return rowSet;
  }

  @Test
  public void testMultipleOutput() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();

    SingleRowSet input1 = makeDataSet(schema, 0, 2, 10).toIndirect();
    SingleRowSet input2 = makeDataSet(schema, 1, 2, 10).toIndirect();

    SingleRowSet output1 = makeDataSet(schema, 0, 1, 10);
    SingleRowSet output2 = makeDataSet(schema, 10, 1, 10);

    runCopierTest(Lists.newArrayList(input1, input2), Lists.newArrayList(output1, output2));
  }

  // Also verifies that SV2s work

  @Test
  public void testMultipleOutputDesc() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();

    SingleRowSet input1 = makeDataSet(schema, 0, 2, 10).toIndirect();
    RowSetUtilities.reverse(input1.getSv2());
    SingleRowSet input2 = makeDataSet(schema, 1, 2, 10).toIndirect();
    RowSetUtilities.reverse(input2.getSv2());

    SingleRowSet output1 = makeDataSet(schema, 19, -1, 10);
    SingleRowSet output2 = makeDataSet(schema, 9, -1, 10);

    runCopierTest(Lists.newArrayList(input1, input2), Lists.newArrayList(output1, output2),
                                     Ordering.ORDER_DESC, Ordering.NULLS_UNSPECIFIED);
  }

  @Test
  public void testAscNullsLast() throws Exception {
    RowSetSchema schema = SortTestUtilities.nullableSchema();

    SingleRowSet input1 = fixture.rowSetBuilder(schema)
        .add(1, "1")
        .add(4, "4")
        .add(null, "null")
        .withSv2()
        .build();
    SingleRowSet input2 = fixture.rowSetBuilder(schema)
        .add(2, "2")
        .add(3, "3")
        .add(null, "null")
        .withSv2()
        .build();

    SingleRowSet output = fixture.rowSetBuilder(schema)
        .add(1, "1")
        .add(2, "2")
        .add(3, "3")
        .add(4, "4")
        .add(null, "null")
        .add(null, "null")
        .build();

    runCopierTest(Lists.newArrayList(input1, input2), Lists.newArrayList(output),
                                     Ordering.ORDER_ASC, Ordering.NULLS_LAST);
  }

  @Test
  public void testAscNullsFirst() throws Exception {
    RowSetSchema schema = SortTestUtilities.nullableSchema();

    SingleRowSet input1 = fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(1, "1")
        .add(4, "4")
        .withSv2()
        .build();
    SingleRowSet input2 = fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(2, "2")
        .add(3, "3")
        .withSv2()
        .build();

    SingleRowSet output = fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(null, "null")
        .add(1, "1")
        .add(2, "2")
        .add(3, "3")
        .add(4, "4")
        .build();

    runCopierTest(Lists.newArrayList(input1, input2), Lists.newArrayList(output),
                                     Ordering.ORDER_ASC, Ordering.NULLS_FIRST);
  }

  @Test
  public void testDescNullsLast() throws Exception {
    RowSetSchema schema = SortTestUtilities.nullableSchema();

    SingleRowSet input1 = fixture.rowSetBuilder(schema)
        .add(4, "4")
        .add(1, "1")
        .add(null, "null")
        .withSv2()
        .build();
    SingleRowSet input2 = fixture.rowSetBuilder(schema)
        .add(3, "3")
        .add(2, "2")
        .add(null, "null")
        .withSv2()
        .build();

    SingleRowSet output = fixture.rowSetBuilder(schema)
        .add(4, "4")
        .add(3, "3")
        .add(2, "2")
        .add(1, "1")
        .add(null, "null")
        .add(null, "null")
        .build();

    runCopierTest(Lists.newArrayList(input1, input2), Lists.newArrayList(output),
                                     Ordering.ORDER_DESC, Ordering.NULLS_LAST);
  }

  @Test
  public void testDescNullsFirst() throws Exception {
    RowSetSchema schema = SortTestUtilities.nullableSchema();

    SingleRowSet input1 = fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(4, "4")
        .add(1, "1")
        .withSv2()
        .build();
    SingleRowSet input2 = fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(3, "3")
        .add(2, "2")
        .withSv2()
        .build();

    SingleRowSet output = fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(null, "null")
        .add(4, "4")
        .add(3, "3")
        .add(2, "2")
        .add(1, "1")
        .build();

    runCopierTest(Lists.newArrayList(input1, input2), Lists.newArrayList(output),
                                     Ordering.ORDER_DESC, Ordering.NULLS_FIRST);
  }

  public void runTypeTest(MinorType type) throws Exception {
    RowSetSchema schema = SortTestUtilities.makeSchema(type, false);

    SingleRowSet input1 = makeDataSet(schema, 0, 2, 5).toIndirect();
    SingleRowSet input2 = makeDataSet(schema, 1, 2, 5).toIndirect();

    SingleRowSet output1 = makeDataSet(schema, 0, 1, 10);

    runCopierTest(Lists.newArrayList(input1, input2), Lists.newArrayList(output1));
  }

  @Test
  public void testTypes() throws Exception {

    runTypeTest(MinorType.INT);
    runTypeTest(MinorType.BIGINT);
    runTypeTest(MinorType.FLOAT4);
    runTypeTest(MinorType.FLOAT8);
    runTypeTest(MinorType.DECIMAL9);
    runTypeTest(MinorType.DECIMAL18);
    runTypeTest(MinorType.VARCHAR);
    runTypeTest(MinorType.VARBINARY);
    runTypeTest(MinorType.DATE);
    runTypeTest(MinorType.TIME);
    runTypeTest(MinorType.TIMESTAMP);
    runTypeTest(MinorType.INTERVALYEAR);

    // Others not tested. See DRILL-5329
  }
  
  public void runWideRowsTest(OperatorFixture fixture, int colCount, int rowCount) {
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

    VectorContainer dest = new VectorContainer();
    SortImpl sort = makeSortImpl(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED, dest);
    timer.reset();
    timer.start();
    sort.setSchema(rowSet.getContainer().getSchema());
    sort.addBatch(rowSet.getVectorAccessible());
    SortResults results = sort.startMerge();
    if (results.getContainer() != dest) {
      dest.clear();
      dest = results.getContainer();
    }
    assertTrue(results.next());
    timer.stop();
    assertFalse(results.next());
    results.close();
    dest.clear();
    sort.close();
    System.out.println(timer.elapsed(TimeUnit.MILLISECONDS));
  }
}
