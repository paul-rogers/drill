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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.xsort.managed.PriorityQueueCopierWrapper.BatchMerger;
import org.apache.drill.exec.physical.impl.xsort.managed.SortTestUtilities.CopierTester;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.accessor.AccessorUtilities;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.OperatorFixture.OperatorFixtureBuilder;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
    OperatorFixtureBuilder builder = OperatorFixture.builder();
    builder.configBuilder()
      .put(ExecConstants.EXTERNAL_SORT_GENERIC_COPIER, false);
    fixture = builder.build();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    fixture.close();
  }

  @Test
  public void testEmptyInput() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    List<BatchGroup> batches = new ArrayList<>();
    PriorityQueueCopierWrapper copier = SortTestUtilities.makeCopier(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED);
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

  @Test
  public void testEmptyBatch() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    CopierTester tester = new CopierTester(fixture);
    tester.addInput(fixture.rowSetBuilder(schema)
          .withSv2()
          .build());

    tester.run();
  }

  @Test
  public void testSingleRow() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    CopierTester tester = new CopierTester(fixture);
    tester.addInput(fixture.rowSetBuilder(schema)
          .add(10, "10")
          .withSv2()
          .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
          .add(10, "10")
          .build());
    tester.run();
  }

  @Test
  public void testTwoBatchesSingleRow() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();
    CopierTester tester = new CopierTester(fixture);
    tester.addInput(fixture.rowSetBuilder(schema)
          .add(10, "10")
          .withSv2()
          .build());
    tester.addInput(fixture.rowSetBuilder(schema)
          .add(20, "20")
          .withSv2()
          .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
          .add(10, "10")
          .add(20, "20")
          .build());
    tester.run();
  }

  public SingleRowSet makeDataSet(RowSetSchema schema, int first, int step, int count) {
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer(count);
    int value = first;
    for (int i = 0; i < count; i++, value += step) {
      writer.next();
      AccessorUtilities.setFromInt(writer.column(0), value);
      writer.column(1).setString(Integer.toString(value));
    }
    writer.done();
    return rowSet;
  }

  @Test
  public void testMultipleOutput() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.addInput(makeDataSet(schema, 0, 2, 10).toIndirect());
    tester.addInput(makeDataSet(schema, 1, 2, 10).toIndirect());

    tester.addOutput(makeDataSet(schema, 0, 1, 10));
    tester.addOutput(makeDataSet(schema, 10, 1, 10));
    tester.run();
  }

  // Also verifies that SV2s work

  @Test
  public void testMultipleOutputDesc() throws Exception {
    RowSetSchema schema = SortTestUtilities.nonNullSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_DESC;
    tester.nullOrder = Ordering.NULLS_UNSPECIFIED;
    SingleRowSet input = makeDataSet(schema, 0, 2, 10).toIndirect();
    RowSetUtilities.reverse(input.getSv2());
    tester.addInput(input);

    input = makeDataSet(schema, 1, 2, 10).toIndirect();
    RowSetUtilities.reverse(input.getSv2());
    tester.addInput(input);

    tester.addOutput(makeDataSet(schema, 19, -1, 10));
    tester.addOutput(makeDataSet(schema, 9, -1, 10));

    tester.run();
  }

  @Test
  public void testAscNullsLast() throws Exception {
    RowSetSchema schema = SortTestUtilities.nullableSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_ASC;
    tester.nullOrder = Ordering.NULLS_LAST;
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(1, "1")
        .add(4, "4")
        .add(null, "null")
        .withSv2()
        .build());
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(2, "2")
        .add(3, "3")
        .add(null, "null")
        .withSv2()
        .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
        .add(1, "1")
        .add(2, "2")
        .add(3, "3")
        .add(4, "4")
        .add(null, "null")
        .add(null, "null")
        .build());

    tester.run();
  }

  @Test
  public void testAscNullsFirst() throws Exception {
    RowSetSchema schema = SortTestUtilities.nullableSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_ASC;
    tester.nullOrder = Ordering.NULLS_FIRST;
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(1, "1")
        .add(4, "4")
        .withSv2()
        .build());
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(2, "2")
        .add(3, "3")
        .withSv2()
        .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(null, "null")
        .add(1, "1")
        .add(2, "2")
        .add(3, "3")
        .add(4, "4")
        .build());

    tester.run();
  }

  @Test
  public void testDescNullsLast() throws Exception {
    RowSetSchema schema = SortTestUtilities.nullableSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_DESC;
    tester.nullOrder = Ordering.NULLS_LAST;
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(4, "4")
        .add(1, "1")
        .add(null, "null")
        .withSv2()
        .build());
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(3, "3")
        .add(2, "2")
        .add(null, "null")
        .withSv2()
        .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
        .add(4, "4")
        .add(3, "3")
        .add(2, "2")
        .add(1, "1")
        .add(null, "null")
        .add(null, "null")
        .build());

    tester.run();
  }

  @Test
  public void testDescNullsFirst() throws Exception {
    RowSetSchema schema = SortTestUtilities.nullableSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_DESC;
    tester.nullOrder = Ordering.NULLS_FIRST;
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(4, "4")
        .add(1, "1")
        .withSv2()
        .build());
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(3, "3")
        .add(2, "2")
        .withSv2()
        .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(null, "null")
        .add(4, "4")
        .add(3, "3")
        .add(2, "2")
        .add(1, "1")
        .build());

    tester.run();
  }

  public void runTypeTest(MinorType type) throws Exception {
    RowSetSchema schema = SortTestUtilities.makeSchema(type, false);

    CopierTester tester = new CopierTester(fixture);
    tester.addInput(makeDataSet(schema, 0, 2, 5).toIndirect());
    tester.addInput(makeDataSet(schema, 1, 2, 5).toIndirect());

    tester.addOutput(makeDataSet(schema, 0, 1, 10));

    tester.run();
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
}
