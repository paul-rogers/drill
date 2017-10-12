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
package org.apache.drill.exec.physical.impl.scan;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

/**
 * Test the row batch merger by merging two batches.
 */

public class TestRowBatchMerger extends SubOperatorTest {

  private SingleRowSet makeFirst() {
    BatchSchema firstSchema = new SchemaBuilder()
        .add("d", MinorType.VARCHAR)
        .add("a", MinorType.INT)
        .build();
    return fixture.rowSetBuilder(firstSchema)
        .addRow("barney", 10)
        .addRow("wilma", 20)
        .build();
  }

  private SingleRowSet makeSecond() {
    BatchSchema secondSchema = new SchemaBuilder()
        .add("b", MinorType.INT)
        .add("c", MinorType.VARCHAR)
        .build();
    return fixture.rowSetBuilder(secondSchema)
        .addRow(1, "foo.csv")
        .addRow(2, "foo.csv")
        .build();
  }

  /**
   * Test the row batch merger by merging two batches:
   * (d, a) and (b, c) to produce (a, b, c, d).
   */

  @Test
  public void testExchange() {

    // Create the first batch

    SingleRowSet first = makeFirst();

    // Create the second batch

    SingleRowSet second = makeSecond();

    RowBatchMerger merger = new RowBatchMerger.Builder()
        .addExchangeProjection(first.container(), 0, 3)
        .addExchangeProjection(first.container(), 1, 0)
        .addExchangeProjection(second.container(), 0, 1)
        .addExchangeProjection(second.container(), 1, 2)
        .build(fixture.allocator());

    // Do the merge

    merger.project(first.rowCount());
    RowSet result = fixture.wrap(merger.getOutput());

    // Since the merge was an exchange, source columns
    // should have been cleared.

    assertEquals(0, first.container().getValueVector(0).getValueVector().getValueCapacity());
    assertEquals(0, first.container().getValueVector(1).getValueVector().getValueCapacity());
    assertEquals(0, second.container().getValueVector(0).getValueVector().getValueCapacity());
    assertEquals(0, second.container().getValueVector(1).getValueVector().getValueCapacity());

    // Verify

    verify(result);
  }

  private void verify(RowSet result) {
    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(10, 1, "foo.csv", "barney")
        .addRow(20, 2, "foo.csv", "wilma")
        .build();

    new RowSetComparison(expected)
      .verifyAndClearAll(result);
  }

  @Test
  public void testDirect() {

    // Create the first batch

    SingleRowSet first = makeFirst();

    // Create the second batch

    SingleRowSet second = makeSecond();

    // Merge: direct for first batch, exchange for second

    RowBatchMerger merger = new RowBatchMerger.Builder()
        .addDirectProjection(  first.container(),  0, 3)
        .addDirectProjection(  first.container(),  1, 0)
        .addExchangeProjection(second.container(), 0, 1)
        .addExchangeProjection(second.container(), 1, 2)
        .build(fixture.allocator());

    // Do the merge

    merger.project(first.rowCount());
    RowSet result = fixture.wrap(merger.getOutput());

    // Since the merge was an exchange, source columns
    // should have been cleared.

    assertTrue(0 < first.container().getValueVector(0).getValueVector().getValueCapacity());
    assertTrue(0 < first.container().getValueVector(1).getValueVector().getValueCapacity());
    assertEquals(0, second.container().getValueVector(0).getValueVector().getValueCapacity());
    assertEquals(0, second.container().getValueVector(1).getValueVector().getValueCapacity());

    // Verify

    verify(result);
  }
}
