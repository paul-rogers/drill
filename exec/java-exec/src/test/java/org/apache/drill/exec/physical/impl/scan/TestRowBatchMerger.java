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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.Projection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.VectorSource;
import org.apache.drill.exec.physical.rowSet.impl.NullResultVectorCacheImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Test the row batch merger by merging two batches. Tests both the
 * "direct" and "exchange" cases. Direct means that the output container
 * contains the source vector directly: they are the same vectors.
 * Exchange means we have two vectors, but we swap the underlying
 * Drillbufs to effectively shift data from source to destination
 * vector.
 */

public class TestRowBatchMerger extends SubOperatorTest {

  public static class RowSetSource implements VectorSource {

    private SingleRowSet rowSet;

    public RowSetSource(SingleRowSet rowSet) {
      this.rowSet = rowSet;
    }

    @Override
    public VectorContainer container() {
      return rowSet.container();
    }

    public RowSet rowSet() { return rowSet; }

    public void clear() {
      rowSet.clear();
    }
  }


  private RowSetSource makeFirst() {
    BatchSchema firstSchema = new SchemaBuilder()
        .add("d", MinorType.VARCHAR)
        .add("a", MinorType.INT)
        .build();
    return new RowSetSource(
        fixture.rowSetBuilder(firstSchema)
          .addRow("barney", 10)
          .addRow("wilma", 20)
          .build());
  }

  private RowSetSource makeSecond() {
    BatchSchema secondSchema = new SchemaBuilder()
        .add("b", MinorType.INT)
        .add("c", MinorType.VARCHAR)
        .build();
    return new RowSetSource(
        fixture.rowSetBuilder(secondSchema)
          .addRow(1, "foo.csv")
          .addRow(2, "foo.csv")
          .build());
  }

  /**
   * Test the row batch merger by merging two batches:
   * (d, a) and (b, c) to produce (a, b, c, d).
   */

  @Test
  public void testExchange() {

    // Create the first batch

    RowSetSource first = makeFirst();

    // Create the second batch

    RowSetSource second = makeSecond();

    VectorContainer output = new VectorContainer(fixture.allocator());

    List<Projection> projections = Lists.newArrayList(
      new Projection(first, false, 0, 3),
      new Projection(first, false, 1, 0),
      new Projection(second, false, 0, 1),
      new Projection(second, false, 1, 2)
      );
    RowBatchMerger merger = new RowBatchMerger(output, projections);
    merger.buildOutput(new NullResultVectorCacheImpl(fixture.allocator()));

    // Do the merge

    merger.project(first.rowSet().rowCount());
    RowSet result = fixture.wrap(merger.getOutput());

    // Since the merge was an exchange, source columns
    // should have been cleared.

    VectorContainer firstContainer = first.rowSet().container();
    assertEquals(0, firstContainer.getValueVector(0).getValueVector().getValueCapacity());
    assertEquals(0, firstContainer.getValueVector(1).getValueVector().getValueCapacity());

    VectorContainer secondContainer = first.rowSet().container();
    assertEquals(0, secondContainer.getValueVector(0).getValueVector().getValueCapacity());
    assertEquals(0, secondContainer.getValueVector(1).getValueVector().getValueCapacity());

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

    RowSetSource first = makeFirst();

    // Create the second batch

    RowSetSource second = makeSecond();

    VectorContainer output = new VectorContainer(fixture.allocator());

    // Merge: direct for first batch, exchange for second

    List<Projection> projections = Lists.newArrayList(
      new Projection(first, true, 0, 3),
      new Projection(first, true, 1, 0),
      new Projection(second, false, 0, 1),
      new Projection(second, false, 1, 2)
      );
    RowBatchMerger merger = new RowBatchMerger(output, projections);
    merger.buildOutput(new NullResultVectorCacheImpl(fixture.allocator()));

    // Do the merge

    merger.project(first.rowSet().rowCount());
    RowSet result = fixture.wrap(merger.getOutput());

    // The direct columns should be the same in input and output,
    // exchange columns should be cleared.

    VectorContainer firstContainer = first.rowSet().container();
    assertTrue(0 < firstContainer.getValueVector(0).getValueVector().getValueCapacity());
    assertTrue(0 < firstContainer.getValueVector(1).getValueVector().getValueCapacity());

    VectorContainer secondContainer = second.rowSet().container();
    assertEquals(0, secondContainer.getValueVector(0).getValueVector().getValueCapacity());
    assertEquals(0, secondContainer.getValueVector(1).getValueVector().getValueCapacity());

    // Verify

    verify(result);
  }
}
