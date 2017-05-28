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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

/**
 * Test tor row batch merger by merging two batches.
 */

public class TestRowBatchMerger extends SubOperatorTest {

  /**
   * Test the row batch merger by merging two batches:
   * (d, a) and (b, c) to produce (a, b, c, d).
   */

  @Test
  public void test() {

    // Create the first batch

    BatchSchema firstSchema = new SchemaBuilder()
        .add("d", MinorType.VARCHAR)
        .add("a", MinorType.INT)
        .build();
    SingleRowSet first = fixture.rowSetBuilder(firstSchema)
        .add("barney", 10)
        .add("wilma", 20)
        .build();

    // Create the second batch

    BatchSchema secondSchema = new SchemaBuilder()
        .add("b", MinorType.INT)
        .add("c", MinorType.VARCHAR)
        .build();
    SingleRowSet second = fixture.rowSetBuilder(secondSchema)
        .add(1, "foo.csv")
        .add(2, "foo.csv")
        .build();

    RowBatchMerger merger = new RowBatchMerger.Builder()
        .addProjection(first.container(), 0, 3)
        .addProjection(first.container(), 1, 0)
        .addProjection(second.container(), 0, 1)
        .addProjection(second.container(), 1, 2)
        .build(fixture.allocator());

    // Do the merge

    merger.project(first.rowCount());
    RowSet result = fixture.wrap(merger.getOutput());

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(10, 1, "foo.csv", "barney")
        .add(20, 2, "foo.csv", "wilma")
        .build();

    new RowSetComparison(expected)
      .verifyAndClearAll(result);
  }

}
