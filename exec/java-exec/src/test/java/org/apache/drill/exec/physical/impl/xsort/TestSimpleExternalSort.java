/**
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
package org.apache.drill.exec.physical.impl.xsort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.ClientFixture;
import org.apache.drill.ClientFixture.FixtureBuilder;
import org.apache.drill.DrillEngineTest;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestSimpleExternalSort extends DrillEngineTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleExternalSort.class);

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(80000);

  @Test
  public void mergeSortWithSv2Managed() throws Exception {
    mergeSortWithSv2(false);
  }

  @Test
  public void mergeSortWithSv2Legacy() throws Exception {
    mergeSortWithSv2(true);
  }

  private void mergeSortWithSv2(boolean testLegacy) throws Exception {
    try (ClientFixture client = standardClient( )) {
      chooseImpl(client, testLegacy);
      List<QueryDataBatch> results = client.runPhysicalFromResource("xsort/one_key_sort_descending_sv2.json");
      assertEquals(500000, client.countResults( results ));
      validateResults(client.allocator(), results);
    }
  }

  private void chooseImpl(ClientFixture client, boolean testLegacy) throws Exception {
    client.alterSession(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName(), testLegacy);
  }

  @Test
  public void sortOneKeyDescendingMergeSortManaged() throws Throwable {
    sortOneKeyDescendingMergeSort(false);
  }

  @Test
  public void sortOneKeyDescendingMergeSortLegacy() throws Throwable {
    sortOneKeyDescendingMergeSort(true);
  }

  private void sortOneKeyDescendingMergeSort(boolean testLegacy) throws Throwable {
    try (ClientFixture client = standardClient( )) {
      chooseImpl(client, testLegacy);
      List<QueryDataBatch> results = client.runPhysicalFromResource("xsort/one_key_sort_descending.json");
      assertEquals(1000000, client.countResults(results));
      validateResults(client.allocator(), results);
    }
  }

  private void validateResults(BufferAllocator allocator, List<QueryDataBatch> results) throws SchemaChangeException {
    long previousBigInt = Long.MAX_VALUE;

    int recordCount = 0;
    int batchCount = 0;

    for (QueryDataBatch b : results) {
      RecordBatchLoader loader = new RecordBatchLoader(allocator);
      if (b.getHeader().getRowCount() > 0) {
        batchCount++;
        loader.load(b.getHeader().getDef(),b.getData());
        BigIntVector c1 = (BigIntVector) loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(new SchemaPath("blue", ExpressionPosition.UNKNOWN)).getFieldIds()).getValueVector();
        BigIntVector.Accessor a1 = c1.getAccessor();

        for (int i = 0; i < c1.getAccessor().getValueCount(); i++) {
          recordCount++;
          assertTrue(String.format("%d > %d", previousBigInt, a1.get(i)), previousBigInt >= a1.get(i));
          previousBigInt = a1.get(i);
        }
      }
      loader.clear();
      b.release();
    }

    System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));
  }


  @Test
  public void sortOneKeyDescendingExternalSortManaged() throws Throwable {
    sortOneKeyDescendingExternalSort(false);
  }

  @Test
  public void sortOneKeyDescendingExternalSortLegacy() throws Throwable {
    sortOneKeyDescendingExternalSort(true);
  }

  private void sortOneKeyDescendingExternalSort(boolean testLegacy) throws Throwable {
    FixtureBuilder builder = newBuilder( )
        .property(ExecConstants.EXTERNAL_SORT_SPILL_THRESHOLD, 4 )
        .property(ExecConstants.EXTERNAL_SORT_SPILL_GROUP_SIZE, 4)
        .property(ExecConstants.EXTERNAL_SORT_BATCH_LIMIT, 4);
    try (ClientFixture client = builder.build( )) {
      chooseImpl(client,testLegacy);
      List<QueryDataBatch> results = client.runPhysicalFromResource("/xsort/one_key_sort_descending.json");
      assertEquals(1000000, client.countResults( results ));
      validateResults(client.allocator(), results);
    }
  }

  @Test
  public void outOfMemoryExternalSortManaged() throws Throwable{
    outOfMemoryExternalSort(false);
  }

  @Test
  public void outOfMemoryExternalSortLegacy() throws Throwable{
    outOfMemoryExternalSort(true);
  }

  private void outOfMemoryExternalSort(boolean testLegacy) throws Throwable{
    FixtureBuilder builder = newBuilder( )
        // Probably do nothing in modern Drill
        .property( "drill.memory.fragment.max", 50000000 )
        .property( "drill.memory.fragment.initial", 2000000 )
        .property( "drill.memory.operator.max", 30000000 )
        .property( "drill.memory.operator.initial", 2000000 );
    try (ClientFixture client = builder.build( )) {
      chooseImpl(client,testLegacy);
      List<QueryDataBatch> results = client.runPhysicalFromResource("/xsort/oom_sort_test.json");
      assertEquals(10000000, client.countResults( results ));

      long previousBigInt = Long.MAX_VALUE;

      int recordCount = 0;
      int batchCount = 0;

      for (QueryDataBatch b : results) {
        RecordBatchLoader loader = new RecordBatchLoader(client.allocator());
        if (b.getHeader().getRowCount() > 0) {
          batchCount++;
          loader.load(b.getHeader().getDef(),b.getData());
          BigIntVector c1 = (BigIntVector) loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(new SchemaPath("blue", ExpressionPosition.UNKNOWN)).getFieldIds()).getValueVector();
          BigIntVector.Accessor a1 = c1.getAccessor();

          for (int i = 0; i < c1.getAccessor().getValueCount(); i++) {
            recordCount++;
            assertTrue(String.format("%d < %d", previousBigInt, a1.get(i)), previousBigInt >= a1.get(i));
            previousBigInt = a1.get(i);
          }
          assertTrue(String.format("%d == %d", a1.get(0), a1.get(a1.getValueCount() - 1)), a1.get(0) != a1.get(a1.getValueCount() - 1));
        }
        loader.clear();
        b.release();
      }
      System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));
    }
  }
}
