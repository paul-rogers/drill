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

import org.apache.drill.BaseTestQuery;
import org.apache.drill.QueryTestUtil;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestSimpleExternalSort extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleExternalSort.class);

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(80000);

  @Test
  public void mergeSortWithSv2Managed() throws Exception {
    chooseImpl(false);
    mergeSortWithSv2();
  }

  @Test
  public void mergeSortWithSv2Legacy() throws Exception {
    chooseImpl(true);
    mergeSortWithSv2();
  }

  private void mergeSortWithSv2() throws Exception {
    List<QueryDataBatch> results = testPhysicalFromFileWithResults("xsort/one_key_sort_descending_sv2.json");
    int count = 0;
    for(QueryDataBatch b : results) {
      count += b.getHeader().getRowCount();
    }
    assertEquals(500000, count);

    long previousBigInt = Long.MAX_VALUE;

    int recordCount = 0;
    int batchCount = 0;

    for (QueryDataBatch b : results) {
      if (b.getHeader().getRowCount() == 0) {
        continue;
      }
      batchCount++;
      RecordBatchLoader loader = new RecordBatchLoader(allocator);
      loader.load(b.getHeader().getDef(),b.getData());
      BigIntVector c1 = (BigIntVector) loader.getValueAccessorById(BigIntVector.class,
              loader.getValueVectorId(new SchemaPath("blue", ExpressionPosition.UNKNOWN)).getFieldIds()).getValueVector();


  @Test
  public void mergeSortWithSv2Legacy() throws Exception {
    mergeSortWithSv2(true);
  }

  /**
   * Tests the external sort using an in-memory sort. Relies on default memory
   * settings to be large enough to do the in-memory sort (there is,
   * unfortunately, no way to double-check that no spilling was done.)
   * This must be checked manually by setting a breakpoint in the in-memory
   * sort routine.
   *
   * @param testLegacy
   * @throws Exception
   */

  private void mergeSortWithSv2(boolean testLegacy) throws Exception {
    try (ClusterFixture client = standardClient( )) {
      chooseImpl(client, testLegacy);
      List<QueryDataBatch> results = client.queryBuilder( ).physicalResource("xsort/one_key_sort_descending_sv2.json").results();
      assertEquals(500000, client.countResults( results ));
      validateResults(client.allocator(), results);
    }
  }

  private void chooseImpl(ClusterFixture client, boolean testLegacy) throws Exception {
    client.alterSession(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName(), testLegacy);
  }

  private void chooseImpl(boolean testLegacy) throws Exception {
    String options = "alter session set `" + ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName() + "` = " +
        Boolean.toString(testLegacy);
    QueryTestUtil.test(client, options);
  }

  @Test
  public void sortOneKeyDescendingMergeSortManaged() throws Throwable {
    chooseImpl(false);
    sortOneKeyDescendingMergeSort();
  }

  @Test
  public void sortOneKeyDescendingMergeSortLegacy() throws Throwable {
    chooseImpl(true);
    sortOneKeyDescendingMergeSort();
  }

  private void sortOneKeyDescendingMergeSort() throws Throwable {
    List<QueryDataBatch> results = testPhysicalFromFileWithResults("xsort/one_key_sort_descending.json");
    int count = 0;
    for (QueryDataBatch b : results) {
      if (b.getHeader().getRowCount() != 0) {
        count += b.getHeader().getRowCount();
      }
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
    chooseImpl(false);
    sortOneKeyDescendingExternalSort();
  }

  @Test
  public void sortOneKeyDescendingExternalSortLegacy() throws Throwable {
    chooseImpl(true);
    sortOneKeyDescendingExternalSort();
  }

  private void sortOneKeyDescendingExternalSort() throws Throwable {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create("/drill-external-sort.conf");

    try (Drillbit bit1 = new Drillbit(config, serviceSet);
//        Drillbit bit2 = new Drillbit(config, serviceSet);
        DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {

      bit1.run();
//      bit2.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("xsort/one_key_sort_descending.json"),
                      Charsets.UTF_8));
      int count = 0;
      for (QueryDataBatch b : results) {
        count += b.getHeader().getRowCount();
      }
      assertEquals(1000000, count);

      long previousBigInt = Long.MAX_VALUE;

      int recordCount = 0;
      int batchCount = 0;

      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() == 0) {
          continue;
        }
        batchCount++;
        RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
        loader.load(b.getHeader().getDef(),b.getData());
        BigIntVector c1 = (BigIntVector) loader.getValueAccessorById(BigIntVector.class, loader.getValueVectorId(new SchemaPath("blue", ExpressionPosition.UNKNOWN)).getFieldIds()).getValueVector();


        BigIntVector.Accessor a1 = c1.getAccessor();

        for (int i =0; i < c1.getAccessor().getValueCount(); i++) {
          recordCount++;
          assertTrue(String.format("%d < %d", previousBigInt, a1.get(i)), previousBigInt >= a1.get(i));
          previousBigInt = a1.get(i);
        }
        loader.clear();
        b.release();
      }
      System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));
    }
  }

  @Test
  public void outOfMemoryExternalSortManaged() throws Throwable{
    chooseImpl(false);
    outOfMemoryExternalSort();
  }

  @Test
  public void outOfMemoryExternalSortLegacy() throws Throwable{
    chooseImpl(true);
    outOfMemoryExternalSort();
  }

  private void outOfMemoryExternalSort() throws Throwable{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create("/drill-oom-xsort.conf");

    try (Drillbit bit1 = new Drillbit(config, serviceSet);
        DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {

      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(org.apache.drill.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/xsort/oom_sort_test.json"),
                      Charsets.UTF_8));
      int count = 0;
      for (QueryDataBatch b : results) {
        count += b.getHeader().getRowCount();
      }
      assertEquals(10000000, count);

      long previousBigInt = Long.MAX_VALUE;

      int recordCount = 0;
      int batchCount = 0;

      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() == 0) {
          continue;
        }
        loader.clear();
        b.release();
      }
      System.out.println(String.format("Sorted %,d records in %d batches.", recordCount, batchCount));
    }
  }
}
