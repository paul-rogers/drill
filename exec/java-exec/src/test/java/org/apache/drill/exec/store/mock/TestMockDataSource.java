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
package org.apache.drill.exec.store.mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMockDataSource extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  /**
   * Verify the SQL version of the mock data source. Can't check the actual
   * data (it is random), but we can check row count and schema.
   *
   * @throws Exception if anything goes wrong
   */

  @Test
  public void testExtendedSqlOneBatch() throws Exception {

    // Assumes a default batch size of 4K rows or larger

    String sql = "SELECT id_i, name_s10 FROM `mock`.`employees_1K`";

    DirectRowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(1_000, results.rowCount());

    BatchSchema expected = new SchemaBuilder()
        .add("id_i", MinorType.INT)
        .add("name_s10", MinorType.VARCHAR, 10)
        .build();

    // Note: assertEquals does not work here. See DRILL-5525

    assertTrue(results.batchSchema().isEquivalent(expected));
    results.clear();
  }

  @Test
  public void testExtendedSqlMultiBatch() throws Exception {
    String sql = "SELECT id_i, name_s10 FROM `mock`.`employees_10K`";

    QuerySummary results = client.queryBuilder().sql(sql).run();
    assertEquals(10_000, results.recordCount());
  }

}
