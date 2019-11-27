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
package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.QueryBuilder.QuerySummary;

public class FilterPerformanceTest {

  public static void main(String[] args) throws Exception {
    BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();
    dirTestWatcher.start(FilterPerformanceTest.class);
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher);
    ClusterFixture cluster = builder.build();
    ClientFixture client = cluster.clientFixture();
    try {
      runTests(client);
    } finally {
      client.close();
      cluster.close();
    }
  }

  private static final int ROW_COUNT = 10_000_000;

  private static void runTests(ClientFixture client) throws Exception {
    System.out.println("Warm up");
    runQuerySet(client, ", b_s100");

    System.out.println("Thin rows");
    runQuerySet(client, "");

    System.out.println("Single VARCHAR");
    runQuerySet(client, ", b_s100");

    System.out.println("Wide");
    String select = "";
    for (int i = 0; i < 10; i++) {
      select += ", " + Character.toString((char)('b' + i)) + "_s100";
    }
    runQuerySet(client, select);

    System.out.println("Nulls");
    select = "";
    for (int i = 0; i < 6; i++) {
      select += ", " + Character.toString((char)('b' + i)) + "_s100n50";
    }
    runQuerySet(client, select);
  }

  public static void runQuerySet(ClientFixture client, String select) throws Exception {
    String sql = "SELECT a_i%s FROM mock.table_%d";
    timeQuery(client, String.format(sql + " WHERE a_i = 0", select, ROW_COUNT));
    timeQuery(client, String.format(sql, select, ROW_COUNT));
    sql += " WHERE mod(a_i, %d) = 0";
    timeQuery(client, String.format(sql, select, ROW_COUNT, 2));
    timeQuery(client, String.format(sql, select, ROW_COUNT, 10));
  }

  public static void timeQuery(ClientFixture client, String sql) throws Exception {
    QuerySummary result = client.queryBuilder().sql(sql).run();
    System.out.println(sql);
    System.out.println(String.format(
        "Time (ms): %d  Rows: %d  Batches: %d",
        result.runTimeMs(), result.recordCount(), result.batchCount()));
    System.out.println("-----");
  }

}
