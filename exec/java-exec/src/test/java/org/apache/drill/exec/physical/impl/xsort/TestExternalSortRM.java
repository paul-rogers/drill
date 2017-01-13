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

import java.io.IOException;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.physical.impl.xsort.LogAnalyzer.EventAnalyzer;
import org.apache.drill.exec.physical.impl.xsort.LogAnalyzer.SortStats;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.Test;

//@Ignore
public class TestExternalSortRM extends DrillTest {

  @Test
  public void testManagedSpilled() throws Exception {
    LogAnalyzer analyzer = new LogAnalyzer(true);
    analyzer.setupLogging();

    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.EXTERNAL_SORT_BATCH_LIMIT, 40)
        .configProperty(ExecConstants.EXTERNAL_SORT_MERGE_LIMIT, 40)
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      performSort(client);
    }

    EventAnalyzer analysis = analyzer.analyzeLog();
    SortStats stats = analysis.getStats();

    // Verify that spilling occurred. That it occurred
    // correctly is verified by the query itself.

    assertTrue(stats.gen1SpillCount > 0);
    assertTrue(stats.gen2SpillCount > 0);
    analysis.report();
  }

  @Test
  public void testManagedSpilledWide() throws Exception {
    LogAnalyzer analyzer = new LogAnalyzer(true);
    analyzer.setupLogging();

    FixtureBuilder builder = ClusterFixture.builder()
        .configResource("xsort/drill-external-sort-rm.conf")
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select * from (select *, row_number() over(order by validitydate) as rn from `dfs.data`.`gen.json`) where rn=10";
      String plan = client.queryBuilder().sql(sql).explainText();
      System.out.println(plan);
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Sorted %,d records in %d batches; %d ms.", summary.recordCount(), summary.batchCount(), summary.runTimeMs()));
    }

    analyzer.analyzeLog();
  }

  @Test
  public void testManagedSpilledWideEx() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configResource("xsort/drill-external-sort-rm.conf")
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select `name`, `monisid`, `validitydate` from `dfs.data`.`gen.json` LIMIT 10";
      String plan = client.queryBuilder().sql(sql).explainText();
      System.out.println(plan);
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Sorted %,d records in %d batches; %d ms.", summary.recordCount(), summary.batchCount(), summary.runTimeMs()));
    }
  }

  @Test
  public void testTPCH09() throws Exception {
    System.setProperty(BaseAllocator.DEBUG_ALLOCATOR, "false");
    FixtureBuilder builder = ClusterFixture.builder()
//      .configProperty(ClassBuilder.SAVE_CODE_OPTION, true)
      .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
      .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = ClusterFixture.loadResource("queries/tpch/09.sql");
      sql = sql.substring(0, sql.length() - 1); // drop the ";"
      String plan = client.queryBuilder().sql(sql).explainText();
      System.out.println(plan);
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Read %,d records in %d batches; %d ms.", summary.recordCount(), summary.batchCount(), summary.runTimeMs()));
      Thread.sleep(1000);
      client.parseProfile(summary).print( );
    }
  }

  @Test
  public void exampleTest() throws Throwable {

    // Configure the cluster. One Drillbit by default.
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .configProperty(ExecConstants.REMOVER_ENABLE_GENERIC_COPIER, true)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .maxParallelization(1)
        ;

    // Launch the cluster and client.
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {

      // Run a query and print a summary.
      String sql = "SELECT id_i FROM `mock`.employee_10M ORDER BY id_i";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      assertEquals(10_000_000, summary.recordCount());
      System.out.println(String.format("Sorted %,d records in %d batches.", summary.recordCount(), summary.batchCount()));
      System.out.println(String.format("Query Id: %s, elapsed: %d ms", summary.queryIdString(), summary.runTimeMs()));
      client.parseProfile(summary.queryIdString()).print();
    }
  }

  @Test
  public void exampleMockTableTest() throws Throwable {

    // Configure the cluster. One Drillbit by default.
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .configProperty(ExecConstants.REMOVER_ENABLE_GENERIC_COPIER, true)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .maxParallelization(1)
        ;

    // Launch the cluster and client.
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {

      // Run a query and print a summary.
      String sql = "SELECT * FROM `mock`.`sort/example-mock.json`";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      assertEquals(20, summary.recordCount());
      System.out.println(String.format("Read %,d records in %d batches.", summary.recordCount(), summary.batchCount()));
      System.out.println(String.format("Query Id: %s, elapsed: %d ms", summary.queryIdString(), summary.runTimeMs()));
//      client.parseProfile(summary.queryIdString()).print();
    }
  }

  @Test
  public void filterTest() throws Throwable {

    // Configure the cluster. One Drillbit by default.
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.REMOVER_ENABLE_GENERIC_COPIER, true)
//        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
//        .configProperty(ClassBuilder.SAVE_CODE_OPTION, true)
        .configProperty(CodeCompiler.PREFER_POJ_CONFIG, true)
        .maxParallelization(1)
        ;

    // Launch the cluster and client.
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {

      String sql = "SELECT id_i FROM `mock`.employee_50M WHERE id_i > 0";
      long sum = 0;
      int n = 5;
      String queryId = null;
      int sampleIndex = (n==1) ? 0 : (n==2) ? 1 : n-2;
      for ( int i = 0; i < n; i++ ) {
        QuerySummary summary = client.queryBuilder().sql(sql).run();
        System.out.println(String.format("Read % d records in %d batches.", summary.recordCount(), summary.batchCount()));
        System.out.println(String.format("Query Id: %s, elapsed: %d ms", summary.queryIdString(), summary.runTimeMs()));
        if ( i > 0 ) {
          sum += summary.runTimeMs();
        }
        if (i == sampleIndex) {
          queryId = summary.queryIdString();
        }
      }
      if (n > 1) {
        System.out.println( "Avg run time: " + sum / (n-1) );
      }
      client.parseProfile(queryId).print();
    }
  }

  @Test
  public void testLegacySpilled() throws Exception {
    LogAnalyzer analyzer = new LogAnalyzer(false);
    analyzer.setupLogging();

    FixtureBuilder builder = ClusterFixture.builder()
        .configResource("xsort/drill-external-sort-legacy.conf")
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      performSort(client);
    }

    analyzer.analyzeLog();
  }

  @Test
  public void testManagedInMemory() throws Exception {
    LogAnalyzer analyzer = new LogAnalyzer(true);
    analyzer.setupLogging();

    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String plan = client.queryBuilder().sqlResource("/xsort/sort-big-all.sql").explainJson();
      System.out.println(plan);
      performSort(client);
    }

    analyzer.analyzeLog();
  }

  @Test
  public void testManagedInMemory2() throws Exception {
    LogAnalyzer analyzer = new LogAnalyzer(true);
    analyzer.setupLogging();

    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("mock", "data", null, null);
      String sql = "SELECT * FROM `mock.data`.`/xsort/test300M.json` ORDER BY sth";
      String plan = client.queryBuilder().sql(sql).explainJson();
      System.out.println(plan);
      performSort(client);
    }

    analyzer.analyzeLog();
  }

  @Test
  public void testSqlMockTable() throws Throwable {
    try (ClusterFixture cluster = ClusterFixture.standardCluster();
         ClientFixture client = cluster.clientFixture()) {
//      cluster.defineWorkspace("mock", "data", null, null);
      client.queryBuilder().sql("SHOW DATABASES").printCsv();
      String sql = "SELECT `id_i`, `num_d`, `name_s50` FROM `mock`.`implicit_10` ORDER BY `name_s50`";
      long count = client.queryBuilder().sql(sql).printCsv();
      System.out.println("Rows: " + count);
    }
  }

  @Test
  public void testManagedGenInMemory() throws Exception {
    LogAnalyzer analyzer = new LogAnalyzer(true);
    analyzer.setupLogging();

    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
//      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      performSort(client);
    }

    analyzer.analyzeLog();
  }

  private void performSort(ClientFixture client) throws IOException {
    QuerySummary summary = client.queryBuilder().sqlResource("/xsort/sort-big-all.sql").run();
    System.out.println(String.format("Sorted %,d records in %d batches; %d ms.", summary.recordCount(), summary.batchCount(), summary.runTimeMs()));
    assertEquals(2880404, summary.recordCount());
  }
}
