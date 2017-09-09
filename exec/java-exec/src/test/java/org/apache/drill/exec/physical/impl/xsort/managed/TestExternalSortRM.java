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

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.drill.exec.testing.Controls;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.LogFixture.LogFixtureBuilder;
import org.apache.drill.test.ProfileParser;
import org.apache.drill.test.ProfileParser.OperatorProfile;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.Test;

import ch.qos.logback.classic.Level;

//@Ignore
public class TestExternalSortRM extends DrillTest {

  public static final int ONE_MB = 1024 * 1024;
  public static final long ONE_GB = ONE_MB * 1024;

//  @Test
//  public void testManagedSpilled() throws Exception {
//    LogAnalyzer analyzer = new LogAnalyzer(true);
//    analyzer.setupLogging();
//
//    FixtureBuilder builder = ClusterFixture.builder()
//        .configProperty(ExecConstants.EXTERNAL_SORT_BATCH_LIMIT, 40)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MERGE_LIMIT, 40)
//        .maxParallelization(1);
//    try (ClusterFixture cluster = builder.build();
//         ClientFixture client = cluster.clientFixture()) {
//      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
//      performSort(client);
//    }
//
//    EventAnalyzer analysis = analyzer.analyzeLog();
//    SortStats stats = analysis.getStats();
//
//    // Verify that spilling occurred. That it occurred
//    // correctly is verified by the query itself.
//
//    assertTrue(stats.gen1SpillCount > 0);
//    assertTrue(stats.gen2SpillCount > 0);
//    analysis.report();
//  }

  @Test
  public void testManagedSpilledWide() throws Exception {
//    LogAnalyzer analyzer = new LogAnalyzer(true);
//    analyzer.setupLogging();

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

//    analyzer.analyzeLog();
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
//        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.REMOVER_ENABLE_GENERIC_COPIER, true)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
//        .sessionOption(ExecConstants.QUERY_PROFILE_OPTION, "none")
        .saveProfiles()
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
//        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.REMOVER_ENABLE_GENERIC_COPIER, true)
//        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .maxParallelization(1)
        ;

    // Launch the cluster and client.
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {

      // Run a query and print a summary.
      String sql = "SELECT * FROM `mock`.`test/example-mock.json`";
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
//    LogAnalyzer analyzer = new LogAnalyzer(false);
//    analyzer.setupLogging();

    FixtureBuilder builder = ClusterFixture.builder()
        .configResource("xsort/drill-external-sort-legacy.conf")
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      performSort(client);
    }

//    analyzer.analyzeLog();
  }

  @Test
  public void testManagedInMemory() throws Exception {
//    LogAnalyzer analyzer = new LogAnalyzer(true);
//    analyzer.setupLogging();

    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String plan = client.queryBuilder().sqlResource("/xsort/sort-big-all.sql").explainJson();
      System.out.println(plan);
      performSort(client);
    }

//    analyzer.analyzeLog();
  }

  @Test
  public void testManagedInMemory2() throws Exception {
//    LogAnalyzer analyzer = new LogAnalyzer(true);
//    analyzer.setupLogging();

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

//    analyzer.analyzeLog();
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
//    LogAnalyzer analyzer = new LogAnalyzer(true);
//    analyzer.setupLogging();

    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
//      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      performSort(client);
    }

//    analyzer.analyzeLog();
  }

  @Test
  public void testRahulsSort() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "select * from (select col_s4000 from `mock`.`rahul_10M` order by col_s4000) d where d.col_s4000 = 'bogus'";
      String plan = client.queryBuilder().sql(sql).explainJson();
      System.out.println(plan);
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %d records, %d batches, %d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testMD1304a() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.DEBUG);
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .withLocalZk()
        .sessionOption(ExecConstants.SLICE_TARGET, 1000);
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
//      String sql = "SELECT col_s3500 FROM `mock`.`table_150K` ORDER BY col_s3500";
//      String sql = "SELECT * FROM `mock`.`xsort/MD1304.json` ORDER BY col1";
      String sql = "SELECT * FROM (SELECT * FROM `mock`.`xsort/MD1304.json` ORDER BY col1) d WHERE d.col1 = 'bogus'";
      runAndDump(client, sql);
    }
  }

  public static void main(String args[]) {
    try {
      new TestExternalSortRM().testDrill5528();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void testMD1322() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.DEBUG)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT * FROM (SELECT * FROM `mock`.`xsort/MD1322a.json` ORDER BY col) d WHERE d.col <> 'bogus'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testMD1322a() throws Exception {
//    LogFixtureBuilder logBuilder = LogFixture.builder()
//        .toConsole()
//        .logger(ExternalSortBatch.class, Level.DEBUG)
//        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .maxParallelization(1)
//        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        ;
    try (/*LogFixture logs = logBuilder.build(); */
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT key_s250 FROM `mock`.`table_43M` ORDER BY key_s250";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testMD1322b() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        .logger(org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
//        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
//        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, true)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
//      String sql = "SELECT * FROM (SELECT * FROM `mock`.`xsort/MD1322a.json` ORDER BY col) d WHERE d.col <> 'bogus'";
      String sql = "select * from (select * from `dfs.data`.`descending-col-length-8k.tbl` order by columns[0])d where d.columns[0] <> 'ljdfhwuehnoiueyf'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testMD1346b() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        ;
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
//      String sql = "SELECT * FROM `dfs.data`.`250wide.tbl` WHERE columns[0] = 'askjdhfjhfds'";
      String sql = "SELECT * FROM `dfs.data`.`250wide.tbl` ORDER BY columns[0]";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testMD1346() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.DEBUG)
//        .logger(BatchGroup.class, Level.TRACE)
//        .logger(org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
//        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
//        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, true)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
//        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 1073741824L)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select d2.col1 from (select d.col1 from (select distinct columns[0] col1 from `dfs.data`.`250wide.tbl`) d order by concat(d.col1, 'ASDF'))d2 where d2.col1 = 'askjdhfjhfds'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testMD1350() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.DEBUG)
//        .logger(BatchGroup.class, Level.DEBUG)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
//      String sql = "select * from (select * from `dfs.data`.`250wide.tbl` order by columns[0])d where d.columns[0] = 'ljdfhwuehnoiueyf'";
      String sql = "select * from (select * from `dfs.data`.`250wide.tbl` order by columns[0]) where columns[0] = 'ljdfhwuehnoiueyf'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testMD1346c() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.DEBUG)
//        .logger(BatchGroup.class, Level.TRACE)
//        .logger(org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
//        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
//        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, true)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select columns[0] col1 from `dfs.data`.`250wide.tbl` order by col1";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testDrill5235() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.DEBUG)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
//        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
//        .configProperty(ExecConstants.EXTERNAL_SORT_SPILL_DIRS, new String[] {
//            "/tmp/drill", "/tmp/drill1", "/tmp/drill2" } )
//        .configProperty(ExecConstants.EXTERNAL_SORT_SPILL_DIRS + ".0", "/tmp/drill" )
//        .configProperty(ExecConstants.EXTERNAL_SORT_SPILL_DIRS + ".1", "/tmp/drill1" )
//        .configProperty(ExecConstants.EXTERNAL_SORT_SPILL_DIRS + ".2", "/tmp/drill2" )
//        .configProperty(ExecConstants.EXTERNAL_SORT_SPILL_DIRS,
//            Lists.newArrayList("/tmp/drill", "/tmp/drill1", "/tmp/drill2") )
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
//      String sql = "select d2.col1 from (select d.col1 from (select distinct columns[0] col1 from `dfs.data`.`250wide.tbl`) d order by concat(d.col1, 'ASDF'))d2 where d2.col1 = 'askjdhfjhfds'";
      String sql = "SELECT columns[0] col1 FROM `dfs.data`.`250wide.tbl` ORDER BY col1";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testDrill5262() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
//        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 30 * 1024 * 1024)
        .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select count(*) from (select * from `dfs.data`.small_large_parquet order by col1 desc) d";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testDrill4301() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
//        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 30 * 1024 * 1024)
        .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select * from `dfs.data`.`fewtypes_boolpartition` where bool_col = true";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testMD1364() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
//        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
//        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, true)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
        .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
//      String sql = "select d2.col1 from (select d.col1 from (select distinct columns[0] col1 from `dfs.data`.`250wide.tbl`) d order by concat(d.col1, 'ASDF'))d2 where d2.col1 = 'askjdhfjhfds'";
      String sql = "select * from `dfs.data`.`1_0_0.parquet` order by c_email_address";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testDrill5210() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
//        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 100 * 1024 * 1024)
        .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      final String controls = Controls.newBuilder()
          .addExceptionOnBit(
              ExternalSortBatch.class,
              ExternalSortBatch.INTERRUPTION_WHILE_MERGING,
              IllegalStateException.class,
              cluster.drillbit().getContext().getEndpoint(),
              3, 1)
          .build();
      client.setControls(controls);
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select * from `dfs.data`.`1_0_0.parquet` order by c_email_address";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testParquet() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort", Level.DEBUG)
        .logger(ExternalSortBatch.class, Level.TRACE)
//        .logger(org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
//        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
//        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, true)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
        .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "SELECT * FROM `dfs.data`.`1_0_0.parquet` order by c_email_address";
//      client.queryBuilder().sql(sql).printCsv();
      runAndDump(client, sql);
    }
  }

  @Test
  public void testMd1365a() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
//        .toConsole()
//        .logger("org.apache.drill.exec.physical.impl.xsort", Level.DEBUG)
//        .logger(ExternalSortBatch.class, Level.TRACE)
////        .logger(org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class, Level.TRACE)
        .logger("org.apache.drill.exec.work.foreman.QueryManager", Level.ERROR)
        .logger("org.apache.drill.exec.rpc.control.ControlServer", Level.ERROR)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
//        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, "3G")
        .maxParallelization(1)
//        .sessionOption(ExecConstants.SLICE_TARGET, 1000)
//        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, true)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 1L * 1024 * 1024 * 1024)
        .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
//      String sql = "SELECT * FROM `dfs.data`.`5000files/text` order by columns[1]";
//      String sql = "SELECT * FROM `dfs.data`.`5000files/text` order by columns[0]";
      String sql = "SELECT * FROM `dfs.data`.`5000files/parquet` order by idx";
//      String sql = "SELECT * FROM `dfs.data`.`5000files/text`";
//      String sql = "SELECT * FROM `dfs.data`.`5000files/text/file1000.tbl`";
//      client.queryBuilder().sql(sql).printCsv();
//      runAndDump(client, sql);
      runAndDump(client, sql);
    }
  }

  @Test
  public void testMd1306() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
//        .logger("org.apache.drill.exec.physical.impl.xsort", Level.DEBUG)
        .logger(ExternalSortBatch.class, Level.DEBUG)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
//        .maxParallelization(1)
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 20 * 1024 * 1024)
//        .sessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, 1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select * from (select * from `dfs.data`.`data1.tsv` order by columns[0]) d where d.columns[0] = 'Q4OUV/SLOWDRILL/Q4OUV!5LJ2JUFLJE4'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void Drill5294a() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select * from (select columns[433] col433, columns[0], columns[1],columns[2],columns[3],columns[4]," +
                                         "columns[5],columns[6],columns[7],columns[8],columns[9],columns[10],columns[11] " +
                                         "from `dfs.data`.`3500cols.tbl` " +
                                         "order by columns[450],columns[330],columns[230],columns[220],columns[110],columns[90]," +
                                         "columns[80],columns[70],columns[40],columns[10],columns[20],columns[30],columns[40]," +
                                         "columns[50]) d where d.col433 = 'sjka skjf'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void Drill5294b() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 62914560)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select * from (select * from `dfs.data`.`250wide-small.tbl` order by columns[0])d where d.columns[0] = 'ljdfhwuehnoiueyf'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void Drill5294c() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 30127360)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select * from (select * from `dfs.data`.`250wide_files` d where cast(d.columns[1] as int) > 0 order by columns[0]) d1 where d1.columns[0] = 'kjhf'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void Drill5294d() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select * from (select * from `dfs.data`.`250wide.tbl` d where cast(d.columns[1] as int) > 0 order by columns[0]) d1 where d1.columns[0] = 'kjhf'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void Drill5226a() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
//        .toFile( new File("/tmp/5266a.log") )
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2 * 104857600)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select col11 from (select * from `dfs.data`.`identical1` order by col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11 desc) d where d.col11 < 10";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testDrill4842() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption("store.json.all_text_mode", true);
        ;
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {

      File dataDir = cluster.makeDataDir("json", "json");
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dataDir, "trailNull.json")))) {
        for (int i = 0; i < 5; i++) {
          writer.write("{ \"c1\" : \"Hello World\" }\n");
        }
        for (int i = 0; i < 5; i++) {
          writer.write("{ \"c1\" : null }\n");
        }
      }
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dataDir, "leadNull.json")))) {
        writer.write("{ \"c1\" : null }\n");
        writer.write("{ \"c1\" : \"Hello World\" }\n");
      }
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dataDir, "someNulls.json")))) {
        for (int i = 0; i < JSONRecordReader.DEFAULT_ROWS_PER_BATCH / 4; i++) {
          writer.write("{ \"c1\" : null }\n");
        }
        writer.write("{ \"c1\" : \"Hello World\" }\n");
      }
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dataDir, "tooManyNulls.json")))) {
        for (int i = 0; i < JSONRecordReader.DEFAULT_ROWS_PER_BATCH; i++) {
          writer.write("{ \"c1\" : null }\n");
        }
        writer.write("{ \"c1\" : \"Hello World\" }\n");
      }

      String sql = "SELECT * FROM `dfs.json`.`trailNull.json`";
//      client.queryBuilder().sql(sql).printCsv();
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      assertEquals(10, summary.recordCount());

      sql = "SELECT * FROM `dfs.json`.`leadNull.json`";
      summary = client.queryBuilder().sql(sql).run();
      assertEquals(2, summary.recordCount());

      sql = "SELECT * FROM `dfs.json`.`someNulls.json`";
      summary = client.queryBuilder().sql(sql).run();
      assertEquals(JSONRecordReader.DEFAULT_ROWS_PER_BATCH / 4 + 1, summary.recordCount());

      sql = "SELECT * FROM `dfs.json`.`tooManyNulls.json`";
      summary = client.queryBuilder().sql(sql).run();
      assertEquals(JSONRecordReader.DEFAULT_ROWS_PER_BATCH + 1, summary.recordCount());
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void testAdHoc1() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 60 * 1024 * 1024)
        .maxParallelization(1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT id_i, name_s250 FROM `mock`.`employee_100K` ORDER BY id_i";
      runAndDump(client, sql);
    }
  }

  public static final int COLUMN_COUNT = 200;
  public static final int NAME_WIDTH = 500;

  public static class SelectBuilder {

    int generation = 0;
    List<String> columns = new ArrayList<>();
    String sql;

    public SelectBuilder() {
      this(null);
    }

    public SelectBuilder(SelectBuilder base) {
      if (base == null) {
        generation = 0;
        buildBaseColumns();
        sql = buildBaseSelect();
      } else {
        generation = base.generation + 1;
        buildOuterCols();
        sql = buildOuterSelect(base);
      }
    }

    private void buildBaseColumns() {
      for ( int i = 0; i < COLUMN_COUNT; i++ ) {
        StringBuilder buf = new StringBuilder();
        buf.append( "col_" );
        buf.append( i );
        buf.append( "_" );
        for (int j = 0; j < NAME_WIDTH; j++ ) {
          buf.append( "x" );
        }
        buf.append( "_i" );
        columns.add(buf.toString());
      }
    }

    private String buildBaseSelect() {
      StringBuilder buf = new StringBuilder();
      buf.append( "SELECT " );
      boolean first = true;
      for (String col : columns) {
        if (! first) {
          buf.append( ", " );
        }
        first = false;
        buf.append(col);
      }
      buf.append( " FROM mock.myTable_10" );
      return buf.toString();
    }

    private void buildOuterCols() {
      for ( int i = 0; i < COLUMN_COUNT; i++ ) {
        StringBuilder buf = new StringBuilder();
        buf.append( "outer" );
        buf.append(generation);
        buf.append("_");
        buf.append( i );
        buf.append( "_" );
        for (int j = 0; j < NAME_WIDTH; j++ ) {
          buf.append( (char) ('a' + generation) );
        }
        columns.add(buf.toString());
      }
    }

    private String buildOuterSelect(SelectBuilder sb) {
      StringBuilder buf = new StringBuilder();
      buf.append( "SELECT " );
      for (int i = 0;  i < columns.size(); i++) {
        if (i > 0) {
          buf.append( ", " );
        }
        buf.append(sb.columns.get(i));
        buf.append(" AS ");
        buf.append(columns.get(i));
      }
      buf.append( " FROM (" );
      buf.append(sb.sql);
      buf.append(")");
      return buf.toString();
    }
  }

  @Test
  public void testAdHoc2() throws Exception {
    try (ClusterFixture cluster = ClusterFixture.standardCluster();
         ClientFixture client = cluster.clientFixture()) {
//      String sql = "SELECT a_i AS b FROM (SELECT a_i FROM mock.table_2)";
//      QuerySummary summary = client.queryBuilder().sql(sql).run();
//      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
      SelectBuilder sb = null;
      for ( ; ; ) {
        sb = new SelectBuilder(sb);
        int len = sb.sql.length();
        System.out.println( "Length: " + len);
        QuerySummary summary = client.queryBuilder().sql(sb.sql).run();
        System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
        if (len > 10_000_000) {
          break;
        }
      }
    }
  }

  @Test
  public void testAdHoc3() throws Exception {
    try (ClusterFixture cluster = ClusterFixture.standardCluster();
         ClientFixture client = cluster.clientFixture()) {
      try (Connection conn = cluster.jdbcConnection()) {
        SelectBuilder sb = null;
        for ( ; ; ) {
          sb = new SelectBuilder(sb);
          int len = sb.sql.length();
          System.out.println( "Length: " + len);
          long start = System.currentTimeMillis();
          try (Statement st = conn.createStatement();
               ResultSet rs = st.executeQuery(sb.sql);) {
            int count = 0;
            while (rs.next()) {
              count++;
            }
            long end = System.currentTimeMillis();
            System.out.println(String.format("Results: %,d records, %,d ms", count, end - start ) );
          }
          if (len > 10_000_000) {
            break;
          }
        }
      }
    }
  }

  @Test
  public void testDrill5528() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        ;
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select count(*) from (select * from dfs.data.`250wide.tbl` order by columns[0])d where d.columns[0] = 'ljdfhwuehnoiueyf'";
//      String sql = "select * FROM (select * from dfs.data.`250wide.tbl`)d where d.columns[0] = 'ljdfhwuehnoiueyf'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testDrill5519() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption("planner.memory.max_query_memory_per_node", 334288000L)
        ;
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select count(*) from (select * from (select flatten(flatten(lst_lst)) num from dfs.data.`nested-large.json`) d order by d.num) d1 where d1.num < -1";
//      String sql = "select * FROM (select * from dfs.data.`250wide.tbl`)d where d.columns[0] = 'ljdfhwuehnoiueyf'";
      runAndDump(client, sql);
    }
  }

  @Test
  public void testDrill5522() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
//        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
//        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
//        .sessionOption("planner.memory.max_query_memory_per_node", 334288000L)
        ;
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "create table `dfs_test.tmp`.xsort_ctas3_multiple partition by (type, aCol) as select type, rptds, rms, s3.rms.a aCol, uid from (\n" +
                   "  select * from (\n" +
                   "    select s1.type type, flatten(s1.rms.rptd) rptds, s1.rms, s1.uid\n" +
                   "    from (\n" +
                   "        select d.type type, d.uid uid, flatten(d.map.rm) rms from dfs.data.`nested-large.json` d order by d.uid\n" +
                   "    ) s1\n" +
                   "  ) s2\n" +
                   "  order by s2.rms.mapid, s2.rptds.a\n" +
                   ") s3";
//      String sql = "select * FROM (select * from dfs.data.`250wide.tbl`)d where d.columns[0] = 'ljdfhwuehnoiueyf'";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5513() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.TRACE)
        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
//        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
//        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .configProperty(ExecConstants.EXTERNAL_SORT_MAX_MEMORY, 20_000_000)
//        .sessionOption("planner.memory.max_query_memory_per_node", 334288000L)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select count(*) from (select s1.type type, flatten(s1.rms.rptd) rptds from\n" +
                   "  (select d.type type, d.uid uid, flatten(d.map.rm) rms \n" +
                   "    from dfs.data.`nested-large.json` d order by d.uid) s1 order by s1.rms.mapid)";
//      String sql = "select * FROM (select * from dfs.data.`250wide.tbl`)d where d.columns[0] = 'ljdfhwuehnoiueyf'";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill4274() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.TRACE)
        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
//        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 3L * 1024 * 1024 * 1024)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
//        .sessionOption("planner.memory.max_query_memory_per_node", 334288000L)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "ds", "/Users/paulrogers/git/drill-test-framework/framework/resources/Datasources/data-shapes/wide-columns/5000/1000rows/parquet", "parquet");
      client.runSqlSilently("USE `dfs.ds`");
//      String sql = "select columns FROM `dfs.ds`.`wide-strings.tbl` ORDER BY columns[0]";
      String sql = "select ws1.* from widestrings ws1 INNER JOIN (select str_var_null_empty from widestrings where str_var_null_empty is not null and length(str_var_null_empty) <> 0 )ws2 on ws1.str_empty_null=ws2.str_var_null_empty where ws1.str_empty_null is not null and length(ws1.str_empty_null) <> 0";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5465() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.TRACE)
        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 200435456)
        .sessionOption(ExecConstants.MAX_WIDTH_GLOBAL_KEY, 1)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "tpcds_sf1_parquet", "/Users/paulrogers/work/data/tpcds_sf1_parquet/parquet", "parquet");
      cluster.defineWorkspace("dfs", "tpcds_sf1_parquet_views", "/Users/paulrogers/work/data/tpcds_sf1_parquet/views", "parquet");
//      client.enableTrace(true);
      client.exec(new File("/Users/paulrogers/work/data/tpcds_sf1_parquet/createViewsParquet.sql"));
      String sql =
          "SELECT dt.d_year,\n" +
          "               item.i_brand_id          brand_id,\n" +
          "               item.i_brand             brand,\n" +
          "               Sum(ss_ext_discount_amt) sum_agg\n" +
          "FROM   date_dim dt,\n" +
          "       store_sales,\n" +
          "       item\n" +
          "WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
          "       AND store_sales.ss_item_sk = item.i_item_sk\n" +
          "       AND item.i_manufact_id = 427\n" +
          "       AND dt.d_moy = 11\n" +
          "GROUP  BY dt.d_year,\n" +
          "          item.i_brand,\n" +
          "          item.i_brand_id\n" +
          "ORDER  BY dt.d_year,\n" +
          "          sum_agg DESC,\n" +
          "          brand_id";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5463() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.DEBUG)
//        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 200435456)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "tpcds_sf1_parquet", "/Users/paulrogers/work/data/tpcds_sf1_parquet/parquet", "parquet");
      cluster.defineWorkspace("dfs", "tpcds_sf1_parquet_views", "/Users/paulrogers/work/data/tpcds_sf1_parquet/views", "parquet");
      client.exec(new File("/Users/paulrogers/work/data/tpcds_sf1_parquet/createViewsParquet.sql"));
      File sqlFile = new File("/Users/paulrogers/work/data/DRILL-5463.sql.txt");
      QuerySummary summary = client.queryBuilder().sql(sqlFile).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5447() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.DEBUG)
//        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "ds", "/Users/paulrogers/work/data", "json");
      client.runSqlSilently("USE `dfs.ds`");
      String sql = "select count(*) from (select * from (select id, flatten(str_list) str from dfs.ds.`flatten-large-small.json`) d order by d.str) d1 where d1.id=0";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5443() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.DEBUG)
        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 52428800)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "ds", "/Users/paulrogers/work/data", "json");
      client.runSqlSilently("USE `dfs.ds`");
      String sql = "select s1.type type, flatten(s1.rms.rptd) rptds from (select d.type type, d.uid uid, flatten(d.map.rm) rms from dfs.ds.`nested-large.json` d order by d.uid) s1 order by s1.rms.mapid";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5753() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.DEBUG)
        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 1_252_428_800)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "ds", "/Users/paulrogers/work/data", "json");
      client.runSqlSilently("USE `dfs.ds`");
      String sql =  "select count(*) from (\n" +
                    "  select * from (\n" +
                    "    select s1.type type, flatten(s1.rms.rptd) rptds, s1.rms, s1.uid \n" +
                    "    from (\n" +
                    "      select d.type type, d.uid uid, flatten(d.map.rm) rms from dfs.ds.`nested-large.json` d order by d.uid\n" +
                    "    ) s1\n" +
                    "  ) s2\n" +
                    "  order by s2.rms.mapid, s2.rptds.a, s2.rptds.do_not_exist\n" +
                    ")";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5744() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.DEBUG)
        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 1_252_428_800)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "ds", "/Users/paulrogers/work/data", "json");
      client.runSqlSilently("USE `dfs.ds`");
      String sql =  "select count(*) from (\n" +
                    "  select * from (\n" +
                    "    select s1.type type, flatten(s1.rms.rptd) rptds, s1.rms, s1.uid \n" +
                    "    from (\n" +
                    "      select d.type type, d.uid uid, flatten(d.map.rm) rms from dfs.ds.`nested-large.json` d order by d.uid\n" +
                    "    ) s1\n" +
                    "  ) s2\n" +
                    "  order by s2.rms.mapid\n" +
                    ")";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5670() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.TRACE)
//        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 482_344_960)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "ds", "/Users/paulrogers/work/data", "json");
      client.runSqlSilently("USE `dfs.ds`");
      String sql =  "select count(*) from (select * from dfs.ds.`3500cols.tbl` " +
                    "order by columns[450],columns[330],columns[230],columns[220],columns[110],columns[90],columns[80]," +
                    "columns[70],columns[40],columns[10],columns[20],columns[30],columns[40],columns[50], columns[454]," +
                    "columns[413],columns[940],columns[834],columns[73],columns[140],columns[104],columns[2222],columns[30]," +
                    "columns[2420],columns[1520], columns[1410], columns[1110],columns[1290],columns[2380],columns[705]," +
                    "columns[45],columns[1054],columns[2430],columns[420],columns[404],columns[3350], columns[3333],columns[153]," +
                    "columns[356],columns[84],columns[745],columns[1450],columns[103],columns[2065],columns[343],columns[3420]," +
                    "columns[530], columns[3210] ) d where d.col433 = 'sjka skjf'";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5416() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.DEBUG)
//        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 52428800)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "ds", "/Users/paulrogers/work/data", "tsv");
      String sql = "select * from dfs.ds.`/5000files/text` order by columns[1]";
      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testDrill5453() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger("org.apache.drill.exec.physical.impl.xsort.managed", Level.DEBUG)
//        .logger("org.apache.drill.exec.vector", Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        .maxParallelization(1)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHJOIN.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
        .keepLocalFiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true)
        ;
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "ds", "/Users/paulrogers/work/data", "tsv");
      File sqlFile = new File("/Users/paulrogers/work/data/DRILL-5453.sql.txt");
      QuerySummary summary = client.queryBuilder().sql(sqlFile).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );
    }
  }

  @Test
  public void testAdHoc4() throws Exception { // DRILL-5519
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .configProperty(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED, false)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 200435456)
        .keepLocalFiles()
        .maxParallelization(1)
        ;
    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT id_i, name_s250 FROM `mock`.`employee_100K` ORDER BY id_i";
      runAndDump(client, sql);
    }
  }

  private void runAndDump(ClientFixture client, String sql) throws Exception {
    String plan = client.queryBuilder().sql(sql).explainJson();
    System.out.println(plan);
    QuerySummary summary = client.queryBuilder().sql(sql).run();
    System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );

    System.out.println("Query ID: " + summary.queryIdString());
    ProfileParser profile = client.parseProfile(summary.queryIdString());
    profile.print();
    List<OperatorProfile> ops = profile.getOpsOfType(CoreOperatorType.EXTERNAL_SORT_VALUE);
    if (ops.isEmpty()) {
      return;
    }
    assertEquals(1, ops.size());
    OperatorProfile sort = ops.get(0);
    long spillCount = sort.getMetric(ExternalSortBatch.Metric.SPILL_COUNT.ordinal());
    long mergeCount = sort.getMetric(ExternalSortBatch.Metric.MERGE_COUNT.ordinal());
//    long inputBatches = sort.getMetric(ExternalSortBatch.Metric.INPUT_BATCHES.ordinal());
    int inputBatches = 0;
    System.out.println(String.format("Input batches: %d, spills: %d, merge/spills: %d",
        inputBatches, spillCount, mergeCount));
  }

  public void dumpProfile() throws IOException {
    String profileName = "2782bf6a-3269-9c23-9109-e43b863cce82.sys.drill.txt";
    File dir = new File("/Users/paulrogers/Downloads/");
    File file = new File( dir, profileName );
    ProfileParser profile = new ProfileParser(file);
    profile.print();
  }

  public void dumpProfile2() throws IOException {
    String profileName = "2776bc68-888f-ce1c-0605-2d805df64626.sys.drill";
    File dir = new File("/Users/paulrogers/Downloads/case_oom_jan_25/");
    File file = new File( dir, profileName );
    ProfileParser profile = new ProfileParser(file);
    profile.printPlan();
//    profile.print();
  }

  public void dumpProfile3() throws IOException {
    String profileName = "plan_profile_1500k";
    File dir = new File("/Users/paulrogers/Downloads/");
    File file = new File( dir, profileName );
    ProfileParser profile = new ProfileParser(file);
    profile.printTime();
//    profile.print();
  }

  public void dumpProfile4() throws IOException {
    String profileName = "27379c45-08e1-d50f-5876-8776fb99b04e.sys.drill";
    File dir = new File("/Users/paulrogers/Downloads");
    File file = new File( dir, profileName );
    ProfileParser profile = new ProfileParser(file);
//    profile.printPlan();
    profile.print();
    List<OperatorProfile> ops = profile.getOpsOfType(CoreOperatorType.HASH_AGGREGATE_VALUE);
    for (OperatorProfile op : ops) {
      System.out.println(String.format("%d-%d-%d", op.majorFragId, op.opId, op.minorFragId));
    }
  }

  private void performSort(ClientFixture client) throws Exception {
    QuerySummary summary = client.queryBuilder().sqlResource("/xsort/sort-big-all.sql").run();
    System.out.println(String.format("Sorted %,d records in %d batches; %d ms.", summary.recordCount(), summary.batchCount(), summary.runTimeMs()));
    assertEquals(2880404, summary.recordCount());
  }
}
