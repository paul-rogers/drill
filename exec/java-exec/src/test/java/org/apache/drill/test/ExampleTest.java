package org.apache.drill.test;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.xsort.managed.ExternalSortBatch;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.LogFixture.LogFixtureBuilder;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.Test;

import ch.qos.logback.classic.Level;

public class ExampleTest {

  @Test
  public void firstTest() throws Exception {
    try (ClusterFixture cluster = ClusterFixture.standardCluster();
         ClientFixture client = cluster.clientFixture()) {
      client.queryBuilder().sql("SELECT * FROM `cp`.`employee.json` LIMIT 10").printCsv();
    }
  }














  @Test
  public void secondTest() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SLICE_TARGET, 10)
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT * FROM `cp`.`employee.json` LIMIT 10";
      System.out.println( client.queryBuilder().sql(sql).explainJson() );
      QuerySummary results = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Read %d rows", results.recordCount()));
    }
  }




















  @Test
  public void thirdTest() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT id_i, name_s10 FROM `mock`.`employees_5`";
      client.queryBuilder().sql(sql).printCsv();
    }
  }





















  @Test
  public void fourthTest() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        .toConsole()
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1)
        .sessionOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
        .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .sessionOption(PlannerSettings.HASHAGG.getOptionName(), false)
        ;

    try (LogFixture logs = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "select d2.col1 from (select d.col1 from (select distinct columns[0] col1 from `dfs.data`.`250wide.tbl`) d order by concat(d.col1, 'ASDF'))d2 where d2.col1 = 'askjdhfjhfds'";
      QuerySummary results = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Read %d rows", results.recordCount()));
    }
  }





















  @Test
  public void fifthTest() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1)
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "psv");
      String sql = "SELECT id_i, name_s10 FROM `mock`.`employees_10K` ORDER BY id_i";

      QuerySummary summary = client.queryBuilder().sql(sql).run();
      System.out.println(String.format("Results: %,d records, %d batches, %,d ms", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );

      System.out.println("Query ID: " + summary.queryIdString());
      ProfileParser profile = client.parseProfile(summary.queryIdString());
      profile.print();
    }
  }
}
