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
package org.apache.drill.exec.physical.rowSet;

import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.FixtureBuilder;
import org.junit.Test;

public class AdHocReaderTest extends DrillTest {

  @Test
  public void readerTest() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1)
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
//      client.queryBuilder().sql("SELECT * from `cp`.`tpch/customer.parquet` LIMIT 10").printCsv();

      TextFormatConfig csvFormat = new TextFormatConfig();
      csvFormat.fieldDelimiter = ',';
      csvFormat.skipFirstLine = false;
      csvFormat.extractHeader = true;
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "csv", csvFormat);
      String sql = "SELECT * FROM `dfs.data`.`csv/test4.csv` LIMIT 10";
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void readerTest2() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1)
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      TextFormatConfig csvFormat = new TextFormatConfig();
      csvFormat.fieldDelimiter = ',';
      csvFormat.skipFirstLine = true;
      csvFormat.extractHeader = false;
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "csv", csvFormat);
      String sql = "SELECT columns[0] AS h, columns[1] AS u FROM `dfs.data`.`csv/test4.csv` LIMIT 10";
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void readerTest3() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1)
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      TextFormatConfig csvFormat = new TextFormatConfig();
      csvFormat.fieldDelimiter = ',';
      csvFormat.skipFirstLine = false;
      csvFormat.extractHeader = false;
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "csv", csvFormat);
//      String sql = "SELECT columns[0] as a, columns[1] as b, columns[2] as c FROM `dfs.data`.`csv/test5.csv`";
      String sql = "SELECT COUNT(DISTINCT columns[1] || columns[2]) FROM `dfs.data`.`csv/test5.csv`";
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void readerTest4() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1)
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "csv");
      String sql = "SELECT columns[1], columns[3] FROM `dfs.data`.`csv/test6.csv`";
      System.out.println(sql);
      client.queryBuilder().sql(sql).printCsv();
      sql = "SELECT columns[3], columns[1] FROM `dfs.data`.`csv/test6.csv`";
      System.out.println(sql);
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void readerTest5() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1)
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "csv");
      String sql = "SELECT columns[70000], columns[3] FROM `dfs.data`.`csv/test6.csv`";
      System.out.println(sql);
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void readerTest7() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1)
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
//      client.queryBuilder().sql("SELECT * from `cp`.`tpch/customer.parquet` LIMIT 10").printCsv();

      TextFormatConfig csvFormat = new TextFormatConfig();
      csvFormat.fieldDelimiter = ',';
      csvFormat.skipFirstLine = false;
      csvFormat.extractHeader = true;
      cluster.defineWorkspace("dfs", "data", "/Users/paulrogers/work/data", "csv", csvFormat);
//      String sql = "SELECT * FROM `dfs.data`.`csv/test11.csv`";
//      String sql = "SELECT `EXPR$1` as A, 1 + 2, 3 + 4 FROM `dfs.data`.`csv/test11.csv`";
      String sql = "SELECT `EXPR$1`, 1 + 2, 3 + 4 FROM `dfs.data`.`csv/test11.csv`";
      client.queryBuilder().sql(sql).printCsv();
    }
  }
}
