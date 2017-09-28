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
package org.apache.drill.hbase;

import static org.junit.Assert.*;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.hbase.HBaseStoragePlugin;
import org.apache.drill.exec.store.hbase.HBaseStoragePluginConfig;
import org.apache.drill.exec.util.GuavaPatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.Test;

public class AdHocTest extends DrillTest {

  static {
    GuavaPatcher.patch();
  }

  @Test
  public void testHBase() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {

      setupHBase(cluster);
      String sql = "select CONVERT_FROM(a.`v`.`e0`, 'UTF8') as k, count(a.`v`.`e0`) p\n" +
                   "from `hbase`.browser_action2 a\n" +
                   "where a.row_key > '0'\n" +
                   "group by a.`v`.`e0`\n" +
                   "order by a.`v`.`e0`";
//      String plan = client.queryBuilder().sql(sql).explainText();
//      System.out.println(plan);
      client.queryBuilder().sql(sql).printCsv();
//      QuerySummary summary = client.queryBuilder().sql(sql).run();
//      System.out.println(String.format("Returned %,d records in %d batches; %d ms.",
//          summary.recordCount(), summary.batchCount(), summary.runTimeMs()));
    }
  }

  @Test
  public void testWildcard() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {

      setupHBase(cluster);
      for (int i = 0; i < 10; i++) {
        System.out.println("Pass " + i + 1);
        String sql = "select *\n" +
                     "from `hbase`.browser_action2 a\n";
//      String plan = client.queryBuilder().sql(sql).explainText();
//      System.out.println(plan);
        client.queryBuilder().sql(sql).printCsv();
//      QuerySummary summary = client.queryBuilder().sql(sql).run();
//      System.out.println(String.format("Returned %,d records in %d batches; %d ms.",
//          summary.recordCount(), summary.batchCount(), summary.runTimeMs()));
      }
    }
  }

  private void setupHBase(ClusterFixture cluster) throws ExecutionSetupException {
    Drillbit drillbit = cluster.drillbit();
    final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
    HBaseStoragePlugin storagePlugin = (HBaseStoragePlugin) pluginRegistry.getPlugin(BaseHBaseTest.HBASE_STORAGE_PLUGIN_NAME);
    HBaseStoragePluginConfig storagePluginConfig = storagePlugin.getConfig();
    storagePluginConfig.setEnabled(true);
    storagePluginConfig.setZookeeperPort(2181);
    pluginRegistry.createOrUpdate(BaseHBaseTest.HBASE_STORAGE_PLUGIN_NAME, storagePluginConfig, true);
  }

}
