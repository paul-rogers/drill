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
package org.apache.drill.exec.store.exec.store.log;

import static org.junit.Assert.assertEquals;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.log.LogFormatConfig;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;

public class TestLogReader extends ClusterTest {

  public static final String DATE_ONLY_PATTERN = "(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d) .*";

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    // Define a regex format config for testing.

    defineRegexPlugin();
  }

  private static void defineRegexPlugin() throws ExecutionSetupException {

    // Create an instance of the regex config.
    // Note: we can't use the ".log" extension; the Drill .gitignore
    // file ignores such files, so they'll never get committed. Instead,
    // make up a fake suffix.

    LogFormatConfig sampleConfig = new LogFormatConfig();
    sampleConfig.extension = "log1";
    sampleConfig.regex = DATE_ONLY_PATTERN;
    sampleConfig.fields = Lists.newArrayList("year", "month", "day");
    sampleConfig.dataTypes = Lists.newArrayList(
        MinorType.INT.name(),
        MinorType.INT.name(),
        MinorType.INT.name());

    // Full Drill log parser definition.

    LogFormatConfig logConfig = new LogFormatConfig();
    logConfig.extension = "log2";
    logConfig.regex = "(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d) " +
                      "(\\d\\d):(\\d\\d):(\\d\\d),\\d+ " +
                      "\\[([^]]*)] (\\w+)\\s+(\\S+) - (.*)";
    logConfig.fields = Lists.newArrayList("year", "month", "day", "hour",
        "minute", "second", "thread", "level", "module", "message");
    sampleConfig.dataTypes = Lists.newArrayList(
        MinorType.INT.name(), // year
        MinorType.INT.name(), // month
        MinorType.INT.name(), // day
        MinorType.INT.name(), // hour
        MinorType.INT.name(), // minute
        MinorType.INT.name(), // second
        MinorType.VARCHAR.name()); // thread, default the others

    // Define a temporary format plugin for the "cp" storage plugin.

    Drillbit drillbit = cluster.drillbit();
    final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin("cp");
    final FileSystemConfig pluginConfig = (FileSystemConfig) plugin.getConfig();
    pluginConfig.formats.put("sample", sampleConfig);
    pluginConfig.formats.put("drill-log", logConfig);
    pluginRegistry.createOrUpdate("cp", pluginConfig, false);
  }

  @Test
  public void testWildcard() throws RpcException {
    String sql = "SELECT * FROM cp.`regex/simple.log1`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("year", MinorType.INT)
        .addNullable("month", MinorType.INT)
        .addNullable("day", MinorType.INT)
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(2017, 12, 17)
        .addRow(2017, 12, 18)
        .addRow(2017, 12, 19)
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExplicit() throws RpcException {
    String sql = "SELECT `day`, `month` FROM cp.`regex/simple.log1`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("day", MinorType.INT)
        .addNullable("month", MinorType.INT)
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(17, 12)
        .addRow(18, 12)
        .addRow(19, 12)
        .build();

//    results.print();
//    expected.print();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testMissing() throws RpcException {
    String sql = "SELECT `day`, `missing`, `month` FROM cp.`regex/simple.log1`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("day", MinorType.INT)
        .addNullable("missing", MinorType.VARCHAR)
        .addNullable("month", MinorType.INT)
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(17, null, 12)
        .addRow(18, null, 12)
        .addRow(19, null, 12)
        .build();

//    results.print();
//    expected.print();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testCount() throws RpcException {
    String sql = "SELECT COUNT(*) FROM cp.`regex/simple.log1`";
    long result = client.queryBuilder().sql(sql).singletonLong();
    assertEquals(3, result);
  }

  @Test
  public void testFull() throws RpcException {
    String sql = "SELECT * FROM cp.`regex/simple.log2`";
    client.queryBuilder().sql(sql).printCsv();
  }
}
