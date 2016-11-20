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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.exec.physical.impl.xsort.BufferingQueryEventListener.QueryEvent;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.test.DrillTest;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestExternalSortRM extends DrillTest {

  @Test
  public void testManagedSpilled() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create("xsort/drill-external-sort-rm.conf");
//    DrillConfig config = DrillConfig.create( );

    try (Drillbit bit = new Drillbit(config, serviceSet);
        DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {

      bit.run();
      defineWorkspace( bit, "dfs", "data", "/Users/paulrogers/work/data", "psv" );
      client.connect();
      setMaxFragmentWidth( client, 1 );
      performSort( client );
    }
  }

  @Test
  public void testManagedInMemory() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

//    DrillConfig config = DrillConfig.create("xsort/drill-external-sort-rm.conf");
    Properties props = new Properties( );
    props.put( ClassBuilder.SAVE_CODE_OPTION, "true");
    DrillConfig config = DrillConfig.create( props );

    try (Drillbit bit = new Drillbit(config, serviceSet);
        DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {

      bit.run();
      defineWorkspace( bit, "dfs", "data", "/Users/paulrogers/work/data", "psv" );
      client.connect();
      setMaxFragmentWidth( client, 3 );
      performSort( client );
    }
  }

  private void performSort(DrillClient client) throws IOException {
    BufferingQueryEventListener listener = new BufferingQueryEventListener( );
    String sql = Files.toString(FileUtils.getResourceAsFile("/xsort/sort-small-all.sql"),
        Charsets.UTF_8);
    long start = System.currentTimeMillis();
    client.runQuery(QueryType.SQL, sql, listener);
    int recordCount = 0;
    int batchCount = 0;
    loop:
    for ( ; ; ) {
      QueryEvent event = listener.get();
      switch ( event.type )
      {
      case BATCH:
        batchCount++;
        recordCount += event.batch.getHeader().getRowCount();
        event.batch.release();
        break;
      case EOF:
        break loop;
      case ERROR:
        event.error.printStackTrace();
        fail( );
        break loop;
      case QUERY_ID:
        break;
      default:
        break;
      }
    }
    long end = System.currentTimeMillis();
    long elapsed = end - start;

    assertEquals(1000, recordCount);

    System.out.println(String.format("Sorted %,d records in %d batches; %d ms.", recordCount, batchCount, elapsed));
  }

  public void defineWorkspace( Drillbit drillbit, String pluginName, String schemaName, String path, String defaultFormat ) throws ExecutionSetupException {
    final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin(pluginName);
    final FileSystemConfig pluginConfig = (FileSystemConfig) plugin.getConfig();
    final WorkspaceConfig newTmpWSConfig = new WorkspaceConfig(path, true, defaultFormat);

    pluginConfig.workspaces.remove(schemaName);
    pluginConfig.workspaces.put(schemaName, newTmpWSConfig);

    pluginRegistry.createOrUpdate(pluginName, pluginConfig, true);
  }

  public void setMaxFragmentWidth( DrillClient drillClient, int maxFragmentWidth ) throws RpcException {
    final List<QueryDataBatch> results = drillClient.runQuery(
        QueryType.SQL, String.format("alter session set `%s` = %d",
            ExecConstants.MAX_WIDTH_PER_NODE_KEY, maxFragmentWidth));
    for (QueryDataBatch queryDataBatch : results) {
      queryDataBatch.release();
    }
  }
}
