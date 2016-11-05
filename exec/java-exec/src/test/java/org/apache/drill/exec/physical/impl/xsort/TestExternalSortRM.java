package org.apache.drill.exec.physical.impl.xsort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.physical.impl.xsort.BufferingQueryEventListener.QueryEvent;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestExternalSortRM extends BaseTestQuery {

  @Test
  public void test() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create("drill-external-sort-rm.conf");

    try (Drillbit bit = new Drillbit(config, serviceSet);
        DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {

      bit.run();
      defineWorkspace( bit, "dfs", "data", "/Users/paulrogers/work/data", "psv" );
      client.connect();
      BufferingQueryEventListener listener = new BufferingQueryEventListener( );
      String sql = Files.toString(FileUtils.getResourceAsFile("/xsort/sort-big-all.sql"),
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
      
      assertEquals(2880404, recordCount);

      System.out.println(String.format("Sorted %,d records in %d batches; %d ms.", recordCount, batchCount, elapsed));

    }
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
}
