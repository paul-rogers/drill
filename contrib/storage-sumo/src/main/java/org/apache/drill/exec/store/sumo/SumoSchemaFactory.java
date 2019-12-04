package org.apache.drill.exec.store.sumo;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

public class SumoSchemaFactory extends AbstractSchemaFactory {

  public static final String LOGS_SCHEMA_NAME = "logs";
  public static final String METRICS_SCHEMA_NAME = "metrics";
  public static final String ALL_LOGS_TABLE_NAME = "logs";
  public static final String ALL_METRICS_TABLE_NAME = "metrics";
  public static final String PT_TZ = "America/Los_Angeles";
  public static final Map<String, SumoQuery> dummySchema;

  static {
    dummySchema = new HashMap<>();

    // Example logs query
    SumoQuery query = new SumoQuery(
        "_sourceHost=\"nite-metricsstore-9\"",
        false,
        "2019-11-12T13:10:00",
        "2019-11-12T13:10:15",
        PT_TZ, false);
    dummySchema.put("logQuery1", query);

    query = new SumoQuery(
        "(_sourceHost=\"nite-metricsstore-9\") AND \"Done acking offset\"",
        false,
        "2019-11-12T13:10:00",
        "2019-11-12T13:10:15",
        PT_TZ, false);
    dummySchema.put("logQuery2", query);

    // Example aggregate query
    query = new SumoQuery(
        "* | count _sourceHost",
        true,
        "2019-11-12T13:10:00",
        "2019-11-12T13:10:15",
        PT_TZ, false);
    dummySchema.put("aggQuery1", query);
  }

  private final SumoStoragePlugin plugin;

  public SumoSchemaFactory(SumoStoragePlugin plugin) {
    super(plugin.getName());
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
      throws IOException {
    parent.add(getName(), new DefaultSchema(getName()));
  }

  class DefaultSchema extends AbstractSchema {

    private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();

    DefaultSchema(String name) {
      super(Collections.emptyList(), name);
    }

    @Override
    public String getTypeName() {
      return SumoStoragePluginConfig.NAME;
    }

    @Override
    public Table getTable(String name) {
      DynamicDrillTable table = activeTables.get(name);
      if (table != null) {
        return table;
      }
      SumoQuery query = lookupQuery(name);
      if (query == null) {
        return null;
      }

      SumoStoragePluginConfig config = plugin.config();
      query = query.applyDefaults(config.getDefaultStartOffset(),
          config.getDefaultEndOffset(), config.getTimeZone());
      return registerTable(name,
          new DynamicDrillTable(plugin, plugin.getName(),
              new SumoScanSpec(LOGS_SCHEMA_NAME, name, query)));
    }

    private SumoQuery lookupQuery(String name) {

      // Table for a query created on the fly
      if (ALL_LOGS_TABLE_NAME.contentEquals(name)) {
        SumoStoragePluginConfig config = plugin.config();
        return new SumoQuery(config.getTimeZone(), config.useReceiptTime());
      }

      // Use a "view"

      return dummySchema.get(name);
    }

    private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
      activeTables.put(name, table);
      return table;
    }

    @Override
    public Set<String> getTableNames() {
      return Sets.newHashSet(ALL_LOGS_TABLE_NAME);
    }
  }
}
