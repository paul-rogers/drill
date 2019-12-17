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

package org.apache.drill.exec.store.http;

import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * In the HTTP storage plugin, users can define specific connections or APIs.  This class represents the
 * database component of other storage plugins.
 */
public class HttpAPIConnectionSchema extends AbstractSchema {

  private static final Logger logger = LoggerFactory.getLogger(HttpAPIConnectionSchema.class);

  private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();

  private final HttpStoragePlugin plugin;

  private final String pluginName;

  public HttpAPIConnectionSchema(HttpSchemaFactory.HttpSchema httpSchema,
                                 String name,
                                 HttpStoragePlugin plugin) {
    super(httpSchema.getSchemaPath(), name);
    this.plugin = plugin;
    pluginName = plugin.getName();
  }

  @Override
  public String getTypeName() {
    return HttpStoragePluginConfig.NAME;
  }

  /**
   * This function get the table that is received from the query.
   * @param tableName The "tableName" actually will contain the URL arguments passed to the record reader
   * @return Table Returns the selected table
   */
  @Override
  public Table getTable(String tableName) {
    DynamicDrillTable table = activeTables.get(name);
    if (table != null) {
      return table;
    }

    if (!activeTables.containsKey(name)) {
      return registerTable(name, new DynamicDrillTable(plugin, pluginName, new HttpScanSpec(pluginName, name, tableName, plugin.getConfig())));
    }
    return null;
  }

  @Override
  public Set<String> getTableNames() {
    return activeTables.keySet();
  }

  private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
    activeTables.put(name, table);
    return table;
  }
}
