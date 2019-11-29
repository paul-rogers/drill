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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpSchemaFactory extends AbstractSchemaFactory {
  private static final Logger logger = LoggerFactory.getLogger(HttpSchemaFactory.class);

  private final HttpStoragePlugin plugin;

  public static final String MY_TABLE = "result_table";

  public HttpSchemaFactory(HttpStoragePlugin plugin, String schemaName) {
    super(schemaName);
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    logger.debug("registerSchema {}", getName());
    HttpSchema schema = new HttpSchema(getName());
    parent.add(schema.getName(), schema);
  }

  class HttpSchema extends AbstractSchema {

    private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();

    public HttpSchema(String name) {
      super(Collections.emptyList(), name);
    }

    @Override
    public Table getTable(String tableName) { // table name can be any of string
      DynamicDrillTable table = activeTables.get(tableName);
      if (table != null) {
        return table;
      }

      logger.debug("HttpSchema.getTable {}", tableName);
      HttpScanSpec spec = new HttpScanSpec(tableName);
      return registerTable(name, new DynamicDrillTable(plugin, plugin.getName(), spec));
    }

    private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
      activeTables.put(name, table);
      return table;
    }

    @Override
    public String getTypeName() {
      return HttpStoragePluginConfig.NAME;
    }

    @Override
    public Set<String> getTableNames() {
      return Sets.newHashSet(MY_TABLE);
    }
  }
}
