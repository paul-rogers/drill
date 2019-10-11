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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;

public class HttpStoragePlugin extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpStoragePlugin.class);

  private DrillbitContext context;

  private final HttpStoragePluginConfig engineConfig;

  private final HttpSchemaFactory schemaFactory;

  public HttpStoragePlugin(HttpStoragePluginConfig configuration, DrillbitContext context, String name) throws IOException {
    super(context, name);
    this.schemaFactory = new HttpSchemaFactory(this, name);
    this.context = context;
    this.engineConfig = configuration;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public HttpStoragePluginConfig getConfig() {
    return engineConfig;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public HttpGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    HttpScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<HttpScanSpec>() {});
    logger.debug("getPhysicalScan {} {} {} {}", userName, selection, selection.getRoot(), spec);
    return new HttpGroupScan(userName, engineConfig, null, spec);
  }
}
