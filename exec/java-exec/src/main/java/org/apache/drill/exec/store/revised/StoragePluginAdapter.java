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
package org.apache.drill.exec.store.revised;

import java.io.IOException;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

public abstract class StoragePluginAdapter<T extends StorageExtension, C extends StoragePluginConfig> extends AbstractStoragePlugin {

  protected C config;
  private T system;
  protected DrillbitContext context;
  protected String name;

  protected StoragePluginAdapter(C config, DrillbitContext context, String schemaName) {
    this.config = config;
    this.context = context;
    this.name = schemaName;
    system = createSystem(schemaName, config, context);
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  public C config() { return config; }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
      throws IOException {
    // TODO Auto-generated method stub

  }

  protected T system( ) {
//    if (system == null) {
//      system = DataSourceRegistry.instance().system( this );
//    }
    return system;
  }

  protected abstract T createSystem(String schemaName, C config, DrillbitContext context);

  @Override
  public boolean equals(Object object) {
    if (object == null  ||  ! (object instanceof StoragePluginAdapter)) {
      return false; }
    StoragePluginAdapter<?, ?> other = (StoragePluginAdapter<?, ?>) object;
    return name.equals(other.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
