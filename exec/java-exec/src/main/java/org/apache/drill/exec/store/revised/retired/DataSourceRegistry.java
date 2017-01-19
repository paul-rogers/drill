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
package org.apache.drill.exec.store.revised.retired;

import java.util.concurrent.ExecutionException;

import org.apache.drill.common.logical.StoragePluginConfig;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class DataSourceRegistry {

  private class Loader extends CacheLoader<StoragePluginAdapter<? extends StorageExtension, ? extends StoragePluginConfig>, StorageExtension> {
    @Override
    public StorageExtension load(final StoragePluginAdapter<? extends StorageExtension, ? extends StoragePluginConfig> adapter) throws Exception {
      return null; // adapter.createSystem();
    }
  }

  public static final int TABLE_SPACE_REGISTRY_SIZE = 100;

  private static Object lock = new Object();
  private static DataSourceRegistry instance;
  private final LoadingCache<StoragePluginAdapter<? extends StorageExtension, ? extends StoragePluginConfig>, ? extends StorageExtension> registry;

  public DataSourceRegistry() {
    registry = CacheBuilder.newBuilder()
        .maximumSize(TABLE_SPACE_REGISTRY_SIZE)
        .build(new Loader());
  }

  // Register known data sources
  // Register known schemas
  // Mapping of schema to data source instance

  public static DataSourceRegistry instance() {
    if (instance != null) {
      return instance;
    }
    synchronized (lock) {
      if (instance == null) {
        instance = new DataSourceRegistry();
      }
    }
    return instance;
  }

  @SuppressWarnings("unchecked")
  public <T extends StorageExtension> T system( StoragePluginAdapter<T, ? extends StoragePluginConfig> adapter ) {
    try {
      return (T) registry.get(adapter);
    } catch (ExecutionException e) {
      throw new IllegalStateException( e );
    }
  }
}
