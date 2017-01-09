package org.apache.drill.exec.store.revised;

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
