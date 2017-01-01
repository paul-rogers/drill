package org.apache.drill.exec.store.revised;

import java.io.IOException;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

public class StoragePluginAdapter extends AbstractStoragePlugin {

  @Override
  public StoragePluginConfig getConfig() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
      throws IOException {
    // TODO Auto-generated method stub

  }

}
