package org.apache.drill.exec.store.revised;

import org.apache.drill.common.logical.StoragePluginConfig;

public interface ExtensionFactory {

  <T extends TableSpaceSystem, C extends StoragePluginConfig> T newSystem( C config );
}
