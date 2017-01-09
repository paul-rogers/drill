package org.apache.drill.exec.store.revised.origExample;

import org.apache.drill.common.logical.StoragePluginConfigBase;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(ProtoPluginConfig.NAME)
public class ProtoPluginConfig extends StoragePluginConfigBase {

  public static final String NAME = "proto";

  @Override
  public boolean equals(Object o) {
    return o instanceof ProtoPluginConfig;
  }

  @Override
  public int hashCode() {
    return 0;
  }

}
