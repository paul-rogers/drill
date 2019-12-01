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
package org.apache.drill.exec.store.base;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Options for the "test mule" storage plugin. The options
 * control how the plugin behaves for testing. A real plugin
 * would simply implement project push down, and, if filter push-down
 * is needed, would implement one of the four strategies described
 * here.
 */

@JsonTypeName(DummyStoragePluginConfig.NAME)
public class DummyStoragePluginConfig extends StoragePluginConfig {

  public static final String NAME = "dummy";

  public enum FilterPushDownStyle {
    NONE, LOGICAL, PHYSICAL
  }

  /**
   * Whether to enable or disable project push down.
   */
  private final boolean supportProjectPushDown;

  /**
   * Whether to enable or disable filter push down. If enabled,
   * whether to do it at the logical or physical stage.
   */
  private final FilterPushDownStyle filterPushDownStyle;

  /**
   * When doing filter push-down, whether to keep the filters in
   * the plan, or remove them (because they are done (simulated)
   * in the reader.
   */

  private final boolean keepFilters;

  public DummyStoragePluginConfig(
      @JsonProperty("supportProjectPushDown") boolean supportProjectPushDown,
      @JsonProperty("filterPushDownStyle") FilterPushDownStyle filterPushDownStyle,
      @JsonProperty("keepFilters") boolean keepFilters) {
    this.supportProjectPushDown = supportProjectPushDown;
    this.filterPushDownStyle = filterPushDownStyle;
    this.keepFilters = keepFilters;
    setEnabled(true);
  }

  @JsonProperty("supportProjectPushDown")
  public boolean supportProjectPushDown() { return supportProjectPushDown; }

  @JsonProperty("filterPushDownStyle")
  public FilterPushDownStyle filterPushDownStyle() { return filterPushDownStyle; }

  @JsonProperty("keepFilters")
  public boolean keepFilters() { return keepFilters; }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || !(o instanceof DummyStoragePluginConfig)) {
      return false;
    }
    DummyStoragePluginConfig other = (DummyStoragePluginConfig) o;
    return supportProjectPushDown == other.supportProjectPushDown &&
           filterPushDownStyle == other.filterPushDownStyle &&
           keepFilters == other.keepFilters;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(supportProjectPushDown, filterPushDownStyle, keepFilters);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("supportProjectPushDown", supportProjectPushDown)
        .field("filterPushDownStyle", filterPushDownStyle)
        .field("keepFilters", keepFilters)
        .toString();
  }
}
