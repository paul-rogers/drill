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

@JsonTypeName(DummyStoragePluginConfig.NAME)
public class DummyStoragePluginConfig extends StoragePluginConfig {

  public static final String NAME = "dummy";

  public enum FilterPushDownStyle {
    NONE, KEEP, REMOVE
  }

  public final boolean supportProjectPushDown;
  public final FilterPushDownStyle filterPushDownStyle;

  public DummyStoragePluginConfig(
      boolean supportProjectPushDown,
      FilterPushDownStyle filterPushDownStyle) {
    this.supportProjectPushDown = supportProjectPushDown;
    this.filterPushDownStyle = filterPushDownStyle;
    setEnabled(true);
  }

  @JsonProperty("supportProjectPushDown")
  public boolean supportProjectPushDown() { return supportProjectPushDown; }

  @JsonProperty("filterPushDownStyle")
  public FilterPushDownStyle filterPushDownStyle() { return filterPushDownStyle; }

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
           filterPushDownStyle == other.filterPushDownStyle;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(supportProjectPushDown, filterPushDownStyle);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("supportProjectPushDown", supportProjectPushDown)
        .field("filterPushDownStyle", filterPushDownStyle)
        .toString();
  }
}
