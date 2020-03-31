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

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@JsonTypeName(HttpStoragePluginConfig.NAME)
public class HttpStoragePluginConfig extends StoragePluginConfigBase {

  public static final String NAME = "http";

  public final Map<String, HttpAPIConfig> connections;

  public final boolean cacheResults;

  /**
   * Timeout in seconds.
   */
  public int timeout;

  @JsonCreator
  public HttpStoragePluginConfig(@JsonProperty("cacheResults") boolean cacheResults,
                                 @JsonProperty("connections") Map<String, HttpAPIConfig> connections,
                                 @JsonProperty("timeout") int timeout) {
    this.cacheResults = cacheResults;

    if (connections == null) {
      this.connections = new HashMap<>();
    } else {
      Map<String, HttpAPIConfig> caseInsensitiveAPIs = CaseInsensitiveMap.newHashMap();
      Optional.of(connections).ifPresent(caseInsensitiveAPIs::putAll);
      this.connections = caseInsensitiveAPIs;
    }

    this.timeout = timeout;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    HttpStoragePluginConfig thatConfig = (HttpStoragePluginConfig) that;
    return (this.cacheResults == thatConfig.cacheResults) &&
      this.connections.equals(thatConfig.connections);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("connections", connections)
      .field("cacheResults", cacheResults)
      .field("timeout", timeout)
      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(cacheResults, connections, timeout);
  }

  @JsonProperty("cacheResults")
  public boolean cacheResults() { return cacheResults; }

  @JsonProperty("connections")
  public Map<String, HttpAPIConfig> connections() { return connections; }

  @JsonProperty("timeout")
  public int timeout() { return timeout;}
}
