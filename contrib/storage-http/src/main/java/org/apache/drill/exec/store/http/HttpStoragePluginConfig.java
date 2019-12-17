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

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.shaded.guava.com.google.common.base.MoreObjects;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@JsonTypeName(HttpStoragePluginConfig.NAME)
public class HttpStoragePluginConfig extends StoragePluginConfigBase {
  private static final Logger logger = LoggerFactory.getLogger(HttpStoragePluginConfig.class);

  public static final String NAME = "http";

  public final Map<String, HttpAPIConfig> connections;

  public final boolean cacheResults;

  @JsonCreator
  public HttpStoragePluginConfig(@JsonProperty("cacheResults") boolean cacheResults,
                                 @JsonProperty("connections") Map<String, HttpAPIConfig> connections) {
    logger.debug("Initialize HttpStoragePluginConfig {}", connections);
    this.cacheResults = cacheResults;

    if (connections != null) {
      Map<String, HttpAPIConfig> caseInsensitiveAPIs = CaseInsensitiveMap.newHashMap();
      Optional.ofNullable(connections).ifPresent(caseInsensitiveAPIs::putAll);
      this.connections = caseInsensitiveAPIs;
    } else {
      this.connections = new HashMap<String, HttpAPIConfig>();
    }
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
  public int hashCode() {
    return Objects.hashCode(cacheResults, connections);
  }

 /*@Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("cacheResults", cacheResults)
      .add("connections", connections)
      .toString();
  }*/

  @JsonProperty("cacheResults")
  public boolean cacheResults() { return cacheResults; }

  @JsonProperty("connections")
  public Map<String, HttpAPIConfig> connections() { return connections; }
}
