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

import org.apache.drill.shaded.guava.com.google.common.base.MoreObjects;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;


@JsonTypeName(HttpStoragePluginConfig.NAME)
public class HttpStoragePluginConfig extends StoragePluginConfigBase {
  private static final Logger logger = LoggerFactory.getLogger(HttpStoragePluginConfig.class);

  public static final String NAME = "http";

  public final String connection;

  public final boolean cacheResults;

  @JsonCreator
  public HttpStoragePluginConfig(@JsonProperty("connection") String connection,
                                 @JsonProperty("cacheResults") boolean cacheResults) {
    logger.debug("Initialize HttpStoragePluginConfig {}", connection);
    this.connection = connection;
    this.cacheResults = cacheResults;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    HttpStoragePluginConfig thatConfig = (HttpStoragePluginConfig) that;
    return this.connection.equals(thatConfig.connection) &&
      this.cacheResults == thatConfig.cacheResults;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(connection);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("connection", connection)
      .add("cacheResults", cacheResults)
      .toString();
  }

  @JsonProperty("connection")
  public String getConnection() {
    return connection;
  }

  @JsonProperty("cacheResults")
  public boolean getCacheResults() { return cacheResults; }
}
