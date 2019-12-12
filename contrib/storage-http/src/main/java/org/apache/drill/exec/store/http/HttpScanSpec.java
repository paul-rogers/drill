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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.drill.shaded.guava.com.google.common.base.MoreObjects;

@JsonTypeName("http-scan-spec")
public class HttpScanSpec {
  private final String database;

  private final Map<String, Object> args = new HashMap<>();

  private final String tableName;

  private final String schemaName;

  private final HttpStoragePluginConfig config;

  @JsonCreator
  public HttpScanSpec(@JsonProperty("schemaName") String schemaName,
                      @JsonProperty("database") String database,
                      @JsonProperty("tableName") String tableName,
                      @JsonProperty("plugin") HttpStoragePlugin plugin) {
    this.database = database;
    this.tableName = tableName;
    this.schemaName = schemaName;
    this.config = plugin.getConfig();
  }

  @JsonProperty("database")
  public String database() {
    return database;
  }

  @JsonProperty("args")
  public Map<String, Object> args() {
    return args;
  }

  @JsonProperty("tableName")
  public String tableName() {
    return tableName;
  }

  /*
  @JsonProperty("config")
  public HttpStoragePluginConfig config() {
    return config;
  }*/


  @JsonIgnore
  public String getURL() {
    if (args.size() == 0) {
      return database();
    }
    Joiner j = Joiner.on('&');

    String url = config.getConnections().get(database).url();
    String argStr = j.withKeyValueSeparator("=").join(args);
    if (url.endsWith("?")) {
      url += argStr;
    } else if (url.contains("?")) {
      url += '&' + argStr;
    } else {
      url += '?' + argStr;
    }
    return url;
  }

  @JsonIgnore
  public void merge(HttpScanSpec that) {
    for (Map.Entry<String, Object> entry : that.args.entrySet()) {
      this.args.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("database", database)
      .add("tableName", tableName)
      .add("schemaName", schemaName)
      .add("config", config)
      .toString();
  }
}
