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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@JsonTypeName(HttpStoragePluginConfig.NAME)
public class HttpStoragePluginConfig extends StoragePluginConfigBase {

  private static final Logger logger = LoggerFactory.getLogger(HttpStoragePluginConfig.class);

  public static final String NAME = "http";

  public final Map<String, HttpAPIConfig> connections;

  public final boolean cacheResults;

  public final String proxyHost;

  public final int proxyPort;

  public final String proxyType;

  public final String proxyUsername;

  public final String proxyPassword;

  /**
   * Timeout in seconds.
   */
  public int timeout;

  @JsonCreator
  public HttpStoragePluginConfig(@JsonProperty("cacheResults") boolean cacheResults,
                                 @JsonProperty("connections") Map<String, HttpAPIConfig> connections,
                                 @JsonProperty("timeout") int timeout,
                                 @JsonProperty("proxyHost") String proxyHost,
                                 @JsonProperty("proxyPort") int proxyPort,
                                 @JsonProperty("proxyType") String proxyType,
                                 @JsonProperty("proxyUsername") String proxyUsername,
                                 @JsonProperty("proxyPassword") String proxyPassword
                                 ) {
    String tempProxyType;
    this.cacheResults = cacheResults;

    if (connections == null) {
      this.connections = new HashMap<>();
    } else {
      Map<String, HttpAPIConfig> caseInsensitiveAPIs = CaseInsensitiveMap.newHashMap();
      Optional.of(connections).ifPresent(caseInsensitiveAPIs::putAll);
      this.connections = caseInsensitiveAPIs;
    }

    this.timeout = timeout;
    this.proxyHost = proxyHost;
    this.proxyPort = proxyPort;
    this.proxyUsername = proxyUsername;
    this.proxyPassword = proxyPassword;

    // Validate Proxy Type
    if (!Strings.isNullOrEmpty(proxyType) &&
        !(proxyType.equalsIgnoreCase("direct") ||
        proxyType.equalsIgnoreCase("http") ||
        proxyType.equalsIgnoreCase("socks")))
    {
      throw UserException
        .validationError()
        .message("Invalid Proxy Type: %s.  Drill supports direct, http and socks proxies.", proxyType)
        .build(logger);
    } else {
      tempProxyType = Strings.isNullOrEmpty(proxyType) ? "" : proxyType.toLowerCase();
    }
    this.proxyType = tempProxyType;
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
      .field("proxyHost", proxyHost)
      .field("proxyPort", proxyPort)
      .field("proxyUsername", proxyUsername)
      .field("proxyPassword", proxyPassword)
      .field("proxyType", proxyType)
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

  @JsonProperty("proxyHost")
  public String proxyHost() { return proxyHost; }

  @JsonProperty("proxyPort")
  public int proxyPort() { return proxyPort; }

  @JsonProperty("proxyUsername")
  public String proxyUsername() { return proxyUsername; }

  @JsonProperty("proxyPassword")
  public String proxyPassword() { return proxyPassword; }

  @JsonProperty("proxyType")
  public String proxyType() { return proxyType; }
}
