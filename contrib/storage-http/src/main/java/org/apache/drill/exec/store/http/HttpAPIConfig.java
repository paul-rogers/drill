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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(value = {"storageformat"}, ignoreUnknown = true)
@JsonTypeName("http-api-config")
public class HttpAPIConfig {

  private static final Logger logger = LoggerFactory.getLogger(HttpAPIConfig.class);

  private final String url;

  private final String method;

  private final Map<String, String> headers;

  private final String authType;

  private final String username;

  private final String password;

  public HttpAPIConfig(@JsonProperty("url") String url,
                       @JsonProperty("method") String method,
                       @JsonProperty("headers") Map<String, String> headers,
                       @JsonProperty("authType") String authType,
                       @JsonProperty("username") String username,
                       @JsonProperty("password") String password) {
    this.url = url;
    if (method.toLowerCase().equals("get") || method.toLowerCase().equals("post")) {
      this.method = method.toLowerCase();
    } else {
      this.method = "get";
    }
    this.headers = headers;
    this.authType = authType;
    this.username = username;
    this.password = password;
  }

  @JsonProperty("url")
  public String url() { return url; }

  @JsonProperty("method")
  public String method() { return method; }

  @JsonProperty("headers")
  public Map<String, String> headers() { return headers; }

  @JsonProperty("authType")
  public String authType() { return authType; }

  @JsonProperty("username")
  public String username() { return username; }

  @JsonProperty("password")
  public String password() { return password; }

  @Override
  public int hashCode() {
    return Arrays.hashCode(
      new Object[]{url, method, headers, authType, username, password});
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("url", url)
      .add("method", method)
      .add("headers", headers)
      .add("authType", authType)
      .add("username", username)
      .add("password", password)
      .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    HttpAPIConfig other = (HttpAPIConfig) obj;
    return Objects.equals(url, other.url)
      && Objects.equals(method, other.method)
      && Objects.equals(headers, other.headers)
      && Objects.equals(authType, other.authType)
      && Objects.equals(username, other.username)
      && Objects.equals(password, other.password);
  }
}
