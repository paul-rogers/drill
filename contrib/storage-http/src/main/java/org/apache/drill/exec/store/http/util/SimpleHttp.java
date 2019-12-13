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
package org.apache.drill.exec.store.http.util;


import jdk.internal.org.objectweb.asm.util.CheckAnnotationAdapter;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.http.HttpAPIConfig;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;


public class SimpleHttp {
  private static final Logger logger = LoggerFactory.getLogger(SimpleHttp.class);

  private final OkHttpClient client;

  private final HttpStoragePluginConfig config;

  private final FragmentContext context;

  private final HttpAPIConfig apiConfig;

  public SimpleHttp(HttpStoragePluginConfig config, FragmentContext context, String connectionName) {
    this.config = config;
    this.context = context;
    this.apiConfig = config.connections().get(connectionName);
    client = setupServer();
  }

  public InputStream getInputStream(String urlStr) {

    Request.Builder requestBuilder = new Request.Builder()
      .url(urlStr);

    // Add headers to request
    if (apiConfig.headers() != null) {
      for (Map.Entry<String, String> entry : apiConfig.headers().entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        requestBuilder.addHeader(key, value);
      }
    }

    // Build the request
    Request request = requestBuilder.build();

    try {
      Response response = client.newCall(request).execute();
      if (!response.isSuccessful()) {
        throw UserException
          .dataReadError()
          .message("Error retrieving data: " + response.code() + " " + response.message())
          .addContext("Response code: " + response)
          .build(logger);
      }
      logger.debug("HTTP Request for {} successful.", urlStr);
      logger.debug("Headers: {} ", response.headers().toString());

      return response.body().byteStream();
    } catch (IOException e) {
      // TODO throw Drill user exception;
      throw UserException
        .functionError()
        .message("Error retrieving data")
        .addContext(e.getMessage())
        .build(logger);
    }
  }

  /**
   * Function configures the server.
   * @return OkHttpClient configured server
   */
  private OkHttpClient setupServer() {
    Builder builder = new OkHttpClient.Builder();

    // Set up the HTTP Cache.   Future possibilities include making the cache size and retention configurable but
    // right now it is on or off.  The writer will write to the Drill temp directory if it is accessible and
    // output a warning if not.
    if (config.cacheResults()) {
      setupCache(builder);
    }

    return builder.build();
  }

  /**
   * This function accepts a Builder object as input and configures response caching. In order for
   * caching to work, the DRILL_TMP_DIR variable must be set either as a system environment variable or in the
   * Drill configurations.
   *
   * The function will attempt to get the DRILL_TMP_DIR from these places, and if it cannot, it will issue a warning in the logger.
   * @param builder Builder the Builder object to which the cacheing is to be configured
   */
  private void setupCache(Builder builder) {
    int cacheSize = 10 * 1024 * 1024;   // TODO Add cache size in MB to config
    String drillTempDir;

    try {
      if (context.getOptions().getOption(ExecConstants.DRILL_TMP_DIR) != null) {
        drillTempDir = context.getOptions().getOption(ExecConstants.DRILL_TMP_DIR).string_val;
      } else {
        drillTempDir = System.getenv("DRILL_TMP_DIR");
      }
      File cacheDirectory = new File(drillTempDir);
      if (cacheDirectory == null) {
        logger.warn("HTTP Storage plugin caching requires the DRILL_TMP_DIR to be configured. Please either set DRILL_TMP_DIR or disable HTTP caching.");
      } else {
        Cache cache = new Cache(cacheDirectory, cacheSize);
        logger.info("Caching HTTP Query Results at: {}", drillTempDir);

        builder.cache(cache);
      }
    } catch (Exception e) {
      logger.warn("HTTP Storage plugin caching requires the DRILL_TMP_DIR to be configured. Please either set DRILL_TMP_DIR or disable HTTP caching.");
    }
  }

}
