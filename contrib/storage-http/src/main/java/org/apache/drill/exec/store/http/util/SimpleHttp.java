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


import okhttp3.Cache;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;


public class SimpleHttp {
  private static final Logger logger = LoggerFactory.getLogger(SimpleHttp.class);

  private final OkHttpClient client;

  private final HttpStoragePluginConfig config;

  private final FragmentContext context;

  public SimpleHttp(HttpStoragePluginConfig config, FragmentContext context) {
    this.config = config;
    this.context = context;
    client = setupServer();
  }

  public String get(String urlStr) {

    logger.debug("Attempting to connect to {}.", urlStr);
    Request request = new Request.Builder().url(urlStr).build();

    try {
      Response response = client.newCall(request).execute();
      if (!response.isSuccessful()) {
        throw UserException
          .dataReadError()
          .message("Error retrieving data:")
          .message("Response code: {}", response)
          .build(logger);
      }
      logger.debug("HTTP Request for {} successful.", urlStr);
      logger.debug("Headers: {} ", response.headers().toString());

      return response.body().string();
    } catch (IOException e) {
      // TODO throw Drill user exception;
      throw UserException
        .functionError()
        .message("Error retrieving data: {}", e.getMessage())
        .addContext(e.getMessage())
        .build(logger);
    }
  }

  public InputStream getInputStream(String urlStr) {
    Request request = new Request.Builder()
      .url(urlStr)
      .build();

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
    if (config.cacheResults) {
      int cacheSize = 10 * 1024 * 1024;   // TODO Add cache size in MB to config
      String drillTempDir;

      try {
        if (context.getOptions().getOption(ExecConstants.DRILL_TMP_DIR) != null) {
          drillTempDir = context.getOptions().getOption(ExecConstants.DRILL_TMP_DIR).string_val;
        } else {
          drillTempDir = System.getenv("DRILL_TMP_DIR");
        }
        File cacheDirectory = new File(drillTempDir);
        Cache cache = new Cache(cacheDirectory, cacheSize);
        logger.info("Caching HTTP Query Results at: {}", drillTempDir);

        builder.cache(cache);
      } catch (Exception e) {
        logger.warn("HTTP Storage plugin caching requires the DRILL_TMP_DIR to be configured. Please either set DRILL_TMP_DIR or disable HTTP caching.");
      }
    }

    return builder.build();
  }
}
