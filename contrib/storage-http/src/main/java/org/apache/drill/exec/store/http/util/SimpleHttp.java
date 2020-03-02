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
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.http.HttpAPIConfig;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


/**
 * This class performs the actual HTTP requests for the HTTP Storage Plugin. The core method is the getInputStream()
 * method which accepts a url and opens an InputStream with that URL's contents.
 */
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
    client = setupHttpClient();
  }



  public InputStream getInputStream(String urlStr) {
    Request.Builder requestBuilder;

    // The configuration does not allow for any other request types other than POST and GET.
    if (apiConfig.method().equals("get")) {
      // Handle GET requests
      requestBuilder = new Request.Builder().url(urlStr);
    } else {
      // Handle POST requests
      FormBody.Builder formBodyBuilder = buildPostBody();
      requestBuilder = new Request.Builder()
        .url(urlStr)
        .post(formBodyBuilder.build());
    }

    // Add headers to request
    if (apiConfig.headers() != null) {
      for (Map.Entry<String, String> entry : apiConfig.headers().entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        requestBuilder.addHeader(key, value);
      }
    }

    // Build the request object
    Request request = requestBuilder.build();
    logger.debug("Headers: {}", request.headers());

    try {
      // Execute the request
      Response response = client
        .newCall(request)
        .execute();

      // If the request is unsuccessful, throw a UserException
      if (!response.isSuccessful()) {
        throw UserException
          .dataReadError()
          .message("Error retrieving data from HTTP Storage Plugin: " + response.code() + " " + response.message())
          .addContext("Response code: ", response.code())
          .build(logger);
      }
      logger.debug("HTTP Request for {} successful.", urlStr);
      logger.debug("Response Headers: {} ", response.headers().toString());

      // Return the InputStream of the response
      return Objects.requireNonNull(response.body()).byteStream();
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Error retrieving data from HTTP Storage Plugin. %s", e.getMessage())
        .addContext("URL Requested:" + urlStr)
        .build(logger);
    }
  }

  /**
   * Function configures the OkHTTP3 server object with configuration info from the user.
   *
   * @return OkHttpClient configured server
   */
  private OkHttpClient setupHttpClient() {
    Builder builder = new OkHttpClient.Builder();

    // Set up the HTTP Cache.   Future possibilities include making the cache size and retention configurable but
    // right now it is on or off.  The writer will write to the Drill temp directory if it is accessible and
    // output a warning if not.
    if (config.cacheResults()) {
      setupCache(builder);
    }

    // If the API uses basic authentication add the authentication code.
    if (apiConfig.authType().toLowerCase().equals("basic")) {
      logger.debug("Adding Interceptor");
      builder.addInterceptor(new BasicAuthInterceptor(apiConfig.userName(), apiConfig.password()));
    }

    // Set timeout
    builder.connectTimeout(config.timeout(), TimeUnit.SECONDS);
    builder.writeTimeout(config.timeout(), TimeUnit.SECONDS);
    builder.readTimeout(config.timeout(), TimeUnit.SECONDS);

    return builder.build();
  }

  /**
   * This function accepts a Builder object as input and configures response caching. In order for
   * caching to work, the DRILL_TMP_DIR variable must be set either as a system environment variable or in the
   * Drill configurations.
   * <p>
   * The function will attempt to get the DRILL_TMP_DIR from these places, and if it cannot, it will issue a warning in the logger.
   *
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
        logger.debug("Caching HTTP Query Results at: {}", drillTempDir);

        builder.cache(cache);
      }
    } catch (Exception e) {
      logger.warn("HTTP Storage plugin caching requires the DRILL_TMP_DIR to be configured. Please either set DRILL_TMP_DIR or disable HTTP caching.");
    }
  }

  /**
   * This function accepts text from a post body in the format:
   * key1=value1
   * key2=value2
   *
   * and creates the appropriate headers.
   *
   * @return FormBodu.Builder The populated formbody builder
   */
  private FormBody.Builder buildPostBody() {
    final Pattern postBpdyPattern = Pattern.compile("^.+=.+$");

    FormBody.Builder formBodyBuilder = new FormBody.Builder();
    String[] lines = apiConfig.postBody().split("\\r?\\n");
    for(String line : lines) {

      // If the string is in the format key=value split it,
      // Otherwise ignore
      if (postBpdyPattern.matcher(line).find()) {
        //Split into key/value
        String[] parts = line.split("=");
        formBodyBuilder.add(parts[0], parts[1]);
      }
    }
    return formBodyBuilder;
  }

  /**
   * This class intercepts requests and adds authentication headers to the request
   */
  public static class BasicAuthInterceptor implements Interceptor {
    private final String credentials;

    public BasicAuthInterceptor(String user, String password) {
      credentials = Credentials.basic(user, password);
      logger.debug("Intercepting request adding creds: {}", credentials);
    }

    @NotNull
    @Override
    public Response intercept(Chain chain) throws IOException {
      logger.debug("Adding headers post intercept{}", credentials);
      // Get the existing request
      Request request = chain.request();

      // Replace with new request containing the authorization headers and previous headers
      Request authenticatedRequest = request.newBuilder().header("Authorization", credentials).build();
      return chain.proceed(authenticatedRequest);
    }
  }
}
