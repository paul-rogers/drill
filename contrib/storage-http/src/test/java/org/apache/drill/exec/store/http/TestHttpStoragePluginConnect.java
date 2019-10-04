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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.drill.exec.store.http.util.JsonConverter;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;


@Ignore("requires remote http server")
public class TestHttpStoragePluginConnect {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestHttpStoragePluginConnect.class);

  public static final String API_MASTER = "https://api.sunrise-sunset.org/json";

  /**
   * This test verifies that the plugin can make a successful connection and receive the results
   * The API used here returns JSON. Since the response will vary, we're just looking to see whether the response
   * contains {"status:"OK"}
   */
  @Test
  public void testHttpConnection() {
    SimpleHttp http = new SimpleHttp();
    String result = http.get("https://api.sunrise-sunset.org/json?lat=36.7201600&lng=-4.4203400&date=today");
    Assert.assertTrue(result, result.contains("\"status\":\"OK\""));
  }

  /**
   * This test verifies that the JSON parser succesfully parses the incoming JSON data.  Response should be:
   * {
   *       "results":
   *       {
   *         "sunrise":"7:27:02 AM",
   *         "sunset":"5:05:55 PM",
   *         "solar_noon":"12:16:28 PM",
   *         "day_length":"9:38:53",
   *         "civil_twilight_begin":"6:58:14 AM",
   *         "civil_twilight_end":"5:34:43 PM",
   *         "nautical_twilight_begin":"6:25:47 AM",
   *         "nautical_twilight_end":"6:07:10 PM",
   *         "astronomical_twilight_begin":"5:54:14 AM",
   *         "astronomical_twilight_end":"6:38:43 PM"
   *       },
   *        "status":"OK"
   *     }
   */
  @Test
  public void testJSONParsing() {
    SimpleHttp http = new SimpleHttp();
    String result = http.get("https://api.sunrise-sunset.org/json?lat=36.7201600&lng=-4.4203400&date=today");
    String key = "results";

    // Make sure there was a successful query
    Assert.assertTrue(result, result.contains("\"status\":\"OK\""));

    //Parse the JSON and check the results
    JsonNode root = key.length() == 0 ? JsonConverter.parse(result) : JsonConverter.parse(result, key);
    logger.debug("response object count {}", root.size());
    Assert.assertTrue(root.size() == 10);
  }
}
