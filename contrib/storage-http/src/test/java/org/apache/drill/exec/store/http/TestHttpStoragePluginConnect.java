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
}
