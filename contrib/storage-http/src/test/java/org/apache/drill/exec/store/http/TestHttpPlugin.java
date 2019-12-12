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
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import okio.Okio;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class TestHttpPlugin extends ClusterTest {
  private static final Logger logger = LoggerFactory.getLogger(TestHttpPlugin.class);

  private final String TEST_JSON_RESPONSE = "{\"results\":{\"sunrise\":\"6:13:58 AM\",\"sunset\":\"5:59:55 PM\",\"solar_noon\":\"12:06:56 PM\",\"day_length\":\"11:45:57\"," +
    "\"civil_twilight_begin\":\"5:48:14 AM\",\"civil_twilight_end\":\"6:25:38 PM\",\"nautical_twilight_begin\":\"5:18:16 AM\",\"nautical_twilight_end\":\"6:55:36 PM\",\"astronomical_twilight_begin\":\"4:48:07 AM\",\"astronomical_twilight_end\":\"7:25:45 PM\"},\"status\":\"OK\"}";

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));

    dirTestWatcher.copyResourceToRoot(Paths.get("data/"));


    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    HttpStoragePluginConfig  storagePluginConfig = new HttpStoragePluginConfig("https://api.sunrise-sunset.org/", true, null);
    storagePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate(HttpStoragePluginConfig.NAME, storagePluginConfig, true);

    HttpStoragePluginConfig  mockStoragePluginConfig = new HttpStoragePluginConfig("http://localhost:8088/", false, null);
    mockStoragePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate("mockRestServer", mockStoragePluginConfig, true);



    HttpAPIConfig apiConfig = new HttpAPIConfig("https://api.worldtradingdata.com/api/v1/stock?symbol=SNAP,TWTR,VOD" +
      ".L&api_token=zuHlu2vZaehdZN6GmJdTiVlp7xgZn6gl6sfgmI4G6TY4ej0NLOzvy0TUl4D4", "get", null, null, null, null);

    Map<String, HttpAPIConfig> configs = new HashMap<String, HttpAPIConfig>();
    configs.put("stock", apiConfig);

    HttpStoragePluginConfig mockStorageConfigWithWorkspace = new HttpStoragePluginConfig("none", true, configs);
    mockStorageConfigWithWorkspace.setEnabled(true);
    pluginRegistry.createOrUpdate("api", mockStorageConfigWithWorkspace, true);

  }

  @Test
  public void test() throws Exception {
    String sql = "SELECT * FROM api.jira.`arg1=true`";
    queryBuilder().sql(sql).run();
  }


  @Test
  public void verifyPluginConfig() throws Exception {
    String sql = "SELECT SCHEMA_NAME, TYPE FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE TYPE='http'";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    logger.debug("Query Results: {}", results.toString());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("SCHEMA_NAME", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("TYPE", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("api", "http")
      .addRow("http", "http")
      .addRow("mockrestserver", "http")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  /**
   * This test evaluates the HTTP plugin with the results from an API that returns the sunrise/sunset times for a given lat/long and date.
   * API documentation is available here: https://sunrise-sunset.org/api
   *
   * This evaluates a `SELECT *` query.
   *
   * The API returns results in the following format:
   *
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
   * }
   *
   * @throws Exception Throws exception if something goes awry
   */
  @Test
  @Ignore("Requires Remote Server")
  public void simpleStarQuery() throws Exception {
    String sql = "SELECT * FROM http.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";

    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMap("results")
      .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("solar_noon", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("day_length", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("civil_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("civil_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("nautical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("nautical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("astronomical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("astronomical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .resumeSchema()
      .add("status", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow( mapValue("6:13:58 AM", "5:59:55 PM", "12:06:56 PM", "11:45:57", "5:48:14 AM", "6:25:38 PM", "5:18:16 AM", "6:55:36 PM", "4:48:07 AM", "7:25:45 PM"), "OK")
      .build();

    int resultCount =  results.rowCount();
    new RowSetComparison(expected).verifyAndClearAll(results);

    assertEquals(1,  resultCount);
  }

  @Test
  @Ignore("Requires Remote Server")
  public void simpleSpecificQuery() throws Exception {
    String sql = "SELECT t1.results.sunrise AS sunrise, t1.results.sunset AS sunset FROM http.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02` AS t1";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    logger.debug("Query Results: {}", results.toString());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("6:13:58 AM", "5:59:55 PM")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
   public void testSerDe() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start(8088);
    logger.debug("Establishing Mock Server at: {}", server.getHostName());

    server.enqueue(
      new MockResponse().setResponseCode(200)
        .setBody(TEST_JSON_RESPONSE)
    );

    String sql = "SELECT COUNT(*) FROM mockRestServer.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match",1L, cnt);
    server.shutdown();
  }

  @Test
  public void simpleTestWithMockServer() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start(8088);
    logger.debug("Establishing Mock Server at: {}", server.getHostName());

    server.enqueue(
      new MockResponse().setResponseCode(200)
        .setBody(TEST_JSON_RESPONSE)
    );

    String sql = "SELECT * FROM mockRestServer.`json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMap("results")
      .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("solar_noon", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("day_length", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("civil_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("civil_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("nautical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("nautical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("astronomical_twilight_begin", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("astronomical_twilight_end", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .resumeSchema()
      .add("status", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow( mapValue("6:13:58 AM", "5:59:55 PM", "12:06:56 PM", "11:45:57", "5:48:14 AM", "6:25:38 PM", "5:18:16 AM", "6:55:36 PM", "4:48:07 AM", "7:25:45 PM"), "OK")
      .build();

    int resultCount =  results.rowCount();
    new RowSetComparison(expected).verifyAndClearAll(results);

    assertEquals(1,  resultCount);
    server.shutdown();
  }

  @Test
  public void specificTestWithMockServer() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start(8088);
    logger.debug("Establishing Mock Server at: {}", server.getHostName());

    server.enqueue(
      new MockResponse().setResponseCode(200)
        .setBody(TEST_JSON_RESPONSE)
    );

    String sql = "SELECT t1.results.sunrise AS sunrise, t1.results.sunset AS sunset FROM mockRestServer.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02` AS t1";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    logger.debug("Query Results: {}", results.toString());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("6:13:58 AM", "5:59:55 PM")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
    server.shutdown();
  }

  @Test
  public void testSlowResponse() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start(8088);
    logger.debug("Establishing Mock Server at: {}", server.getHostName());

    server.enqueue(
      new MockResponse().setResponseCode(200)
        .setBody(TEST_JSON_RESPONSE)
        .throttleBody(64, 4, TimeUnit.SECONDS)
    );

    String sql = "SELECT t1.results.sunrise AS sunrise, t1.results.sunset AS sunset FROM mockRestServer.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02` AS t1";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    logger.debug("Query Results: {}", results.toString());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("sunrise", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("sunset", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("6:13:58 AM", "5:59:55 PM")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
    server.shutdown();
  }

  @Test
  public void testZeroByteResponse() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start(8088);
    logger.debug("Establishing Mock Server at: {}", server.getHostName());

    server.enqueue(
      new MockResponse().setResponseCode(200)
        .setBody("")
    );

    String sql = "SELECT * FROM mockRestServer.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertNull(results);
    server.shutdown();
  }

  @Test
  public void testEmptyJSONObjectResponse() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start(8088);
    logger.debug("Establishing Mock Server at: {}", server.getHostName());

    server.enqueue(
      new MockResponse().setResponseCode(200)
        .setBody("{}")
    );

    String sql = "SELECT * FROM mockRestServer.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    logger.debug("Query Results: {}", results.toString());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("**", TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow((Integer)null)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
    server.shutdown();
  }

  @Test
  public void testErrorResponse() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start(8088);
    logger.debug("Establishing Mock Server at: {}", server.getHostName());

    server.enqueue(
      new MockResponse().setResponseCode(404)
        .setBody("{}")
    );

    String sql = "SELECT * FROM mockRestServer.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02`";

    try {
      RowSet results = client.queryBuilder().sql(sql).rowSet();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("DATA_READ ERROR: Error retrieving data: 404 Client Error"));
    }
    server.shutdown();
  }


  @Test
  public void specificTestWithLargeFile() throws Exception {
    MockWebServer server = new MockWebServer();
    server.start(8088);
    logger.debug("Establishing Mock Server at: {}", server.getHostName());

    File largeFile = new File(dirTestWatcher.getRootDir().getAbsolutePath() + "/" + "data/sample_data.json");
    server.enqueue(
      new MockResponse().setResponseCode(200)
        .setBody(fileToBytes(largeFile))
    );

    String sql = "SELECT * " +
      "FROM mockRestServer.`/json?lat=36.7201600&lng=-4.4203400&date=2019-10-02` " +
      "LIMIT 1";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    logger.debug("Query Results: {}", results.toString());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
      .add("first_name", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("last_name", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("email", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("gender", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("ip_address", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("random_number", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "Eba", "Le Sarr", "elesarr0@springer.com", "Female", "249.113.19.117", 92.3)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
    server.shutdown();
  }

  private Buffer fileToBytes(File file) throws IOException {
    Buffer result = new Buffer();
    result.writeAll(Okio.source(file));
    return result;
  }

  /*
Odd format (that requires, say, all text mode)
Very large output (which will blow up the current load-to-string implementation)
   */

}
