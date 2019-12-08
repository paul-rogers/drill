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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
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


public class TestHttpPlugin extends ClusterTest {
  private static final Logger logger = LoggerFactory.getLogger(TestHttpPlugin.class);

  private final String TEST_JSON_RESPONSE = "{\"results\":{\"sunrise\":\"6:13:58 AM\",\"sunset\":\"5:59:55 PM\",\"solar_noon\":\"12:06:56 PM\",\"day_length\":\"11:45:57\"," +
    "\"civil_twilight_begin\":\"5:48:14 AM\",\"civil_twilight_end\":\"6:25:38 PM\",\"nautical_twilight_begin\":\"5:18:16 AM\",\"nautical_twilight_end\":\"6:55:36 PM\",\"astronomical_twilight_begin\":\"4:48:07 AM\",\"astronomical_twilight_end\":\"7:25:45 PM\"},\"status\":\"OK\"}";


  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    HttpStoragePluginConfig  storagePluginConfig = new HttpStoragePluginConfig("https://api.sunrise-sunset.org/");
    storagePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate(HttpStoragePluginConfig.NAME, storagePluginConfig, true);

    HttpStoragePluginConfig  mockStoragePluginConfig = new HttpStoragePluginConfig("http://localhost:8088/");
    mockStoragePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate("mockRestServer", mockStoragePluginConfig, true);
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
      .addRow("http", "http")
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
    server.shutdown();
  }
}
