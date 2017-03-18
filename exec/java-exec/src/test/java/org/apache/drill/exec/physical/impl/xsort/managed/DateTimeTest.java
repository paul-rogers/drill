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
 ******************************************************************************/
package org.apache.drill.exec.physical.impl.xsort.managed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.FixtureBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DateTimeTest extends ClusterTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .saveProfiles()
        .maxParallelization(1)
        ;
    startCluster(builder);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void testDateConstants() throws RpcException {
    testDateLong(client, "date '1970-1-1'", 0);
    testDateLong(client, "date '1970-1-2'", 24 * 60 * 60 * 1000);
    testDateLong(client, "date '1969-12-31'", -24 * 60 * 60 * 1000);
  }

  private void testDateLong(ClientFixture client, String constExpr, long expected) throws RpcException {
    String sql = "SELECT " + constExpr + " FROM (VALUES(1))";
    long value = client.queryBuilder().sql(sql).singletonLong();
    System.out.print(constExpr + " = ");
    System.out.println(value);
    assertEquals(expected, value);
  }

//  private long runLongQuery(ClientFixture client, String sql) throws RpcException {
//    return client.queryBuilder().sql(sql).singletonLong();
//  }

  @Test
  public void testTimeConstants() throws RpcException {
    // Verify time constants.

    testTime(client, "time '00:00:00'", 0);
    testTime(client, "time '00:00:01'", 1000);
    testTime(client, "time '10:00:00'", 10 * 60 * 60 * 1000);
  }

  private int runIntQuery(ClientFixture client, String sql) throws RpcException {
    return client.queryBuilder().sql(sql).singletonInt();
  }

  private void testTime(ClientFixture client, String constExpr, int expected) throws RpcException {
    String sql = "SELECT " + constExpr + " FROM (VALUES(1))";
    int value = runIntQuery(client, sql);
    assertEquals(expected, value);
  }

  @Test
  public void testNowVector() throws RpcException {
    // Timestamp value returned is in server time zone. Because the server
    // is embedded, the value is the same as the default time zone.
    // Ensure that the difference between returned time and UTC is the
    // same as the server (and default) time zone offset.

    long before = System.currentTimeMillis();
    String sql = "SELECT NOW() FROM (VALUES(1))";
    long value = client.queryBuilder().sql(sql).singletonLong();
    long after = System.currentTimeMillis();
    int offset = TimeZone.getDefault().getOffset(before);
    long utc = value - offset;
    assertTrue( before < utc && utc < after );
  }

//  private void testLocalDate(ClientFixture client, String constExpr, long expectedUTC) throws RpcException {
//    String sql = "SELECT " + constExpr + " FROM (VALUES(1))";
//    long value = runLongQuery(client, sql);
//    int offset = TimeZone.getDefault().getOffset(expectedUTC);
//    assertEquals(offset, value - expectedUTC, 1000);
//  }

  // Try the same query with JDBC. JDBC will do the conversion using the
  // client time zone (which, here, is the same as the embedded server
  // time zone, but won't be in the general case.
  // Requires that the Drill JDBC project be on the launch class path.

  @Test
  public void testNowJDBC() throws RpcException, SQLException {
    long before = System.currentTimeMillis();
    try (Connection conn = cluster.jdbcConnection()) {
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SELECT NOW() FROM (VALUES(1))");) {
        assertTrue(rs.next());
        long after = System.currentTimeMillis();
        long now = rs.getTimestamp(1).getTime();
        assertTrue(before < now && now < after);
      }
    }
  }

  @Test
  public void testCurrentTimestamp() throws Exception {
    String sql = "SELECT CURRENT_TIMESTAMP FROM (VALUES(1))";
    System.out.println(client.queryBuilder().sql(sql).explainJson());
    long before = System.currentTimeMillis();
    long value = client.queryBuilder().sql(sql).singletonLong();
    long after = System.currentTimeMillis();
    int offset = TimeZone.getDefault().getOffset(before);
    long utc = value - offset;
    assertTrue( before < utc && utc < after );
  }

  public static final int MS_PER_DAY = 24 * 60 * 60 * 1000;

  private long timePart(long dateTime) {
    return dateTime % (MS_PER_DAY);
  }

  private long datePart(long dateTime) {
    return dateTime - timePart(dateTime);
  }

  @Test
  public void testCurrentDate() throws Exception {
    String sql = "SELECT CURRENT_DATE FROM (VALUES(1))";
    System.out.println(client.queryBuilder().sql(sql).explainJson());
    long before = System.currentTimeMillis();
    long value = client.queryBuilder().sql(sql).singletonLong();
    long after = System.currentTimeMillis();
    int offset = TimeZone.getDefault().getOffset(before);
    assertTrue( datePart(before + offset) <= value && value <= datePart(after + offset) );
  }

  @Test
  public void testCurrentTime() throws Exception {
    String sql = "SELECT CURRENT_TIME() FROM (VALUES(1))";
    System.out.println(client.queryBuilder().sql(sql).explainJson());
    long before = System.currentTimeMillis();
    long value = client.queryBuilder().sql(sql).singletonLong();
    long after = System.currentTimeMillis();
    // Won't work right at midnight!
    assertTrue( timePart(before) < timePart(value) &&
                timePart(value) < timePart(after) );
  }
}
