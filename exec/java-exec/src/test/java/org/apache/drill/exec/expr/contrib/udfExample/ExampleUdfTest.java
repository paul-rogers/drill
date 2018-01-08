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
package org.apache.drill.exec.expr.contrib.udfExample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.contrib.udfExample.Log2Function;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.LogFixture.LogFixtureBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class ExampleUdfTest extends ClusterTest {

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.classpath.scanning.cache.enabled", false);
    startCluster(builder);
  }

  @Test
  public void demoTest() {
    String sql = "SELECT * FROM `cp`.`employee.json` LIMIT 3";
    client.queryBuilder().sql(sql).printCsv();
  }

  @Test
  public void testAnnotation() {
    Class<? extends DrillSimpleFunc> fnClass = Log2Function.class;
    FunctionTemplate fnDefn = fnClass.getAnnotation(FunctionTemplate.class);
    assertNotNull(fnDefn);
    assertEquals("log2", fnDefn.name());
    assertEquals(FunctionScope.SIMPLE, fnDefn.scope());
    assertEquals(NullHandling.NULL_IF_NULL, fnDefn.nulls());
  }

  @Test
  public void testFnOrig() {
    Log2Function logFn = new Log2Function();
    logFn.setup();
    logFn.x = new Float8Holder();
    logFn.out = new Float8Holder();

    logFn.x.value = 2;
    logFn.eval();
    assertEquals(1.0D, logFn.out.value, 0.001D);
  }

  private static Log2Function instance() {
    Log2Function fn = new Log2Function();
    fn.x = new Float8Holder();
    fn.out = new Float8Holder();
    fn.setup();
    return fn;
  }

  public static double call(Log2Function fn, double x) {
    fn.x.value = x;
    fn.eval();
    return fn.out.value;
  }

  @Test
  public void testFn() {
    Log2Function logFn = instance();

    assertEquals(1D, call(logFn, 2), 0.001D);
    assertEquals(2D, call(logFn, 4), 0.001D);
    assertEquals(-1D, call(logFn, 1.0/2.0), 0.001D);
  }


  @Test
  public void testIntegration2() {
    LogFixtureBuilder logBuilder = new LogFixtureBuilder()
//        .rootLogger(Level.INFO)
        ;
    try (LogFixture logFixture = logBuilder.build()) {
      String sql = "SELECT log2(4) FROM (VALUES (1))";
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void testIntegration() {
    String sql = "SELECT log2w(4) FROM (VALUES (1))";
    client.queryBuilder().sql(sql).printCsv();
  }

  @Test
  public void testImpl() {
    assertEquals(1D, Log2Wrapper.log2(2), 0.001D);
    assertEquals(2D, Log2Wrapper.log2(4), 0.001D);
    assertEquals(-1D, Log2Wrapper.log2(1.0/2.0), 0.001D);
  }
}
