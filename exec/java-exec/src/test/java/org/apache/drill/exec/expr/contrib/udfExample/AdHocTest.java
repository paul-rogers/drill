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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.contrib.udfExample.CountLettersFunctions.DupItFunction;
import org.apache.drill.exec.expr.contrib.udfExample.WeightedAverageImpl.WeightedAvgFunc;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.LogFixture.LogFixtureBuilder;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Charsets;

import ch.qos.logback.classic.Level;

public class AdHocTest extends DrillTest {

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @Test
  public void test() throws Exception {
    testAgg();
  }

  @Test
  public void testLog2() throws Exception {
    LogFixtureBuilder logBuilder = new LogFixtureBuilder()
//        .rootLogger(Level.INFO)
        ;
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.classpath.scanning.cache.enabled", false);
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT log2w(4) FROM (VALUES (1))";
      client.queryBuilder().sql(sql).printCsv();

      RowSet actual = client.queryBuilder().sql(sql).rowSet();

      BatchSchema expectedSchema = new SchemaBuilder()
          .addNullable("EXPR$0", MinorType.FLOAT8)
          .build();

      RowSet expected = client.rowSetBuilder(expectedSchema)
          .addRow(2.0D)
          .build();

      new RowSetComparison(expected).verifyAndClearAll(actual);
    }
  }

  @Test
  public void testCountAscii() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.classpath.scanning.cache.enabled", false);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT word, countLettersAscii(word) FROM `cp`.`letterInput.json`";
      client.queryBuilder().sql(sql).printCsv();
//      sql = "SELECT countLettersAscii('Αθήνα?') FROM (VALUES (1))";
//      client.queryBuilder().sql(sql).printCsv();
//      sql = "SELECT countLettersAscii('-Mосква-') FROM (VALUES (1))";
//      client.queryBuilder().sql(sql).printCsv();
//      sql = "SELECT countLettersAscii('海.') FROM (VALUES (1))";
//      client.queryBuilder().sql(sql).printCsv();
      System.out.println(Character.isAlphabetic('海'));
      sql = "SELECT word, countLetters(word) FROM `cp`.`letterInput.json`";
      client.queryBuilder().sql(sql).printCsv();
      sql = "SELECT word, extractLetters(word) FROM `cp`.`letterInput.json`";
//      sql = "SELECT countLetters('Αθήνα?') FROM (VALUES (1))";
      client.queryBuilder().sql(sql).printCsv();
//      sql = "SELECT countLetters('-Mосква-') FROM (VALUES (1))";
//      client.queryBuilder().sql(sql).printCsv();
//      sql = "SELECT countLetters('海.') FROM (VALUES (1))";
//      client.queryBuilder().sql(sql).printCsv();
//      sql = "SELECT extractLetters('aA 12 zZ!') FROM (VALUES (1))";
//      client.queryBuilder().sql(sql).printCsv();
//      sql = "SELECT extractLetters('Αθήνα?') FROM (VALUES (1))";
//      client.queryBuilder().sql(sql).printCsv();
//      sql = "SELECT extractLetters('-Mосква-') FROM (VALUES (1))";
//      client.queryBuilder().sql(sql).printCsv();
//      sql = "SELECT extractLetters('海.') FROM (VALUES (1))";
    }
  }


  @Test
  public void testIt() {
    System.out.println(Character.isAlphabetic('次'));
  }

  @Test
  public void example() throws Exception {
    LogFixtureBuilder logBuilder = new LogFixtureBuilder()
        .logger("org.apache.drill.exec.compile.JaninoClassCompiler", Level.DEBUG)
//        .rootLogger(Level.DEBUG)
        ;
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.classpath.scanning.cache.enabled", false);
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT extractLetters('海.') FROM (VALUES (1))";
//      String sql = "SELECT a, b + c AS d FROM cp.`example.json`";
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void dummy1() throws Exception {
    LogFixtureBuilder logBuilder = new LogFixtureBuilder()
//        .logger("org.apache.drill.exec.compile.JaninoClassCompiler", Level.DEBUG)
//        .rootLogger(Level.DEBUG)
        ;
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.classpath.scanning.cache.enabled", false);
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT word FROM `cp`.`letterInput.json`";
//      String sql = "SELECT a, b + c AS d FROM cp.`example.json`";
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void dummy2() throws Exception {
    LogFixtureBuilder logBuilder = new LogFixtureBuilder()
        .logger("org.apache.drill.exec.compile.JaninoClassCompiler", Level.DEBUG)
//        .rootLogger(Level.DEBUG)
        ;
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.classpath.scanning.cache.enabled", false);
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String sql = "SELECT extractLetters(word) FROM `cp`.`letterInput.json`";
//      String sql = "SELECT a, b + c AS d FROM cp.`example.json`";
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void testAgg() throws Exception {
    LogFixtureBuilder logBuilder = new LogFixtureBuilder()
        .logger("org.apache.drill.exec.compile.JaninoClassCompiler", Level.DEBUG)
        .rootLogger(Level.WARN)
        ;
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.classpath.scanning.cache.enabled", false);
    try (LogFixture logFixture = logBuilder.build();
         ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
//      String sql = "SELECT myAvg(`b`) FROM `cp`.`simpleAgg.json` GROUP BY `a`";
      String sql = "SELECT wtAvg(`b`, `c`) FROM `cp`.`simpleAgg.json` GROUP BY `a`";
//      String sql = "SELECT wtAvg2(`b`, `c`) FROM `cp`.`simpleAgg.json` GROUP BY `a`";
      client.queryBuilder().sql(sql).printCsv();
    }
  }

  @Test
  public void testAggFn() throws Exception {
    // Create an instance of the aggregate function
    WeightedAvgFunc fn = newAfgFn();

    // Test an empty group

    fn.reset();
    assertEquals(0D, callOutput(fn), 0.0001);

    // Test only nulls

    fn.reset();
    callAdd(fn, null, 1.0);
    callAdd(fn, 100.0, null);
    callAdd(fn, null, null);
    assertEquals(0D, callOutput(fn), 0.0001);

    // Actual values

    fn.reset();
    callAdd(fn, null, 1.0);
    callAdd(fn, 100.0, 10.0);
    callAdd(fn, 1000.0, 0.0);
    callAdd(fn, 200.0, 5.0);
    double expected = ((100.0 * 10) + (200 * 5)) / (10 + 5);
    assertEquals(expected, callOutput(fn), 0.0001);
  }

  private WeightedAvgFunc newAfgFn() {
    WeightedAvgFunc fn = new WeightedAvgFunc();
    fn.input = new NullableFloat8Holder();
    fn.weight = new NullableFloat8Holder();
    fn.output = new Float8Holder();
    fn.sum = new Float8Holder();
    fn.totalWeights = new Float8Holder();
    fn.setup();
    return fn;
  }

  private void callAdd(WeightedAvgFunc fn, Double value, Double weight) {
    setHolder(fn.input, value);
    setHolder(fn.weight, weight);
    fn.add();
  }

  private void setHolder(NullableFloat8Holder holder, Double value) {
    if (value == null) {
      holder.isSet = 0;
      holder.value = Double.MAX_VALUE;
    } else {
      holder.isSet = 1;
      holder.value = value;
    }
  }

  private double callOutput(WeightedAvgFunc fn) {
    // Catch unset values.
    fn.output.value = Double.MAX_VALUE;
    fn.output();
    return fn.output.value;
  }

  @Test
  public void testDupItFn() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      OperatorContext context = fixture.operatorContext(null);
      try {
        DupItFunction fn = newDupIt(context);
        assertEquals("", callDupIt(fn, "x", 0));
        assertEquals("xyxyxyxy", callDupIt(fn, "xy", 4));
        String result = callDupIt(fn, "0123456789", 1000);
        assertEquals(10_000, result.length());
      } finally {
        context.close();
      }
    }
  }

  private DupItFunction newDupIt(OperatorContext context) {
    DupItFunction fn = new DupItFunction();
    fn.input = new VarCharHolder();
    // Larger than our largest output
    fn.input.buffer = context.getManagedBuffer(1024);
    fn.count = new IntHolder();
    fn.output = new VarCharHolder();
    // Default size, function should realloc
    fn.outputBuf = context.getManagedBuffer();
    fn.setup();
    return fn;
  }

  private String callDupIt(DupItFunction fn, String value, int count) {
    fn.count.value = count;
    byte byteVal[] = value.getBytes(Charsets.UTF_8);
    fn.input.start = 0;
    fn.input.end = byteVal.length;
    fn.input.buffer.setBytes(0, byteVal);
    fn.eval();
    return StringFunctionHelpers.getStringFromVarCharHolder(fn.output);
  }

}
