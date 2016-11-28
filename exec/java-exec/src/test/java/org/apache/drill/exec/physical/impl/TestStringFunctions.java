/**
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
package org.apache.drill.exec.physical.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import mockit.Injectable;

public class TestStringFunctions extends ExecTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestStringFunctions.class);

  private final DrillConfig c = DrillConfig.create();
  private PhysicalPlanReader reader;
  private FunctionImplementationRegistry registry;
  private FragmentContext context;
  private @Injectable DrillbitContext bitContext;
  
  @Before
  public void init( ) throws Exception {
    mockDrillbitContext(bitContext);
  }

  public Object[] getRunResult(SimpleRootExec exec) {
    int size = 0;
    for (final ValueVector v : exec) {
      size++;
    }

    final Object[] res = new Object [size];
    int i = 0;
    for (final ValueVector v : exec) {
      if  (v instanceof VarCharVector) {
        res[i++] = new String( ((VarCharVector) v).getAccessor().get(0), Charsets.UTF_8);
      } else {
        res[i++] =  v.getAccessor().getObject(0);
      }
    }
    return res;
  }

  public void runTest(@Injectable UserServer.UserClientConnection connection, Object[] expectedResults, String planPath) throws Throwable {

    final String planString = Resources.toString(Resources.getResource(planPath), Charsets.UTF_8);
    if (reader == null) {
      reader = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(c);
    }
    if (registry == null) {
      registry = new FunctionImplementationRegistry(c);
    }
    if (context == null) {
      context =  new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry); //new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, registry);
    }
    final PhysicalPlan plan = reader.readPhysicalPlan(planString);
    final SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    exec.next(); // skip schema batch
    while(exec.next()) {
      final Object [] res = getRunResult(exec);
      assertEquals("return count does not match", expectedResults.length, res.length);

      for (int i = 0; i<res.length; i++) {
        assertEquals(String.format("column %s does not match", i), expectedResults[i],  res[i]);
      }
    }

    if (context.getFailureCause() != null) {
      throw context.getFailureCause();
    }
    assertTrue(!context.isFailed());
  }

  @Test
  public void testCharLength(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    Object [] expected = new Object[] {new Long(8), new Long(0), new Long(5), new Long(5),
                                       new Long(8), new Long(0), new Long(5), new Long(5),
                                       new Long(8), new Long(0), new Long(5), new Long(5),};
    runTest(connection, expected, "functions/string/testCharLength.json");
  }

  @Test
  public void testLike(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE};
    runTest(connection, expected, "functions/string/testLike.json");
  }

  @Test
  public void testSimilar(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {Boolean.TRUE, Boolean.FALSE, Boolean.TRUE, Boolean.FALSE};
    runTest(connection, expected, "functions/string/testSimilar.json");
  }

  @Test
  public void testLtrim(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"def", "abcdef", "dabc", "", "", ""};
    runTest(connection, expected, "functions/string/testLtrim.json");
  }

  @Test
  public void testTrim(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"fghI", "", "", "!", " aaa "};
    runTest(connection, expected, "functions/string/testTrim.json");
  }

  @Test
  public void testReplace(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"aABABcdf", "ABABbABbcdf", "aababcdf", "acdf", "ABCD", "abc"};
    runTest(connection, expected, "functions/string/testReplace.json");
  }

  @Test
  public void testRtrim(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"abc", "abcdef", "ABd", "", "", ""};
    runTest(connection, expected, "functions/string/testRtrim.json");
  }

  @Test
  public void testConcat(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"abcABC", "abc", "ABC", ""};
    runTest(connection, expected, "functions/string/testConcat.json");
  }

  @Test
  public void testLower(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"abcefgh", "abc", ""};
    runTest(connection, expected, "functions/string/testLower.json");
  }

  @Test
  public void testPosition(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {new Long(2), new Long(0), new Long(0), new Long(0),
                                       new Long(2), new Long(0), new Long(0), new Long(0)};
    runTest(connection, expected, "functions/string/testPosition.json");
  }

  @Test
  public void testRight(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"ef", "abcdef", "abcdef", "cdef", "f", "", ""};
    runTest(connection, expected, "functions/string/testRight.json");
  }

  @Test
  public void testSubstr(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"abc", "bcd", "bcdef", "bcdef", "", "", "", "", "भारत", "वर्ष", "वर्ष", "cdef", "", "", "", "ड्रिल"};
    runTest(connection, expected, "functions/string/testSubstr.json");
  }

  @Test
  public void testLeft(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"ab", "abcdef", "abcdef", "abcd", "a", "", ""};
    runTest(connection, expected, "functions/string/testLeft.json");
  }

  @Test
  public void testLpad(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"", "", "abcdef", "ab", "ab", "abcdef", "AAAAabcdef", "ABABabcdef", "ABCAabcdef", "ABCDabcdef"};
    runTest(connection, expected, "functions/string/testLpad.json");
  }

  @Test
  public void testRegexpReplace(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"ThM", "Th", "Thomas"};
    runTest(connection, expected, "functions/string/testRegexpReplace.json");
  }

  @Test
  public void testRpad(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"", "", "abcdef", "ab", "ab", "abcdef", "abcdefAAAA", "abcdefABAB", "abcdefABCA", "abcdefABCD"};
    runTest(connection, expected, "functions/string/testRpad.json");
  }

  @Test
  public void testUpper(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {"ABCEFGH", "ABC", ""};
    runTest(connection, expected, "functions/string/testUpper.json");
  }

  @Test
  public void testNewStringFuncs(@Injectable UserServer.UserClientConnection connection) throws Throwable {
    final Object [] expected = new Object[] {97, 65, -32, "A", "btrim", "Peace Peace Peace ", "हकुना मताता हकुना मताता ", "katcit", "\u00C3\u00A2pple", "नदम"};
    runTest(connection, expected, "functions/string/testStringFuncs.json");
  }
}
