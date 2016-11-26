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
package org.apache.drill.exec;

import java.io.File;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.io.FileUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.exec.compile.CodeCompilerTestFactory;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.sys.store.provider.LocalPersistentStoreProvider;
import org.apache.drill.exec.util.GuavaPatcher;
import org.apache.drill.test.DrillTest;
import org.junit.After;
import org.junit.BeforeClass;

import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Files;

import mockit.Expectations;


public class ExecTest extends DrillTest {

  protected static SystemOptionManager optionManager;
  static {
    GuavaPatcher.patch();
  }

  private static final DrillConfig c = DrillConfig.create();

  @After
  public void clear(){
    // TODO:  (Re DRILL-1735) Check whether still needed now that
    // BootstrapContext.close() resets the metrics.
    DrillMetrics.resetMetrics();
  }

  @BeforeClass
  public static void setupOptionManager() throws Exception{
    final LocalPersistentStoreProvider provider = new LocalPersistentStoreProvider(c);
    provider.start();
    optionManager = new SystemOptionManager(PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(c), provider);
    optionManager.init();
  }

  /**
   * Create a temp directory to store the given <i>dirName</i>.
   * Directory will be deleted on exit.
   * @param dirName directory name
   * @return Full path including temp parent directory and given directory name.
   */
  public static String getTempDir(final String dirName) {
    final File dir = Files.createTempDir();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        FileUtils.deleteQuietly(dir);
      }
    });
    return dir.getAbsolutePath() + File.separator + dirName;
  }

  protected void mockDrillbitContext(final DrillbitContext bitContext) throws Exception {

    // This hack is necessary to work around the fact that JMockit insists
    // that minTimes=0 be used only in a @Before method, but this test was
    // already set up to do the work here.

//    TestRun.setRunningIndividualTest( TestRun.getCurrentTestInstance(), true );
    new Expectations() {{
      bitContext.getMetrics(); result = new MetricRegistry(); minTimes = 0;
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c)); minTimes = 0;
      bitContext.getConfig(); result = c; minTimes = 0;
      bitContext.getOptionManager(); result = optionManager; minTimes = 0;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c); minTimes = 0;
    }};
//    TestRun.setRunningIndividualTest( TestRun.getCurrentTestInstance(), false );
  }

  protected LogicalExpression parseExpr(String expr) throws RecognitionException {
    final ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final ExprParser parser = new ExprParser(tokens);
    final ExprParser.parse_return ret = parser.parse();
    return ret.e;
  }

}
