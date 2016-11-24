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
package org.apache.drill.exec.compile;

import java.io.IOException;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.compile.ClassTransformer.ClassSet;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.codehaus.commons.compiler.CompileException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClassTransformation extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestClassTransformation.class);

  private static final int ITERATION_COUNT = Integer.valueOf(System.getProperty("TestClassTransformation.iteration", "1"));

  private static SessionOptionManager sessionOptions;

  @BeforeClass
  public static void beforeTestClassTransformation() throws Exception {
    final UserSession userSession = UserSession.Builder.newBuilder()
      .withOptionManager(getDrillbitContext().getOptionManager())
      .build();
    sessionOptions = (SessionOptionManager) userSession.getOptions();
  }

  @Test
  public void testJaninoClassCompiler() throws Exception {
    logger.debug("Testing JaninoClassCompiler");
    sessionOptions.setOption(OptionValue.createString(OptionType.SESSION, QueryClassLoader.JAVA_COMPILER_OPTION, QueryClassLoader.CompilerPolicy.JANINO.name()));
    try (QueryClassLoader loader = new QueryClassLoader(config, sessionOptions)) {
      for (int i = 0; i < ITERATION_COUNT; i++) {
        compilationInnerClass(loader);
      }
    }
  }

  @Test
  public void testJDKClassCompiler() throws Exception {
    logger.debug("Testing JDKClassCompiler");
    sessionOptions.setOption(OptionValue.createString(OptionType.SESSION, QueryClassLoader.JAVA_COMPILER_OPTION, QueryClassLoader.CompilerPolicy.JDK.name()));
    try (QueryClassLoader loader = new QueryClassLoader(config, sessionOptions)) {
      for (int i = 0; i < ITERATION_COUNT; i++) {
        compilationInnerClass(loader);
      }
    }
  }

  @Test
  public void testCompilationNoDebug() throws CompileException, ClassNotFoundException, ClassTransformationException, IOException {
    CodeGenerator<ExampleInner> cg = newCodeGenerator(ExampleInner.class, ExampleTemplateWithInner.class);
    ClassSet classSet = new ClassSet(null, cg.getDefinition().getTemplateClassName(), cg.getMaterializedClassName());
    String sourceCode = cg.generateAndGet();
    sessionOptions.setOption(OptionValue.createString(OptionType.SESSION, QueryClassLoader.JAVA_COMPILER_OPTION, QueryClassLoader.CompilerPolicy.JDK.name()));

    sessionOptions.setOption(OptionValue.createBoolean(OptionType.SESSION, QueryClassLoader.JAVA_COMPILER_DEBUG_OPTION, false));
    int sizeWithoutDebug = 0;
    try (QueryClassLoader loader = new QueryClassLoader(config, sessionOptions)) {
      final byte[][] codeWithoutDebug = loader.getClassByteCode(classSet.generated, sourceCode);
      loader.close();
      for (byte[] bs : codeWithoutDebug) {
        sizeWithoutDebug += bs.length;
      }

      sessionOptions.setOption(OptionValue.createBoolean(OptionType.SESSION, QueryClassLoader.JAVA_COMPILER_DEBUG_OPTION, true));
    }
    try (QueryClassLoader loader = new QueryClassLoader(config, sessionOptions)) {
      final byte[][] codeWithDebug = loader.getClassByteCode(classSet.generated, sourceCode);
       int sizeWithDebug = 0;
      for (byte[] bs : codeWithDebug) {
        sizeWithDebug += bs.length;
      }

      Assert.assertTrue("Debug code is smaller than optimized code!!!", sizeWithDebug > sizeWithoutDebug);
      logger.debug("Optimized code is {}% smaller than debug code.", (int)((sizeWithDebug - sizeWithoutDebug)/(double)sizeWithDebug*100));
    }
  }

  /**
   * Do a test of a three level class to ensure that nested code generators works correctly.
   * @throws Exception
   */
  private void compilationInnerClass(QueryClassLoader loader) throws Exception{
    CodeGenerator<ExampleInner> cg = newCodeGenerator(ExampleInner.class, ExampleTemplateWithInner.class);

    ClassTransformer ct = new ClassTransformer(sessionOptions);
    @SuppressWarnings({ "unchecked" })
    Class<? extends ExampleInner> c = (Class<? extends ExampleInner>) ct.getImplementationClass(loader, cg.getDefinition(), cg.generateAndGet(), cg.getMaterializedClassName());
    ExampleInner t = (ExampleInner) c.newInstance();
    outside = 0;
    inside = 0;
    inside2 = 0;
    t.doOutside();
    Assert.assertEquals( outside, 1 );
    t.doInsideOutside();
    Assert.assertEquals( inside, 1 );
    Assert.assertEquals( inside2, 1 );
  }

  public static int outside = 0;
  public static int inside = 0;
  public static int inside2 = 0;
  private <T, X extends T> CodeGenerator<T> newCodeGenerator(Class<T> iface, Class<X> impl) {
    final TemplateClassDefinition<T> template = new TemplateClassDefinition<T>(iface, impl);
    CodeGenerator<T> cg = CodeGenerator.get(template, getDrillbitContext().getFunctionImplementationRegistry(), getDrillbitContext().getOptionManager());

    ClassGenerator<T> root = cg.getRoot();
    root.setMappingSet(new MappingSet(new GeneratorMapping("doOutside", null, null, null)));
    String thisClassName = getClass( ).getName();
//    root.getSetupBlock().directStatement("System.out.println(\"outside\");");
    root.getSetupBlock().directStatement(thisClassName + ".outside = 1;");


    ClassGenerator<T> inner = root.getInnerGenerator("TheInnerClass");
    inner.setMappingSet(new MappingSet(new GeneratorMapping("doInside", null, null, null)));
    inner.getSetupBlock().directStatement(thisClassName + ".inside = 1;");
//    inner.getSetupBlock().directStatement("System.out.println(\"inside\");");

    ClassGenerator<T> doubleInner = inner.getInnerGenerator("DoubleInner");
    doubleInner.setMappingSet(new MappingSet(new GeneratorMapping("doDouble", null, null, null)));
    doubleInner.getSetupBlock().directStatement(thisClassName + ".inside2 = 1;");
    return cg;
  }
}
