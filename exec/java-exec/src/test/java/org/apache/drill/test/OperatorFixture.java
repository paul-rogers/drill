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
package org.apache.drill.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.FragmentExecContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperExecContext;
import org.apache.drill.exec.ops.OperExecContextImpl;
import org.apache.drill.exec.ops.OperatorStatReceiver;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.options.BaseOptionManager;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetSchema;

public class OperatorFixture implements AutoCloseable {

  public static class OperatorFixtureBuilder
  {
    ConfigBuilder configBuilder = new ConfigBuilder();
    TestOptionSet options = new TestOptionSet();

    public ConfigBuilder configBuilder() {
      return configBuilder;
    }

    public TestOptionSet options() {
      return options;
    }

    public OperatorFixture build() {
      return new OperatorFixture(this);
    }
  }

  public static class TestOptionSet extends BaseOptionManager {

    private Map<String,OptionValue> values = new HashMap<>();

    public TestOptionSet() {
      // Crashes in FunctionImplementationRegistry if not set
      set(ExecConstants.CAST_TO_NULLABLE_NUMERIC, false);
      // Crashes in the Dynamic UDF code if not disabled
      set(ExecConstants.USE_DYNAMIC_UDFS_KEY, false);
//      set(ExecConstants.CODE_GEN_EXP_IN_METHOD_SIZE_VALIDATOR, false);
    }

    public void set(String key, int value) {
      set(key, (long) value);
    }

    public void set(String key, long value) {
      values.put(key, OptionValue.createLong(OptionType.SYSTEM, key, value));
    }

    public void set(String key, boolean value) {
      values.put(key, OptionValue.createBoolean(OptionType.SYSTEM, key, value));
    }

    public void set(String key, double value) {
      values.put(key, OptionValue.createDouble(OptionType.SYSTEM, key, value));
    }

    public void set(String key, String value) {
      values.put(key, OptionValue.createString(OptionType.SYSTEM, key, value));
    }

    @Override
    public OptionValue getOption(String name) {
      return values.get(name);
    }
  }

  public static class TestCodeGenContext implements FragmentExecContext {

    private final DrillConfig config;
    private final OptionSet options;
    private final CodeCompiler compiler;
    private final FunctionImplementationRegistry functionRegistry;
    private ExecutionControls controls;

    public TestCodeGenContext(DrillConfig config, OptionSet options) {
      this.config = config;
      this.options = options;
      ScanResult classpathScan = ClassPathScanner.fromPrescan(config);
      functionRegistry = new FunctionImplementationRegistry(config, classpathScan, options);
      compiler = new CodeCompiler(config, options);
    }

    public void setExecutionControls(ExecutionControls controls) {
      this.controls = controls;
    }

    @Override
    public FunctionImplementationRegistry getFunctionRegistry() {
      return functionRegistry;
    }

    @Override
    public OptionSet getOptionSet() {
      return options;
    }

    @Override
    public <T> T getImplementationClass(final ClassGenerator<T> cg)
        throws ClassTransformationException, IOException {
      return getImplementationClass(cg.getCodeGenerator());
    }

    @Override
    public <T> T getImplementationClass(final CodeGenerator<T> cg)
        throws ClassTransformationException, IOException {
      return compiler.createInstance(cg);
    }

    @Override
    public <T> List<T> getImplementationClass(final ClassGenerator<T> cg, final int instanceCount) throws ClassTransformationException, IOException {
      return getImplementationClass(cg.getCodeGenerator(), instanceCount);
    }

    @Override
    public <T> List<T> getImplementationClass(final CodeGenerator<T> cg, final int instanceCount) throws ClassTransformationException, IOException {
      return compiler.createInstances(cg, instanceCount);
    }

    @Override
    public boolean shouldContinue() {
      return true;
    }

    @Override
    public ExecutionControls getExecutionControls() {
      return controls;
    }

    @Override
    public DrillConfig getConfig() {
      return config;
    }
  }

  public static class MockStats implements OperatorStatReceiver {

    public Map<Integer,Double> stats = new HashMap<>();

    @Override
    public void addLongStat(MetricDef metric, long value) {
      setStat(metric, getStat(metric) + value);
    }

    @Override
    public void addDoubleStat(MetricDef metric, double value) {
      setStat(metric, getStat(metric) + value);
    }

    @Override
    public void setLongStat(MetricDef metric, long value) {
      setStat(metric, value);
    }

    @Override
    public void setDoubleStat(MetricDef metric, double value) {
      setStat(metric, value);
    }

    public double getStat(MetricDef metric) {
      return getStat(metric.metricId());
    }

    private double getStat(int metricId) {
      Double value = stats.get(metricId);
      return value == null ? 0 : value;
    }

    private void setStat(MetricDef metric, double value) {
      setStat(metric.metricId(), value);
    }

    private void setStat(int metricId, double value) {
      stats.put(metricId, value);
    }
  }

  private final DrillConfig config;
  private final BufferAllocator allocator;
  private final TestOptionSet options;
  private final TestCodeGenContext context;
  private final OperatorStatReceiver stats;

  protected OperatorFixture(OperatorFixtureBuilder builder) {
    config = builder.configBuilder().build();
    allocator = RootAllocatorFactory.newRoot(config);
    options = builder.options();
    context = new TestCodeGenContext(config, options);
    stats = new MockStats();
   }

  public BufferAllocator allocator() { return allocator; }

  public TestOptionSet options() { return options; }


  public FragmentExecContext codeGenContext() { return context; }

  @Override
  public void close() throws Exception {
    allocator.close();
  }

  public static OperatorFixtureBuilder builder() {
    return new OperatorFixtureBuilder();
  }

  public OperExecContext newOperExecContext(PhysicalOperator opDefn) {
    return new OperExecContextImpl(context, allocator, stats, opDefn, null);
  }

  public RowSetBuilder rowSetBuilder(RowSetSchema schema) {
    return new RowSetBuilder(allocator, schema);
  }

  public RowSet rowSet(RowSetSchema schema) {
    return new RowSet(allocator, schema);
  }

}