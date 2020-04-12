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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.planner.PlannerPhase;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.dfs.FormatPlugin;

/**
 * Abstract class for StorePlugin implementations.
 * See StoragePlugin for description of the interface intent and its methods.
 */
public abstract class AbstractStoragePlugin implements StoragePlugin {

  protected final StoragePluginContext pluginContext;
  protected final DrillbitContext context;
  private final String name;

  /**
   * @deprecated Change plugin constructor to use StoragePluginContext
   */
  @Deprecated
  protected AbstractStoragePlugin(DrillbitContext context, String inName) {
    this.context = context;
    this.pluginContext = null;
    this.name = inName == null ? null : inName.toLowerCase();
  }

  protected AbstractStoragePlugin(StoragePluginContext pluginContext, String inName) {
    this.context = null;
    this.pluginContext = pluginContext;
    this.name = inName == null ? null : inName.toLowerCase();
  }

  @Override
  public String getName() { return name; }

  @Override
  public boolean supportsRead() { return false; }

  @Override
  public boolean supportsWrite() { return false; }

  /**
   * @deprecated use {@link #pluginContext()} instead. {@code DrillbitContext}
   * is too complex for plugins, will not be available in future versions.
   */
  @Deprecated
  public DrillbitContext getContext() { return context; }

  @Override
  public StoragePluginContext pluginContext() { return pluginContext; }

  /**
   * @deprecated Marking for deprecation in next major version release. Use
   *             {@link #getOptimizerRules(org.apache.drill.exec.ops.OptimizerRulesContext, org.apache.drill.exec.planner.PlannerPhase)}
   */
  @Override
  @Deprecated
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext) {
    return ImmutableSet.of();
  }

  /**
   * @deprecated Marking for deprecation in next major version release. Use
   *             {@link #getOptimizerRules(org.apache.drill.exec.ops.OptimizerRulesContext, org.apache.drill.exec.planner.PlannerPhase)}
   */
  @Deprecated
  public Set<? extends RelOptRule> getLogicalOptimizerRules(OptimizerRulesContext optimizerContext) {
    return ImmutableSet.of();
  }

  /**
   * @deprecated Marking for deprecation in next major version release. Use
   *             {@link #getOptimizerRules(org.apache.drill.exec.ops.OptimizerRulesContext, org.apache.drill.exec.planner.PlannerPhase)}
   */
  @Deprecated
  public Set<? extends RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    // To be backward compatible, by default call the getOptimizerRules() method.
    return getOptimizerRules(optimizerRulesContext);
  }

  /**
   *
   * Note: Move this method to {@link StoragePlugin} interface in next major version release.
   */
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {
    switch (phase) {
    case LOGICAL_PRUNE_AND_JOIN:
    case LOGICAL_PRUNE:
    case PARTITION_PRUNING:
      return getLogicalOptimizerRules(optimizerContext);
    case PHYSICAL:
      return getPhysicalOptimizerRules(optimizerContext);
    case LOGICAL:
    case JOIN_PLANNING:
    default:
      return ImmutableSet.of();
    }
  }

  @Deprecated
  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    throw new UnsupportedOperationException("Physical scan is not supported by '" + getName() + "' storage plugin.");
  }

  @Deprecated
  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
    return getPhysicalScan(userName, selection);
  }

  @Deprecated
  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
      OptionSet options) throws IOException {
    return getPhysicalScan(userName, selection);
  }

  @Deprecated
  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
      OptionSet options, MetadataProviderManager metadataProviderManager) throws IOException {
    return getPhysicalScan(userName, selection, options);
  }

  @Deprecated
  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
      List<SchemaPath> columns, OptionSet options) throws IOException {
    return getPhysicalScan(userName, selection, columns);
  }

  @Deprecated
  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
      List<SchemaPath> columns, OptionSet options,
      MetadataProviderManager metadataProviderManager) throws IOException {
    return getPhysicalScan(userName, selection, columns, options);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(ScanRequest scanRequest) throws IOException {
    return getPhysicalScan(scanRequest.userName(), scanRequest.jsonOptions(),
        scanRequest.columns(), scanRequest.options(), scanRequest.metadataProviderManager());
  }

  @Override
  public FormatPlugin getFormatPlugin(FormatPluginConfig config) {
    throw new UnsupportedOperationException(String.format("%s doesn't support format plugins", getClass().getName()));
  }

  @Override
  public void close() throws Exception { }
}
