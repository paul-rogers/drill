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

import java.util.List;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeName("http-scan")
public class HttpGroupScan extends AbstractGroupScan {
  static final Logger logger = LoggerFactory.getLogger(HttpGroupScan.class);


  private HttpStoragePlugin httpStoragePlugin;
  private HttpScanSpec httpScanSpec;
  private HttpStoragePluginConfig httpStoragePluginConfig;
  private boolean filterPushedDown = true;
  private List<SchemaPath> columns;

  @JsonCreator
  public HttpGroupScan(@JsonProperty("httpScanSpec") HttpScanSpec httpScanSpec,
                       @JsonProperty("httpStoragePluginConfig") HttpStoragePluginConfig httpStoragePluginConfig,
                       @JsonProperty("columns") List<SchemaPath> columns,
                       @JacksonInject StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    this((HttpStoragePlugin) pluginRegistry.getPlugin(httpStoragePluginConfig), httpScanSpec, columns);
  }

  public HttpGroupScan(HttpStoragePlugin httpStoragePlugin,
                       HttpScanSpec httpScanSpec,
                       List<SchemaPath> columns) {
    super((String) null);
    this.httpStoragePlugin = httpStoragePlugin;
    this.httpStoragePluginConfig = httpStoragePlugin.getConfig();
    this.httpScanSpec = httpScanSpec;
    this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;
  }

  public HttpGroupScan(HttpGroupScan that) {
    super(that);
    httpScanSpec = that.httpScanSpec;
    httpStoragePluginConfig = that.httpStoragePluginConfig;
    columns = that.columns;
  }

  public HttpGroupScan(String userName, HttpStoragePluginConfig config, HttpScanSpec spec) {
    super(userName);
    httpScanSpec = spec;
    httpStoragePluginConfig = config;
    this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;
  }

  public HttpScanSpec getScanSpec() {
    return httpScanSpec;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) { // pass to HttpScanBatchCreator
    logger.debug("HttpGroupScan getSpecificScan");
    return new HttpSubScan(httpStoragePluginConfig, httpScanSpec, columns);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    // selection columns from here
    logger.debug("HttpGroupScan clone {}", columns);
    return this;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints)
    throws PhysicalOperatorSetupException {
    logger.debug("HttpGroupScan applyAssignments");
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HttpGroupScan(this);
  }

  @Override
  public ScanStats getScanStats() {
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT,1, 1, 1);
  }

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  public HttpStoragePluginConfig getStorageConfig() {
    return httpStoragePluginConfig;
  }
}
