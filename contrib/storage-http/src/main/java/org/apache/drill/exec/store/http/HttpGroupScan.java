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
import org.apache.drill.common.expression.SchemaPath;

import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeName("http-scan")
public class HttpGroupScan extends AbstractGroupScan {
  private static final Logger logger = LoggerFactory.getLogger(HttpGroupScan.class);

  private final List<SchemaPath> columns;
  private final HttpScanSpec httpScanSpec;
  private final HttpStoragePluginConfig config;
  private boolean filterPushedDown = false;

  public HttpGroupScan (
    HttpStoragePluginConfig config,
    HttpScanSpec scanSpec,
    List<SchemaPath> columns
  ) {
    super("no-user");
    this.config = config;
    this.httpScanSpec = scanSpec;
    this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS : columns;
  }

  public HttpGroupScan(HttpGroupScan that) {
    super(that);
    config = that.config();
    httpScanSpec = that.httpScanSpec();
    columns = that.getColumns();
  }

  @JsonCreator
  public HttpGroupScan(
    @JsonProperty("config") HttpStoragePluginConfig config,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("httpScanSpec") HttpScanSpec httpScanSpec,
    @JacksonInject StoragePluginRegistry engineRegistry
  ) {
    super("no-user");
    this.config = config;
    this.columns = columns;
    this.httpScanSpec = httpScanSpec;
  }

  @JsonProperty
  public HttpStoragePluginConfig config() { return config; }

  @JsonProperty
  public List<SchemaPath> columns() { return columns; }

  @JsonProperty
  public HttpScanSpec httpScanSpec() { return httpScanSpec; }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    logger.debug("HttpGroupScan applyAssignments");
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 0;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    logger.debug("HttpGroupScan getSpecificScan");
    return new HttpSubScan(config, httpScanSpec, columns);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    logger.debug("HttpGroupScan clone {}", columns);
    return new HttpGroupScan(this);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HttpGroupScan(this);
  }

  @Override
  public ScanStats getScanStats() {


    int estRowCount = 1;
    int estDataSize = estRowCount * 200;
    int estCpuCost = 1;
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT,estRowCount, estCpuCost, estDataSize);
  }

  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " [" +
      "httpScanSpec=" + httpScanSpec.toString() +
      "columns=" + columns.toString() +
      "httpStoragePluginConfig=" + config.toString() +
      "]";
  }
}
