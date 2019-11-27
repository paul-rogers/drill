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
import org.apache.drill.common.expression.SchemaPath;

import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpGroupScan extends AbstractGroupScan {
  private static final Logger logger = LoggerFactory.getLogger(HttpGroupScan.class);

  private final List<SchemaPath> columns;
  private final HttpScanSpec httpScanSpec;
  private final HttpStoragePluginConfig httpStoragePluginConfig;
  private boolean filterPushedDown = true;

  public HttpGroupScan (
    HttpStoragePluginConfig config,
    HttpScanSpec scanSpec,
    List<SchemaPath> columns
  ) {
    super("no-user");
    this.httpStoragePluginConfig = config;
    this.httpScanSpec = scanSpec;
    this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS : columns;
  }

  public HttpGroupScan(HttpGroupScan that) {
    super(that);
    httpStoragePluginConfig = that.getStorageConfig();
    httpScanSpec = that.getScanSpec();
    columns = that.getColumns();
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    logger.debug("HttpGroupScan applyAssignments");
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }


  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    logger.debug("HttpGroupScan getSpecificScan");
    return new HttpSubScan(httpStoragePluginConfig, httpScanSpec, columns);
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
  public List<SchemaPath> getColumns() {
    return columns;
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

  public HttpStoragePluginConfig getStorageConfig() {
    return httpStoragePluginConfig;
  }

  public HttpScanSpec getScanSpec() {
    return httpScanSpec;
  }

  @Override
  public String toString() {
    return "[" + this.getClass().getSimpleName() +
      "httpScanSpec=" + httpScanSpec.toString() +
      "columns=" + columns.toString() +
      "httpStoragePluginConfig=" + httpStoragePluginConfig.toString() +
      "]";
  }
}
