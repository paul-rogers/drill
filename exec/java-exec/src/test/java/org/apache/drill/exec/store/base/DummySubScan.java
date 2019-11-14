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
package org.apache.drill.exec.store.base;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.filter.DisjunctionFilterSpec;
import org.apache.drill.exec.store.base.filter.RelOp;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Dummy sub scan that simply passes along the group scan
 * information. A real scan operator definition would likely translate the
 * group scan information into the form needed by the
 * underlying storage system.
 */

@JsonTypeName("dummy-sub-scan")
@JsonPropertyOrder({"userName", "scanSpec", "columns",
                    "andFilters", "orFilters", "config"})
@JsonInclude(Include.NON_NULL)
public class DummySubScan extends BaseSubScan {

  private final DummyScanSpec scanSpec;
  private final List<RelOp> andFilters;
  private final DisjunctionFilterSpec orFilters;

  @JsonCreator
  public DummySubScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("config") StoragePluginConfig config,
      @JsonProperty("scanSpec") DummyScanSpec scanSpec,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("andFilters") List<RelOp> andFilters,
      @JsonProperty("orFilters") DisjunctionFilterSpec orFilters,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(userName, config, columns, engineRegistry);
    this.scanSpec = scanSpec;
    this.andFilters = andFilters;
    this.orFilters = orFilters;
 }

  public DummySubScan(DummyGroupScan groupScan) {
    super(groupScan);
    this.scanSpec = groupScan.scanSpec();
    this.andFilters = groupScan.andFilters();
    this.orFilters = groupScan.orFilters();
  }

  @JsonProperty("scanSpec")
  public DummyScanSpec scanSpec() { return scanSpec; }

  @JsonProperty("andFilters")
  public List<RelOp> andFilters() { return andFilters; }

  @JsonProperty("orFilters")
  public DisjunctionFilterSpec orFilters() { return orFilters; }

  @Override
  public void buildPlanString(PlanStringBuilder builder) {
    super.buildPlanString(builder);
    builder.field("scanSpec", scanSpec);
    builder.field("andFilters", andFilters);
    builder.field("orFilters", orFilters);
  }
}
