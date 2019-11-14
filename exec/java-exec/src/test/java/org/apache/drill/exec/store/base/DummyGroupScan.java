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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.filter.DisjunctionFilterSpec;
import org.apache.drill.exec.store.base.filter.RelOp;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("dummy-scan")
@JsonPropertyOrder({"userName", "scanSpec", "columns",
                    "andFilters", "orFilters", "cost", "config"})
public class DummyGroupScan extends BaseGroupScan {

  private final DummyScanSpec scanSpec;
  private final List<RelOp> andFilters;
  private final DisjunctionFilterSpec orFilters;

  public DummyGroupScan(DummyStoragePlugin storagePlugin, String userName,
      DummyScanSpec scanSpec) {
    super(storagePlugin, userName, null);
    this.scanSpec = scanSpec;
    andFilters = null;
    orFilters = null;
  }

  public DummyGroupScan(DummyGroupScan from, List<SchemaPath> columns) {
    super(from.storagePlugin, from.getUserName(), columns);
    this.scanSpec = from.scanSpec;
    this.andFilters = from.andFilters;
    this.orFilters = from.orFilters;
  }

  @JsonCreator
  public DummyGroupScan(
      @JsonProperty("config") DummyStoragePluginConfig config,
      @JsonProperty("userName") String userName,
      @JsonProperty("scanSpec") DummyScanSpec scanSpec,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("andFilters") List<RelOp> andFilters,
      @JsonProperty("orFilters") DisjunctionFilterSpec orFilters,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(config, userName, columns, engineRegistry);
    this.scanSpec = scanSpec;
    this.andFilters = andFilters;
    this.orFilters = orFilters;
  }

  public DummyGroupScan(DummyGroupScan from,
      List<RelOp> andFilters,
      DisjunctionFilterSpec orFilters) {
    super(from);
    this.scanSpec = from.scanSpec;
    this.andFilters = andFilters;
    this.orFilters = orFilters;
  }

  @JsonProperty("scanSpec")
  public DummyScanSpec scanSpec() { return scanSpec; }

  @JsonProperty("andFilters")
  public List<RelOp> andFilters() { return andFilters; }

  @JsonProperty("orFilters")
  public DisjunctionFilterSpec orFilters() { return orFilters; }

  public boolean hasFilters() {
    return andFilters != null || orFilters != null;
  }

  private static final List<String> FILTER_COLS = ImmutableList.of("a", "b", "id");

  public RelOp acceptFilter(RelOp relOp) {

    // Pretend that "id" is a special integer column. Can handle
    // equality only.

    if (relOp.colName.contentEquals("id")) {

      // To allow easier testing, require exact type match: no
      // attempt at type conversion here.

      if (relOp.op != RelOp.Op.EQ || relOp.value.type != MinorType.INT) {
        return null;
      }
      return relOp;
    }

    // All other columns apply only if projected

    if (!FILTER_COLS.contains(relOp.colName)) {
      return null;
    }

    // Only supports a few operators so we can verify that the
    // others are left in the WHERE clause.
    // Neither are really implemented. Less-than lets us check
    // inverting operations for the const op col case.

    switch (relOp.op) {
    case EQ:
    case LT:
    case LE:

      // Convert to target type (pretend all columns are VARCHAR)

      return relOp.normalize(relOp.value.toVarChar());
    case IS_NULL:
    case IS_NOT_NULL:
      return relOp;
    default:
      return null;
    }
  }

//  private boolean hasColumn(String colName) {
//    for (SchemaPath col : scanSpec.columns()) {
//      if (col.isLeaf() && col.getRootSegmentPath().contentEquals(colName)) {
//        return true;
//      }
//    }
//    return false;
//  }

  @Override
  public ScanStats computeScanStats() {

    // No good estimates at all, just make up something.

    int estRowCount = 10_000;

    // If filter push down, assume this reduces data size.
    // Just need to get Calcite to choose this version rather
    // than the un-filtered version.

    if (hasFilters()) {
      estRowCount /= 2;
    }

    // Make up an average row width.

    int estDataSize = estRowCount * 200;

    // If columns provided, then assume this saves data transfer

    if (getColumns() != BaseGroupScan.ALL_COLUMNS) {
      estDataSize = estDataSize * 3 / 4;
    }
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, 1, estDataSize);
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return new DummySubScan(this);
  }

  @Override
  public void buildPlanString(PlanStringBuilder builder) {
    super.buildPlanString(builder);
    builder.field("scanSpec", scanSpec);
    builder.field("andFilters", andFilters);
    builder.field("orFilters", orFilters);
  }
}
