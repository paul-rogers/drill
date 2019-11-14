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
package org.apache.drill.exec.store.kafka;

import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPushDownFilterIntoScan extends StoragePluginOptimizerRule {
  static final Logger logger = LoggerFactory.getLogger(KafkaPushDownFilterIntoScan.class);

  public static final StoragePluginOptimizerRule INSTANCE = new KafkaPushDownFilterIntoScan();

  private KafkaPushDownFilterIntoScan() {
    super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
          "KafkaPushFilterIntoScan:Filter_On_Scan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScanPrel scan = call.rel(1);
    final FilterPrel filter = call.rel(0);
    final RexNode condition = filter.getCondition();

    LogicalExpression conditionExp =
        DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);

    KafkaGroupScan groupScan = (KafkaGroupScan) scan.getGroupScan();
    logger.info("Partitions ScanSpec before pushdown: " + groupScan.getPartitionScanSpecList());
    KafkaPartitionScanSpecBuilder builder = new KafkaPartitionScanSpecBuilder(groupScan, conditionExp);
    List<KafkaPartitionScanSpec> newScanSpec = null;
    newScanSpec = builder.parseTree();
    builder.close(); // Close consumer

    // No pushdown
    if (newScanSpec == null) {
      return;
    }

    logger.info("Partitions ScanSpec after pushdown: " + newScanSpec);
    GroupScan newGroupScan = groupScan.cloneWithNewSpec(newScanSpec);
    final ScanPrel newScanPrel =
      new ScanPrel(scan.getCluster(), filter.getTraitSet(), newGroupScan, scan.getRowType(), scan.getTable());
    call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(newScanPrel)));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (!super.matches(call)) {
      return false;
    }
    final ScanPrel scan = call.rel(1);
    return scan.getGroupScan() instanceof KafkaGroupScan;
  }
}
