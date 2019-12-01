package org.apache.drill.exec.store.base.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;

public class FilterPushDownLogicalStrategy extends FilterPushDownStrategy {

  /**
   * Base rule that passes target information to the push-down strategy
   */

  private static abstract class AbstractFilterPushDownRule extends StoragePluginOptimizerRule {

    protected final FilterPushDownLogicalStrategy strategy;

    public AbstractFilterPushDownRule(RelOptRuleOperand operand, String description,
        FilterPushDownLogicalStrategy strategy) {
      super(operand, description);
      this.strategy = strategy;
    }
  }

  /**
   * Calcite rule for FILTER --> PROJECT --> SCAN
   */

  private static class ProjectAndFilterRule extends AbstractFilterPushDownRule {

    private ProjectAndFilterRule(FilterPushDownLogicalStrategy strategy) {
      super(RelOptHelper.some(FilterPrel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
            strategy.namePrefix() + "PushDownFilterLogical:Filter_On_Project",
            strategy);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      if (!super.matches(call)) {
        return false;
      }
      DrillScanRel scan = call.rel(2);
      return strategy.isTargetScan(scan.getGroupScan());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      DrillFilterRel filterRel = call.rel(0);
      DrillProjectRel projectRel = call.rel(1);
      DrillScanRel scanRel = call.rel(2);
      strategy.onMatch(call, filterRel, projectRel, scanRel);
    }
  }

  /**
   * Calcite rule for FILTER --> SCAN
   */

  private static class FilterWithoutProjectRule extends AbstractFilterPushDownRule {

    private FilterWithoutProjectRule(FilterPushDownLogicalStrategy strategy) {
      super(RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
            strategy.namePrefix() + "PushDownFilterLogical:Filter_On_Scan",
            strategy);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      if (!super.matches(call)) {
        return false;
      }
      DrillScanRel scan = call.rel(1);
      return strategy.isTargetScan(scan.getGroupScan());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      DrillFilterRel filterRel = call.rel(0);
      DrillScanRel scanRel = call.rel(1);
      strategy.onMatch(call, filterRel, null, scanRel);
    }
  }

  public FilterPushDownLogicalStrategy(OptimizerRulesContext optimizerContext, FilterPushDownListener listener) {
    super(optimizerContext, listener);
  }

  public Set<StoragePluginOptimizerRule> rules() {
    return ImmutableSet.of(
        new ProjectAndFilterRule(this),
        new FilterWithoutProjectRule(this));
  }

  public static Set<StoragePluginOptimizerRule> rulesFor(
      OptimizerRulesContext optimizerContext, FilterPushDownListener listener) {
    return new FilterPushDownLogicalStrategy(optimizerContext, listener).rules();
  }

  public void onMatch(RelOptRuleCall call, DrillFilterRel filter, DrillProjectRel project, DrillScanRel scan) {

    // Skip if rule has already been applied.

    if (!listener.needsApplication(scan.getGroupScan())) {
      return;
    }

    // Predicates which cannot be converted to a filter predicate

    List<RexNode> nonConvertedPreds = new ArrayList<>();

    List<Pair<RexNode, List<RelOp>>> cnfTerms =
        sortPredicates(nonConvertedPreds, call, filter, project, scan);
    if (cnfTerms == null) {
      return;
    }
    Pair<List<Pair<RexNode, RelOp>>, Pair<RexNode, DisjunctionFilterSpec>> filterTerms =
        convertToFilterSpec(cnfTerms);
    if (filterTerms == null) {
      return;
    }

    Pair<GroupScan, List<RexNode>> translated =
        listener.transform(scan.getGroupScan(), filterTerms.left, filterTerms.right);

    // Listener rejected the DNF terms

    GroupScan newGroupScan = translated.left;
    if (newGroupScan == null) {
      return;
    }

    // Gather unqualified and rewritten predicates

    List<RexNode> remainingPreds = new ArrayList<>();
    remainingPreds.addAll(nonConvertedPreds);
    if (translated.right != null) {
      remainingPreds.addAll(translated.right);
    }

    // Replace the child with the new filter on top of the child/scan

    call.transformTo(
        rebuildTree(scan, newGroupScan, filter, project, remainingPreds));
  }

  private List<Pair<RexNode, List<RelOp>>> sortPredicates(
      List<RexNode> nonConvertedPreds, RelOptRuleCall call, DrillFilterRel filter,
      DrillProjectRel project, DrillScanRel scan) {

    // Get the filter expression

    RexNode condition;
    if (project == null) {
      condition = filter.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushPastProject(filter.getCondition(), project);
    }

    // Skip if no expression or expression is trivial.
    // This seems to never happen because Calcite optimizes away
    // any expression of the form WHERE true, 1 = 1 or 0 = 1.

    if (condition == null || condition.isAlwaysTrue() || condition.isAlwaysFalse()) {
      return null;
    }

    // Get a conjunctions of the filter condition. For each conjunction, if it refers
    // to ITEM or FLATTEN expression then it cannot be pushed down. Otherwise, it's
    // qualified to be pushed down.

    List<RexNode> filterPreds = RelOptUtil.conjunctions(
        RexUtil.toCnf(filter.getCluster().getRexBuilder(), condition));

    DrillParseContext parseContext = new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner()));
    List<Pair<RexNode, List<RelOp>>> cnfTerms = new ArrayList<>();
    for (RexNode pred : filterPreds) {
      List<RelOp> relOps = identifyCandidate(parseContext, scan, pred);
      if (relOps == null) {
        nonConvertedPreds.add(pred);
      } else {
        cnfTerms.add(Pair.of(pred, relOps));
      }
    }
    return cnfTerms.isEmpty() ? null : cnfTerms;
  }

  public List<RelOp> identifyCandidate(DrillParseContext parseContext, DrillScanRel scan, RexNode pred) {
    if (DrillRelOptUtil.findOperators(pred, Collections.emptyList(), BANNED_OPERATORS) != null) {
      return null;
    }

    // Extract an AND term, which may be an OR expression.

    LogicalExpression drillPredicate = DrillOptiq.toDrill(parseContext, scan, pred);
    List<RelOp> relOps = drillPredicate.accept(FilterPushDownPhysicalUtils.REL_OP_EXTRACTOR, null);
    if (relOps == null || relOps.isEmpty()) {
      return null;
    }

    // Check if each term can be pushed down, and, if so, return a new RelOp
    // with the value normalized.

    List<RelOp> normalized = new ArrayList<>();
    for (RelOp relOp : relOps) {
      RelOp rewritten = listener.accept(scan.getGroupScan(), relOp);

      // Must discard the entire OR clause if any part is rejected

      if (rewritten == null) {
        return null;
      }
      normalized.add(rewritten);
    }
    return normalized.isEmpty() ? null : normalized;
  }


  private RelNode rebuildTree(DrillScanRel oldScan, GroupScan newGroupScan, DrillFilterRel filter,
      DrillProjectRel project, List<RexNode> remainingPreds) {

    // Rebuild the subtree with transformed nodes.

    // Scan: new if available, else existing.

    RelNode newNode;
    if (newGroupScan == null) {
      newNode = oldScan;
    } else {
      newNode = new DrillScanRel(oldScan.getCluster(), oldScan.getTraitSet(), oldScan.getTable(),
          newGroupScan, oldScan.getRowType(), oldScan.getColumns());
    }

    // Copy project, if exists

    if (project != null) {
      newNode = project.copy(project.getTraitSet(), Collections.singletonList(newNode));
    }

    // Add filter, if any predicates remain.

    if (!remainingPreds.isEmpty()) {

      // If some of the predicates weren't used in the filter, creates new filter with them
      // on top of current scan. Excludes the case when all predicates weren't used in the filter.
      // FILTER(a, b, c) --> SCAN becomes FILTER(a, d) --> SCAN

      newNode = filter.copy(filter.getTraitSet(), newNode,
          RexUtil.composeConjunction(
              filter.getCluster().getRexBuilder(),
              remainingPreds,
              true));
    }

    return newNode;
  }
}
