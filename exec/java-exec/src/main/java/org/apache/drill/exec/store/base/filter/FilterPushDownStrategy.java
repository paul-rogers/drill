package org.apache.drill.exec.store.base.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public abstract class FilterPushDownStrategy {

  protected static final Collection<String> BANNED_OPERATORS =
      Lists.newArrayList("flatten");

  protected final OptimizerRulesContext optimizerContext;
  protected final FilterPushDownListener listener;

  public FilterPushDownStrategy(OptimizerRulesContext optimizerContext, FilterPushDownListener listener) {
    this.optimizerContext = optimizerContext;
    this.listener = listener;
  }

  protected String namePrefix() { return listener.prefix(); }

  protected boolean isTargetScan(GroupScan scan) {
    return listener.isTargetScan(scan);
  }

  /**
   * Parse the allowed filter forms:<br><code><pre>
   * x AND y
   * a OR b
   * x AND y AND (a OR b)</pre></code>
   * <p>
   * The last form is equivalent to a partitioned scan:<br><code><pre>
   * (x AND y AND a) OR
   * (x AND y AND b)</pre></code>
   * <p>
   * Input is in the first form, output is in the second.
   */

  protected Pair<List<Pair<RexNode, RelOp>>, Pair<RexNode, DisjunctionFilterSpec>>
      convertToFilterSpec(List<Pair<RexNode, List<RelOp>>> cnfTerms) {

    // If no qualified predicates, nothing to do.

    if (cnfTerms.isEmpty()) {
      return null;
    }

    // Any OR clauses?

    Pair<RexNode, List<RelOp>> orTerm = null;
    List<Pair<RexNode, RelOp>> andTerms = new ArrayList<>();
    for (Pair<RexNode, List<RelOp>> cnfTerm : cnfTerms) {
      List<RelOp> relOps = cnfTerm.right;
      if (relOps.size() == 1) {
        andTerms.add(Pair.of(cnfTerm.left, relOps.get(0)));
      } else if (orTerm == null) {
        orTerm = cnfTerm;
      } else {

        // Can't handle (w OR x) AND (y OR z)
        return null;
      }
    }

    if (orTerm == null) {

      // No OR, all are simple CNF terms
      // ((x), (y)) --> ((x, y))

      return Pair.of(andTerms, null);
    }

    // If an OR clause, all must be on the same column, all
    // must be equality and all must be of the same type.

    List<RelOp> orRelops = orTerm.right;
    assert orRelops.size() > 1; // Sanity check

    // All OR terms must be equality

    for (RelOp relOp : orRelops) {
      if (relOp.op != RelOp.Op.EQ) {
        return null;
      }
    }

    // All must be for the same column and of the same type.

    RelOp first = orRelops.get(0);
    String colName = first.colName;
    MinorType valueType = first.value.type;
    for (int i = 1; i < orRelops.size(); i++) {
      RelOp orRelop = orRelops.get(i);
      if (!colName.equals(orRelop.colName)) {
        return null;
      }

      // The following should not happen if the listener is well-behaved:
      // converted all values for a column to a single type. But if the listener
      // is lazy, and did not do so, we reject the OR expression here.

      if (orRelop.value.type != valueType) {
        return null;
      }
    }

    // Convert to a disjunction filter

    Pair<RexNode, DisjunctionFilterSpec> disjunction =
        Pair.of(orTerm.left, new DisjunctionFilterSpec(orRelops));
    return Pair.of(andTerms, disjunction);
  }

}
