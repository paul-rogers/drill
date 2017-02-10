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
 ******************************************************************************/
package org.apache.drill.exec.planner.cost;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;

/**
 * Implements revised rules for the default selectivity when no
 * meta-data is available. While Calcite provides defaults, the defaults
 * are too selective and don't follow the laws of probability.
 * The revised rules are:
 *
 * <table>
 * <tr><th>Operator</th><th>Revised</th><th>Notes</th><th>Calcite Default</th></tr>
 * <tr><td>=</td><td>0.15</td><td>Default in Calcite</td><td>.15</td></tr>
 * <tr><td>&lt;></td><td>0.85</td><td>p(<>) = 1 - p(=)</td><td>.5</td></tr>
 * <tr><td>&lt;</td><td>0.425</td><td>p(<>) / 2</td><td>.5</td></tr>
 * <tr><td>></td><td>0.425</td><td>p(<>) / 2</td><td>.5</td></tr>
 * <tr><td>&lt;=</td><td>0.575</td><td>p(<) + p(=)</td><td>.5</td></tr>
 * <tr><td>>=</td><td>0.575</td><td>p(>) + p(=)</td><td>.5</td></tr>
 * <tr><td>LIKE</td><td>0.25</td><td>Default in Calcite</td><td>.25</td></tr>
 * <tr><td>NOT LIKE</td><td>0.75</td><td>p(NOT LIKE) = 1 - p(LIKE)</td><td>.25</td></tr>
 * <tr><td>IN</td><td>0.15</td><td>IN same as =</td><td>.25</td></tr>
 * <tr><td>IN A, B, ...</td><td>max(1,p(=)*n)</td><td>Same as OR</td><td>.25 ?</td></tr>
 * <tr><td>IS NOT NULL</td><td>0.9</td><td>Default in Calcite</td><td>.9</td></tr>
 * <tr><td>IS NULL</td><td>0.1</td><td>1 - p(IS NULL)</td><td>.25 ?</td></tr>
 * <tr><td>IS TRUE</td><td>0.45</td><td>p(IS NOT NULL) / 2</td><td>.25 ?</td></tr>
 * <tr><td>IS NOT TRUE</td><td>0.55</td><td>1 - p(TRUE)</td><td>.25 ?</td></tr>
 * </table>
 * <p>
 * The OR operator is the sum of its operands, limited to 1.0. A series of
 * IN items is the equivalent of a series of ORs (in fact, that is how
 * Calcite implements an IN list.) The rules for IS FALSE and IS NOT FALSE
 * are the same as IS TRUE and IS NOT TRUE.
 */

public class DrillRelMdSelectivity extends RelMdSelectivity {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.SELECTIVITY.method, new DrillRelMdSelectivity());

  // Catch-all rule when none of the others apply.
  @Override
  public Double getSelectivity(RelNode rel, RexNode predicate) {
    return guessSelectivity(predicate);
  }

  /**
   * Returns default estimates for selectivities, in the absence of stats.
   *
   * @param predicate      predicate for which selectivity will be computed;
   *                       null means true, so gives selectity of 1.0
   * @return estimated selectivity
   */
  public  double guessSelectivity(
      RexNode predicate) {
    double sel = 1.0;
    if ((predicate == null) || predicate.isAlwaysTrue()) {
      return sel;
    }

    for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
      sel *= guessExprSelectivity( pred );
    }

    return sel;
  }

  // Computed probabilities from the above table.

  public static final double PROB_A_EQ_B = 0.15; // From Calcite
  public static final double PROB_A_NE_B = 1 - PROB_A_EQ_B;
  public static final double PROB_A_LT_B = PROB_A_NE_B / 2;
  public static final double PROB_A_GT_B = PROB_A_NE_B / 2;
  public static final double PROB_A_LE_B = PROB_A_LT_B + PROB_A_EQ_B;
  public static final double PROB_A_GE_B = PROB_A_GT_B + PROB_A_EQ_B;
  public static final double PROB_A_LIKE_B = 0.25; // From Calcite
  public static final double PROB_A_NOT_LIKE_B = 1 - PROB_A_LIKE_B;
  public static final double PROB_A_NOT_NULL = 0.9; // From Calcite
  public static final double PROB_A_IS_NULL = 1 - PROB_A_NOT_NULL;
  public static final double PROB_A_IN_B = PROB_A_EQ_B;
  public static final double PROB_A_BETWEEN_B_C = 0.5; // From Calcite
  public static final double PROB_A_IS_TRUE = PROB_A_NOT_NULL / 2;
  public static final double PROB_A_IS_FALSE = PROB_A_IS_TRUE;
  public static final double PROB_A_NOT_TRUE = 1 - PROB_A_IS_TRUE;
  public static final double PROB_A_NOT_FALSE = 1 - PROB_A_IS_FALSE;
  public static final double DEFAULT_PROB = 0.25; // From Calcite

  private double guessExprSelectivity(RexNode pred) {
    switch ( pred.getKind() ) {
    case BETWEEN:
      return PROB_A_BETWEEN_B_C;
    case EQUALS:
      return PROB_A_EQ_B;
    case GREATER_THAN:
      return PROB_A_GT_B;
    case GREATER_THAN_OR_EQUAL:
      return PROB_A_GE_B;
    case IN:
      return PROB_A_IN_B;
    case IS_FALSE:
      return PROB_A_IS_FALSE;
    case IS_NOT_FALSE:
      return PROB_A_NOT_FALSE;
    case IS_NOT_NULL:
      return PROB_A_NOT_NULL;
    case IS_NOT_TRUE:
      return PROB_A_NOT_TRUE;
    case IS_NULL:
      return PROB_A_IS_NULL;
    case IS_TRUE:
      return PROB_A_IS_TRUE;
    case IS_UNKNOWN:
      return DEFAULT_PROB;
    case LESS_THAN:
      return PROB_A_LT_B;
    case LESS_THAN_OR_EQUAL:
      return PROB_A_LE_B;
    case LIKE:
      return PROB_A_LIKE_B;
    case NOT:
      return 1.0 - guessExprSelectivity(((RexCall) pred).getOperands().get(0));
    case NOT_EQUALS:
      return PROB_A_NE_B;
    case OR: {
        RexCall rexCall = (RexCall) pred;
        double sel = 0;
        for (RexNode op : rexCall.getOperands()) {
          sel += guessExprSelectivity(op);
        }
        return Math.min(1.0, sel);
      }
    case AND: {
        RexCall rexCall = (RexCall) pred;
        double sel = 1.0;
        for (RexNode op : rexCall.getOperands()) {
          sel *= guessExprSelectivity(op);
        }
        return Math.min(1.0, sel);
      }
    default:
      return DEFAULT_PROB;
    }
//    if (pred.getKind() == SqlKind.IS_NOT_NULL) {
//      return .9;
//    } else if (
//        (pred instanceof RexCall)
//            && (((RexCall) pred).getOperator()
//            == RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC)) {
//      return RelMdUtil.getSelectivityValue(pred);
//    } else if (pred.isA(SqlKind.EQUALS)) {
//      return .15;
//    } else if (pred.isA(SqlKind.COMPARISON)) {
//      return .5;
//    } else {
//      return .25;
//    }
  }

}
