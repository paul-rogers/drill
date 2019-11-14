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
package org.apache.drill.exec.store.base.filter;

import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.physical.ScanPrel;

/**
 * Call-back (listener) implementation for a push-down filter.
 * Abstracts away the common work; plugins implement this class
 * to do work specific to the plugin.
 * <p>
 * Supports two kinds of filter push down:
 * <dl>
 * <dt>Conjunctive Normal Form (CNF)</dt>
 * <dd>A series of expressions joined by an AND: the scan should
 * produce only rows that satisfy all the conditions.</dd>
 * <dt>Disjunctive Normal Form (DNF)</dt>
 * <dd>A series of alternative values for a single column, essentially
 * a set of expressions joined by OR. The scan spits into multiple
 * scans, each scanning one of the partitions (or regions or
 * queries) identified by the case. This is an implementation of the
 * SQL <tt>IN</tt> clause.</dd>
 * <dl>
 * <p>
 * In both cases, the conditions are in the form of a
 * {@link RelOp} in which one side refers to a column in the scan
 * and the other is a constant expression. The "driver" will ensure
 * the rel op is of the correct form; this class ensures that the
 * column is valid for the scan and the type of the value matches the
 * column type (or can be converted.)
 * <p>
 * The DNF form further ensures that all rel ops refer to the same
 * column, and that only the equality operator appears in the
 * terms.
 */

public interface FilterPushDownListener {

  /**
   * Prefix to display in filter rules
   */
  String prefix();

  /**
   * Broad check to see if the scan is of the correct type for this
   * listener. Generally implemented as: <code><pre>
   * public boolean isTargetScan(ScanPrel scan) {
   *   return scan.getGroupScan() instanceof MyGroupScan;
   * }
   * </pre></code>
   */
  boolean isTargetScan(ScanPrel scan);

  /**
   * Check if the filter rule should be applied to the target group scan.
   * Calcite will run this rule multiple times during planning, but the
   * transform only needs to occur once.
   * Allows the group scan to mark in its own way whether the rule has
   * been applied.
   *
   * @param scan the scan node
   * @return true if filter push-down should be applied
   */

  boolean needsApplication(ScanPrel scan);

  /**
   * Determine if the given relational operator (which is already in the form
   * <tt>&lt;col name> &lt;relop> &lt;const></tt>, qualifies for push down for
   * this scan.
   * <p>
   * If so, return an equivalent RelOp with the value normalized to what
   * the plugin needs. The returned value may be the same as the original
   * one if the value is already normalized.
   *
   * @param scan the scan element. Use <tt>scan.getGroupScan()</tt> to get the
   * group scan
   * @param relOp the description of the relational operator expression
   * @return a normalized RelOp if this relop can be transformed into a filter
   * push-down, <tt>null</tt> if not and thus the relop should remain in
   * the Drill plan
   */

  RelOp accept(ScanPrel scan, RelOp relOp);

  /**
   * Transform a normalized DNF term into a new scan. Normalized form is:
   * <br><code><pre>
   * (a AND b AND (x OR y))</pre></code><br>
   * In which each <code>OR</code> term represents a scan partition. It
   * is up to the code here to determine if the scan partition can be handled,
   * corresponds to a storage partition, or can be done as a separate
   * scan (as for a JDBC or REST plugin, say.)
   * <p>
   * Each term is accompanied by the Calcite expression from which it was
   * derived. The caller is responsible for determining which expressions,
   * if any, to leave in the query by returning a list AND'ed (CNF) terms
   * to leave in the query. Those terms can be the ones passed in, or
   * new terms to handle special needs.
   *
   * @param scan the scan node
   * @param rexNode the Calcite OR node from which the list of relops was
   * derived
   * @param dnfTerms the list of OR conditions. All refer to the same column,
   * and all are equality operators; only the values differ
   * @return a pair of elements: a new scan (that represents the pushed filters),
   * and the original or new expression to appear in the WHERE clause
   * joined by AND with any non-candidate expressions. One or the other must
   * be non-null. Or, return null if the filter condition can't be handled
   * and the query should remain unchanged.
   */

  Pair<GroupScan, List<RexNode>> transform(ScanPrel scan,
      List<Pair<RexNode, RelOp>> andTerms, Pair<RexNode, DisjunctionFilterSpec> orTerm);
}
