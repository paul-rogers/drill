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
package org.apache.drill.exec.work;

import java.util.List;

import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;

public class QueryWorkUnit {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWorkUnit.class);

  /**
   * Definition of a minor fragment that contains the (unserialized) fragment operator
   * tree and the (partially build) fragment. Allows the resource manager to apply
   * memory allocations before serializing the fragments to JSON.
   */

  public static class MinorFragmentDefn {
    private final FragmentRoot root;
    private PlanFragment fragment;
    private final OptionList options;

    public MinorFragmentDefn(final FragmentRoot root, final PlanFragment fragment, OptionList options) {
      this.root = root;
      this.fragment = fragment;
      this.options = options;
    }

    public FragmentRoot root() { return root; }
    public PlanFragment fragment() { return fragment; }
    public OptionList options() { return options; }

    public void applyPlan(PhysicalPlanReader reader) throws ForemanSetupException {
      // get plan as JSON
      String plan;
      String optionsData;
      try {
        plan = reader.writeJson(root);
        optionsData = reader.writeJson(options);
      } catch (JsonProcessingException e) {
        throw new ForemanSetupException("Failure while trying to convert fragment into json.", e);
      }

      fragment = PlanFragment.newBuilder(fragment)
          .setFragmentJson(plan)
          .setOptionsJson(optionsData)
          .build();
    }
  }

  private final PlanFragment rootFragment; // for local
  private final FragmentRoot rootOperator; // for local
  private final List<PlanFragment> fragments;
  private final List<MinorFragmentDefn> minorFragmentDefns;

  public QueryWorkUnit(final FragmentRoot rootOperator, final PlanFragment rootFragment,
      final List<PlanFragment> fragments,
      final List<MinorFragmentDefn> minorFragmentDefns) {
    Preconditions.checkNotNull(rootFragment);
    Preconditions.checkNotNull(fragments);
    Preconditions.checkNotNull(rootOperator);
    Preconditions.checkNotNull(minorFragmentDefns);

    this.rootFragment = rootFragment;
    this.fragments = fragments;
    this.rootOperator = rootOperator;
    this.minorFragmentDefns = minorFragmentDefns;
  }

  public PlanFragment getRootFragment() {
    return rootFragment;
  }

  public List<PlanFragment> getFragments() {
    return fragments;
  }

  public List<MinorFragmentDefn> getMinorFragmentDefns() {
    return minorFragmentDefns;
  }

  public FragmentRoot getRootOperator() {
    return rootOperator;
  }

  public void applyPlan(PhysicalPlanReader reader) throws ForemanSetupException {
    for (MinorFragmentDefn defn : minorFragmentDefns) {
      defn.applyPlan(reader);
    }
  }
}
