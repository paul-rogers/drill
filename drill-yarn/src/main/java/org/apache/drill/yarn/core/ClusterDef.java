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
package org.apache.drill.yarn.core;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.apache.drill.yarn.appMaster.TaskSpec;
import org.mortbay.log.Log;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

public class ClusterDef {
  // The following keys are relative to the cluster group definition

  public static final String GROUP_NAME = "name";
  public static final String GROUP_TYPE = "type";
  public static final String GROUP_SIZE = "count";

  // For the labeled pool

  public static final String DRILLBIT_LABEL = "drillbit-label-expr";
  public static final String AM_LABEL = "am-label-expr";

  /**
   * Defined cluster tier types. The value of the type appears as the value of
   * the {@link $CLUSTER_TYPE} parameter in the config file.
   */

  public enum GroupType {
    BASIC("basic"), LABELED("labeled");

    private String value;

    private GroupType(String value) {
      this.value = value;
    }

    public static GroupType toEnum(String value) {
      for (GroupType type : GroupType.values()) {
        if (type.value.equalsIgnoreCase(value)) {
          return type;
        }
      }
      return null;
    }

    public String toValue() {
      return value;
    }
  }

  public static class ClusterGroup {
    public String name;
    public int count;
    public GroupType type;

    public void getPairs(int index, List<NameValuePair> pairs) {
      String key = DrillOnYarnConfig.append(DrillOnYarnConfig.CLUSTERS,
          Integer.toString(index));
      addPairs(pairs, key);
    }

    protected void addPairs(List<NameValuePair> pairs, String key) {
      pairs.add(
          new NameValuePair(DrillOnYarnConfig.append(key, GROUP_NAME), name));
      pairs.add(
          new NameValuePair(DrillOnYarnConfig.append(key, GROUP_TYPE), type));
      pairs.add(
          new NameValuePair(DrillOnYarnConfig.append(key, GROUP_SIZE), count));
    }

    public void dump(String prefix, PrintStream out) {
      out.print(prefix);
      out.print("name = ");
      out.println(name);
      out.print(prefix);
      out.print("type = ");
      out.println(type.toValue());
      out.print(prefix);
      out.print("count = ");
      out.println(count);
    }

    public void load(Map<String, Object> pool, int index) {
      try {
        count = (Integer) pool.get(GROUP_SIZE);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            "Expected an integer for " + GROUP_SIZE + " for tier " + index);
      }
      Object nameValue = pool.get(GROUP_NAME);
      if (nameValue != null) {
        name = nameValue.toString();
      }
      if (DoYUtil.isBlank(name)) {
        name = "tier-" + Integer.toString(index);
      }
    }

    public void modifyTaskSpec(TaskSpec taskSpec) {
    }
  }

  public static class BasicGroup extends ClusterGroup {

  }

  public static class LabeledGroup extends ClusterGroup {
    public String drillbitLabelExpr;

    @Override
    public void load(Map<String, Object> pool, int index) {
      super.load(pool, index);
      drillbitLabelExpr = (String) pool.get(DRILLBIT_LABEL);
      if (drillbitLabelExpr == null) {
        Log.warn("Labeled pool is missing the drillbit label expression ("
            + DRILLBIT_LABEL + "), will treat pool as basic.");
      }
    }

    @Override
    public void dump(String prefix, PrintStream out) {
      out.print(prefix);
      out.print("Drillbit label expr = ");
      out.println((drillbitLabelExpr == null) ? "<none>" : drillbitLabelExpr);
    }

    @Override
    protected void addPairs(List<NameValuePair> pairs, String key) {
      super.addPairs(pairs, key);
      pairs.add(new NameValuePair(DrillOnYarnConfig.append(key, DRILLBIT_LABEL),
          drillbitLabelExpr));
    }

    @Override
    public void modifyTaskSpec(TaskSpec taskSpec) {
      taskSpec.containerSpec.nodeLabelExpr = drillbitLabelExpr;
    }
  }

  /**
   * Deserialize a node tier from the configuration file.
   *
   * @param n
   * @return
   */

  public static ClusterGroup getCluster(Config config, int n) {
    int index = n + 1;
    ConfigList tiers = config.getList(DrillOnYarnConfig.CLUSTERS);
    ConfigValue value = tiers.get(n);
    @SuppressWarnings("unchecked")
    Map<String, Object> tier = (Map<String, Object>) value.unwrapped();
    String type;
    try {
      type = tier.get(GROUP_TYPE).toString();
    } catch (NullPointerException e) {
      throw new IllegalArgumentException(
          "Pool type is required for cluster group " + index);
    }
    GroupType groupType = GroupType.toEnum(type);
    if (groupType == null) {
      throw new IllegalArgumentException(
          "Undefined type for cluster group " + index + ": " + type);
    }
    ClusterGroup tierDef;
    switch (groupType) {
    case BASIC:
      tierDef = new BasicGroup();
      break;
    case LABELED:
      tierDef = new LabeledGroup();
      break;
    default:
      assert false;
      throw new IllegalStateException(
          "Undefined cluster group type: " + groupType);
    }
    tierDef.type = groupType;
    tierDef.load(tier, index);
    return tierDef;
  }
}
