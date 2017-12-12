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
package org.apache.drill.exec.physical.rowSet.project;

import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.exec.physical.rowSet.project.ProjectedTuple.ProjectedColumn;

/**
 * Represents one name element. Like a {@link NameSegment}, except that this
 * version is an aggregate. If the projection list contains `a.b` and `a.c`,
 * then one name segment exists for a, and contains segments for both b and c.
 */

public class ProjectedColumnImpl implements ProjectedColumn {

  /**
   * Special marker to indicate that that a) the item is an
   * array, and b) that all indexes are to be projected.
   * Used when seeing both a and a[x].
   */

  private static final Set<Integer> ALL_INDEXES = new HashSet<>();

  private final ProjectedTuple parent;
  private final String name;
  private ProjectedTuple members;
  private Set<Integer> indexes;

  public ProjectedColumnImpl(ProjectedTuple parent, String name) {
    this.parent = parent;
    this.name = name;
  }

  @Override
  public String name() { return name; }
  @Override
  public boolean isWildcard() { return name.equals(SchemaPath.WILDCARD); }
  @Override
  public boolean isSimple() { return ! isTuple() && ! isArray(); }
  @Override
  public boolean isArray() { return indexes != null; }
  @Override
  public boolean isTuple() { return members != null; }

  public ProjectedTuple asTuple() {
    if (members == null) {
      members = new ProjectedTupleImpl(this);
    }
    return members;
  }

  public ProjectedTuple projectAllMembers(boolean projectAll) {
    members = projectAll ? NullProjectedTuple.ALL_MEMBERS : NullProjectedTuple.NO_MEMBERS;
    return members;
  }

  public void addIndex(int index) {
    if (indexes == null) {
      indexes = new HashSet<>();
    }
    if (indexes != ALL_INDEXES) {
      indexes.add(index);
    }
  }

  public void projectAllElements() {
    indexes = ALL_INDEXES;
  }

  @Override
  public boolean hasIndexes() {
    return indexes != null && indexes != ALL_INDEXES;
  }

  @Override
  public boolean hasIndex(int index) {
    return hasIndexes() ? indexes.contains(index) : false;
  }

  @Override
  public int maxIndex() {
    if (! hasIndexes()) {
      return 0;
    }
    int max = 0;
    for (Integer index : indexes) {
      max = Math.max(max, index);
    }
    return max;
  }

  @Override
  public boolean[] indexes() {
    if (! hasIndexes()) {
      return null;
    }
    int max = maxIndex();
    boolean map[] = new boolean[max+1];
    for (Integer index : indexes) {
      map[index] = true;
    }
    return map;
  }

  @Override
  public String fullName() {
    StringBuilder buf = new StringBuilder();
    buildName(buf);
    return buf.toString();
  }

  public boolean isRoot() { return parent == null; }

  protected void buildName(StringBuilder buf) {
    parent.buildName(buf);
    buf.append('`')
       .append(name)
       .append('`');
  }

  @Override
  public String summary() {
    if (isArray() && isTuple()) {
      return "repeated map";
    }
    if (isArray()) {
      return "array column";
    }
    if (isTuple()) {
      return "map column";
    }
    return "column";
  }

  @Override
  public boolean nameEquals(String target) {
    return name.equalsIgnoreCase(target);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf
      .append("[")
      .append(getClass().getSimpleName())
      .append(" name=")
      .append(name())
      .append(", type=")
      .append(summary());
    if (isArray()) {
      buf
        .append(", array=")
        .append(indexes);
    }
    if (isTuple()) {
      buf
        .append(", tuple=")
        .append(members);
    }
    buf.append("]");
    return buf.toString();
  }

  @Override
  public ProjectedTuple mapProjection() { return members; }
}