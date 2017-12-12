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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.PathSegment;

/**
 * Represents a wildcard: SELECT * when used at the root tuple.
 * When used with maps, means selection of all map columns, either
 * implicitly, or because the map itself is selected.
 */

public class NullProjectedTuple implements ProjectedTuple {

  public static final ProjectedTuple ALL_MEMBERS =
      new NullProjectedTuple(true);
  public static final ProjectedTuple NO_MEMBERS =
      new NullProjectedTuple(false);
  public static final List<ProjectedColumn> EMPTY_COLS = new ArrayList<>();

  private boolean allProjected;

  public NullProjectedTuple(boolean allProjected) {
    this.allProjected = allProjected;
  }

  @Override
  public boolean isProjected(String colName) { return allProjected; }

  @Override
  public ProjectedTuple mapProjection(String colName) {
    return ALL_MEMBERS;
  }

  @Override
  public void parseSegment(PathSegment child) { }

  @Override
  public ProjectedColumn get(String colName) { return null; }

  @Override
  public List<ProjectedColumn> projections() { return EMPTY_COLS; }

  @Override
  public void buildName(StringBuilder buf) { }
}
