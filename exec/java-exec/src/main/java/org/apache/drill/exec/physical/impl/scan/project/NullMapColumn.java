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
package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple.NullMapTuple;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Represents a column which is implicitly a map (because it has children
 * in the project list), but which does not match any column in the table.
 * This kind of column gives rise to a map of null columns in the output.
 */

public class NullMapColumn extends ResolvedColumn {

  public static final int ID = 17;

  private final String name;
  private final NullMapTuple members;

  public NullMapColumn(String name, ResolvedTuple parent) {
    super(null, -1);
    this.name = name;
    this.members = new NullMapTuple(name, parent);
  }

  @Override
  public String name() { return name; }

  @Override
  public int nodeType() { return ID; }

  public ResolvedTuple members() { return members; }

  @Override
  public void project(ResolvedTuple dest) {
    dest.addVector(members.buildMap(name));
  }

  @Override
  public MaterializedField schema() {
    return MaterializedField.create(name, Types.required(MinorType.MAP));
  }
}