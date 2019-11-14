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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.base.PlanStringBuilder;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class DisjunctionFilterSpec {
  public final String column;
  public final MinorType type;
  public final Object[] values;

  public DisjunctionFilterSpec(String column, MinorType type,
      Object[] values) {
    this.column = column;
    this.type = type;
    this.values = values;
  }

  public DisjunctionFilterSpec(List<RelOp> orTerms) {
    Preconditions.checkArgument(orTerms != null & orTerms.size() > 1);
    RelOp first = orTerms.get(0);
    column = first.colName;
    type = first.value.type;
    values = new Object[orTerms.size()];
    for (int i = 0; i < orTerms.size(); i++) {
      values[i] = orTerms.get(i).value.value;
    }
  }

  public List<List<RelOp>> distributeOverCnf(List<List<RelOp>> cnfTerms) {

    // Distribute the (single) OR clause over the AND clauses
    // (a AND b AND (x OR y)) --> ((a AND b AND x), (a AND b AND y))

    List<List<RelOp>> dnfTerms = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      RelOp orTerm = new RelOp(RelOp.Op.EQ, column,
          new ConstantHolder(type, values[i]));
      for (List<RelOp> andTerm : cnfTerms) {
        List<RelOp> newTerm = new ArrayList<>();
        newTerm.addAll(andTerm);
        newTerm.add(orTerm);
        dnfTerms.add(newTerm);
      }
    }
    return dnfTerms;
  }

  @Override
  public String toString() {
    PlanStringBuilder builder = new PlanStringBuilder(this);
    builder.field("column", column);
    builder.field("type", type);
    builder.field("values", values);
    return builder.toString();
  }
}
