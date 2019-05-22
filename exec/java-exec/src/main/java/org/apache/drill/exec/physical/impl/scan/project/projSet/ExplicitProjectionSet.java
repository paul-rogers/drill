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
package org.apache.drill.exec.physical.impl.scan.project.projSet;

import org.apache.drill.exec.physical.rowSet.ProjectionSet;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;

/**
 * Projection set based on an explicit set of columns provided
 * in the physical plan. Columns in the list are projected, others
 * are not.
 */

public class ExplicitProjectionSet implements ProjectionSet {

  private final RequestedTuple requestedProj;

  public ExplicitProjectionSet(RequestedTuple requestedProj) {
    this.requestedProj = requestedProj;
  }

  @Override
  public ColumnReadProjection readProjection(ColumnMetadata col) {
    RequestedColumn reqCol = requestedProj.get(col.name());
    if (reqCol == null) {
      return new UnprojectedReadColProj(col);
    }
    ProjectionSetFactory.validateProjection(reqCol, col);
    return new ExplicitReadColProj(col, reqCol);
  }
}
