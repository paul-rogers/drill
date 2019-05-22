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

import org.apache.drill.exec.physical.rowSet.project.ProjectionType;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;

/**
 * Projected column based on an explicit projection, which may provide
 * constraints for the type of column allowed. No type conversion
 * needed: output type is whatever the reader chooses.
 */

public class ExplicitReadColProj extends AbstractReadColProj {
  protected final RequestedColumn requestedCol;

  public ExplicitReadColProj(ColumnMetadata col, RequestedColumn reqCol) {
    super(col);
    requestedCol = reqCol;
  }

  @Override
  public RequestedTuple mapProjection() { return requestedCol.mapProjection(); }

  @Override
  public ProjectionType projectionType() { return requestedCol.type(); }
}
