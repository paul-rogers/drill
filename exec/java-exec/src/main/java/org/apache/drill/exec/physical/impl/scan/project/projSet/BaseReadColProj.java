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

import org.apache.drill.exec.physical.rowSet.project.ImpliedTupleRequest;
import org.apache.drill.exec.physical.rowSet.project.ProjectionType;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;

public abstract class BaseReadColProj extends AbstractReadColProj {
  protected final RequestedColumn requestedCol;
  protected final ColumnMetadata outputSchema;

  public BaseReadColProj(ColumnMetadata readSchema) {
    this(readSchema, null, null);
  }

  public BaseReadColProj(ColumnMetadata readSchema, RequestedColumn requestedCol) {
    this(readSchema, requestedCol, null);
  }

  public BaseReadColProj(ColumnMetadata readSchema, ColumnMetadata outputSchema) {
    this(readSchema, null, outputSchema);
  }

  public BaseReadColProj(ColumnMetadata readSchema, RequestedColumn requestedCol, ColumnMetadata outputSchema) {
    super(readSchema);
    this.requestedCol = requestedCol;
    this.outputSchema = outputSchema;
  }

  @Override
  public ColumnMetadata outputSchema() { return outputSchema; }

  @Override
  public RequestedTuple mapProjection() {
    return requestedCol == null ?
        ImpliedTupleRequest.ALL_MEMBERS : requestedCol.mapProjection();
  }

  @Override
  public ProjectionType projectionType() {
    return requestedCol == null ? null : requestedCol.type();
  }
}
