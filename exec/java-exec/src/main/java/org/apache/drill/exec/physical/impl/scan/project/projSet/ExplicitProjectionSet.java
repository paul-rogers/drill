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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.rowSet.project.ProjectionType;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;

/**
 * Projection set based on an explicit set of columns provided
 * in the physical plan. Columns in the list are projected, others
 * are not.
 */

public class ExplicitProjectionSet extends AbstractProjectionSet {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExplicitProjectionSet.class);

  private final RequestedTuple requestedProj;

  public ExplicitProjectionSet(RequestedTuple requestedProj, TypeConverter typeConverter) {
    super(typeConverter);
    this.requestedProj = requestedProj;
  }

  @Override
  public ColumnReadProjection readProjection(ColumnMetadata col) {
    RequestedColumn reqCol = requestedProj.get(col.name());
    if (reqCol == null) {
      return new UnprojectedReadColumn(col);
    }
    ColumnMetadata outputSchema = outputSchema(col);
    validateProjection(reqCol, outputSchema == null ? col : outputSchema);
    ColumnConversionFactory conv = conversion(col, outputSchema);
    return new ProjectedReadColumn(col, null, outputSchema, conv);
  }

  public static void validateProjection(RequestedColumn colReq, ColumnMetadata readCol) {
    if (colReq == null || readCol == null) {
      return;
    }
    ProjectionType type = colReq.type();
    if (type == null) {
      return;
    }
    ProjectionType neededType = ProjectionType.typeFor(readCol.majorType());
    if (type.isCompatible(neededType)) {
      return;
    }
    throw UserException.validationError()
      .message("Column type not compatible with projection format")
      .addContext("Column:", readCol.name())
      .addContext("Projection type:", type.label())
      .addContext("Column type:", Types.getSqlTypeName(readCol.majorType()))
      .build(logger);
  }
}
