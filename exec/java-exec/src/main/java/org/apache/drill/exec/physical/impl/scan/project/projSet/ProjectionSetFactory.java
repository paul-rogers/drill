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

import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.rowSet.ProjectionSet;
import org.apache.drill.exec.physical.rowSet.ProjectionSet.CustomTypeTransform;
import org.apache.drill.exec.physical.rowSet.project.ProjectionType;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.physical.rowSet.project.RequestedTupleImpl;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionDefn;

public class ProjectionSetFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectionSetFactory.class);

  public static ProjectionSet projectAll() { return WildcardProjectionSet.PROJECT_ALL; }

  public static ProjectionSet projectNone() { return EmptyProjectionSet.PROJECT_NONE; }

  public static ProjectionSet wrap(RequestedTuple mapProjection) {
    switch (mapProjection.type()) {
    case ALL:
      return projectAll();
    case NONE:
      return projectNone();
    case SOME:
      return new ExplicitProjectionSet(mapProjection);
    default:
      throw new IllegalStateException(mapProjection.type().toString());
    }
  }

  public static ProjectionSet build(List<SchemaPath> selection) {
    if (selection == null) {
      return projectAll();
    }
    return wrap(RequestedTupleImpl.parse(selection));
  }

  public static CustomTypeTransform simpleTransform(ColumnConversionFactory colFactory) {
    return new CustomTypeTransform() {

      @Override
      public ColumnConversionFactory transform(ColumnMetadata inputDefn,
          ColumnMetadata outputDefn, ConversionDefn defn) {
        return colFactory;
      }
    };
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
