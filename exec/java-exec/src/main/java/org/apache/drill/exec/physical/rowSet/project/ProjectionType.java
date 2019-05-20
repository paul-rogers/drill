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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.metadata.ColumnMetadata;

/**
 * Specifies the type of projection obtained by parsing the
 * projection list. The type is returned from a query of the
 * form "how is this column projected, if at all?"
 * <p>
 * The projection type allows the scan framework to catch
 * inconsistencies, such as projecting an array as a map,
 * and so on.
 */

public enum ProjectionType {

  /**
   * The column is not projected in the query.
   */

  UNPROJECTED,

  /**
   * Projection is a wildcard.
   */
  WILDCARD,     // *

  /**
   * Projection is by simple name. "General" means that
   * we have no hints about the type of the column from
   * the projection.
   */

  GENERAL,      // x

  /**
   * The column is projected as a scalar. This state
   * requires metadata beyond the projection list and
   * is returned only when that metadata is available.
   */

  SCALAR,       // x (from schema)

  /**
   * Applies to the parent of an x.y pair in projection: the
   * existence of a dotted-member tells us that the parent
   * must be a tuple (e.g. a Map.)
   */

  TUPLE,        // x.y

  /**
   * The projection includes an array suffix, so the column
   * must be an array.
   */

  ARRAY,        // x[0]

  /**
   * Combination of array and map hints.
   */

  TUPLE_ARRAY;  // x[0].y

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectionType.class);

  public boolean isTuple() {
    return this == ProjectionType.TUPLE || this == ProjectionType.TUPLE_ARRAY;
  }

  public boolean isArray() {
    return this == ProjectionType.ARRAY || this == ProjectionType.TUPLE_ARRAY;
  }

  /**
   * We can't tell, just from the project list, if a column must
   * be scalar. A column of the form "a" could be a scalar, but
   * that form is also consistent with maps and arrays.
   */
  public boolean isMaybeScalar() {
    return this == GENERAL || this == SCALAR;
  }

  public static ProjectionType typeFor(MajorType majorType) {
    if (majorType.getMinorType() == MinorType.MAP) {
      if (majorType.getMode() == DataMode.REPEATED) {
        return TUPLE_ARRAY;
      } else {
        return TUPLE;
      }
    }
    if (majorType.getMode() == DataMode.REPEATED) {
      return ARRAY;
    }
    return SCALAR;
  }

  public boolean isCompatible(ProjectionType other) {
    switch (other) {
    case UNPROJECTED:
    case GENERAL:
    case WILDCARD:
      return true;
    default:
      break;
    }

    switch (this) {
    case ARRAY:
    case TUPLE_ARRAY:
      return other == ARRAY || other == TUPLE_ARRAY;
    case SCALAR:
      return other == SCALAR;
    case TUPLE:
      return other == TUPLE;
    case UNPROJECTED:
    case GENERAL:
    case WILDCARD:
      return true;
    default:
      throw new IllegalStateException(toString());
    }
  }

  public void validateProjection(ColumnMetadata readSchema) {
    switch (readSchema.structureType()) {
    case TUPLE:
      if (readSchema.isArray()) {
        validateMapArray(readSchema);
      } else {
        validateSingleMap(readSchema);
      }
      break;
    case VARIANT:
      // Variant: UNION or (non-repeated) LIST
      if (readSchema.isArray()) {
        // (non-repeated) LIST (somewhat like a repeated UNION)
        validateList(readSchema);
      } else {
        // (Non-repeated) UNION
        validateUnion(readSchema);
      }
      break;
    case MULTI_ARRAY:
      validateRepeatedList(readSchema);
      break;
    default:
      validatePrimitive(readSchema);
    }
  }

  public void validateMapArray(ColumnMetadata readSchema) {
    // Compatible with all schema paths
  }

  public void validateSingleMap(ColumnMetadata readSchema) {
    switch (this) {
    case ARRAY:
    case TUPLE_ARRAY:
      incompatibleProjection(readSchema);
      break;
    default:
      break;
    }
  }

  public void validateList(ColumnMetadata readSchema) {
    switch (this) {
    case TUPLE:
    case TUPLE_ARRAY:
      incompatibleProjection(readSchema);
      break;
    default:
      break;
    }
  }

  public void validateUnion(ColumnMetadata readSchema) {
    switch (this) {
    case ARRAY:
    case TUPLE:
    case TUPLE_ARRAY:
      incompatibleProjection(readSchema);
      break;
    case UNPROJECTED:
      throw new UnsupportedOperationException("Drill does not currently support unprojected union columns: " +
          readSchema.name());
    default:
      break;
    }
  }

  public void validateRepeatedList(ColumnMetadata readSchema) {
    switch (this) {
    case TUPLE:
    case TUPLE_ARRAY:
      incompatibleProjection(readSchema);
      break;
    default:
      break;
    }
  }

  public void validatePrimitive(ColumnMetadata readSchema) {
    // Enforce correspondence between implied type from the projection list
    // and the actual type of the column.

    switch (this) {
    case ARRAY:
      if (! readSchema.isArray()) {
        incompatibleProjection(readSchema);
      }
      break;
    case TUPLE:
    case TUPLE_ARRAY:
      incompatibleProjection(readSchema);
      break;
    default:
      break;
    }
  }

  public String label() {
    switch (this) {
    case SCALAR:
      return "scalar (a)";
    case ARRAY:
      return "array (a[n])";
    case TUPLE:
      return "tuple (a.x)";
    case TUPLE_ARRAY:
      return "tuple array (a[n].x)";
    case WILDCARD:
      return "wildcard (*)";
    default:
      return name();
    }
  }

  private void incompatibleProjection(ColumnMetadata readSchema) {
    throw UserException.validationError()
      .message("Column type not compatible with projection format")
      .addContext("Column:", readSchema.name())
      .addContext("Projection type:", label())
      .addContext("Column type:", Types.getSqlTypeName(readSchema.majorType()))
      .build(logger);
  }
}
