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

import java.util.Collection;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.rowSet.ProjectionSet;
import org.apache.drill.exec.physical.rowSet.ProjectionSet.CustomTypeTransform;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.TupleProjectionType;
import org.apache.drill.exec.physical.rowSet.project.RequestedTupleImpl;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class ProjectionSetBuilder {

  private Collection<SchemaPath> projectionList;
  private RequestedTuple parsedProjection;
  private TupleMetadata providedSchema;
  private CustomTypeTransform transform;
  private Map<String, String> properties;

  /**
   * Record (batch) readers often read a subset of available table columns,
   * but want to use a writer schema that includes all columns for ease of
   * writing. (For example, a CSV reader must read all columns, even if the user
   * wants a subset. The unwanted columns are simply discarded.)
   * <p>
   * This option provides a projection list, in the form of column names, for
   * those columns which are to be projected. Only those columns will be
   * backed by value vectors; non-projected columns will be backed by "null"
   * writers that discard all values.
   *
   * @param projection the list of projected columns
   * @return this builder
   */

  public ProjectionSetBuilder projectionList(Collection<SchemaPath> projection) {
    projectionList = projection;
    return this;
  }

  public ProjectionSetBuilder parsedProjection(RequestedTuple projection) {
    parsedProjection = projection;
    return this;
  }

  public ProjectionSetBuilder outputSchema(TupleMetadata schema) {
    providedSchema = schema;
    return this;
  }

  public ProjectionSetBuilder transform(CustomTypeTransform transform) {
    this.transform = transform;
    return this;
  }

  public ProjectionSet build() {
    // If projection, build the projection map.
    // The caller might have already built the map. If so,
    // use it.

    Preconditions.checkArgument(projectionList == null || parsedProjection == null);
    if (projectionList != null) {
      parsedProjection = RequestedTupleImpl.parse(projectionList);
    }
    TupleProjectionType projType = parsedProjection == null ?
        TupleProjectionType.ALL : parsedProjection.type();
    switch (projType) {
    case ALL:
      if (providedSchema != null) {
        return new WildcardAndSchemaProjectionSet(providedSchema, properties, transform);
      } else if (transform != null) {
         return new WildcardAndTransformProjectionSet(transform);
      } else {
        return ProjectionSetFactory.projectAll();
      }
    case NONE:
      return ProjectionSetFactory.projectNone();
    case SOME:
      if (providedSchema == null) {
        return new ExplicitProjectionSet(parsedProjection);
      } else {
        return new ExplicitSchemaProjectionSet(parsedProjection, providedSchema, properties, transform);
      }
    default:
      throw new IllegalStateException(projType.toString());
    }
  }
}
