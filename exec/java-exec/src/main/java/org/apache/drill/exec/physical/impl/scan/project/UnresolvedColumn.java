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

import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;

/**
 * Represents a projected column that has not yet been bound to a
 * table column, special column or a null column. Once bound, this
 * column projection is replaced with the detailed binding.
 * <p>
 * Each unprojected column is tied to an item in the projection
 * list from the physical plan. If the unresolved column is the
 * result of a wildcard expansion, then the column is associated
 * with that wildcard item.
 * <p>
 * The projection may provide hints about expected types. For
 * example, if the projection is "a.b", then we know that "a"
 * must be a map.
 * <p>
 * The projection may also be associated with an "provided" schema
 * column. In this case, we have additional constraints on the
 * types or modes allowed for the reader. The reader can insert
 * a type conversion to handle type differences, but no conversion
 * will convert an array to a map, or a map to a scalar.
 */

public abstract class UnresolvedColumn implements ColumnProjection {

  /**
   * Represents a column requested in the project list, but which has
   * not yet been resolved against a column provided by a reader.
   */

  public static class UnresolvedProjectedColumn extends UnresolvedColumn {

    public UnresolvedProjectedColumn(RequestedColumn inCol) {
      super(inCol);
    }

    @Override
    public String name() { return inCol.name(); }

    public RequestedColumn element() { return inCol; }

    @Override
    public String toString() {
      return new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(", incol=")
        .append(inCol.toString())
        .toString();
    }
  }

  /**
   * Represents the wildcard (*) in a projection list which has not yet
   * been expanded against a set of columns provided by the reader.
   */

  public static class UnresolvedWildcardColumn extends UnresolvedColumn {

    public UnresolvedWildcardColumn(RequestedColumn inCol) {
      super(inCol);
    }

    @Override
    public String name() { return inCol.name(); }
  }

  /**
   * Special case of a projection column. The projection list contained a wildcard,
   * and the environment provided a "provisioned" schema. In this case, the wildcard
   * expands against the provided schema, but must be further resolved against a
   * reader column.
   */
  public static class UnresolvedSchemaColumn extends UnresolvedColumn {

    private final ColumnMetadata colDefn;

    public UnresolvedSchemaColumn(RequestedColumn inCol, ColumnMetadata colDefn) {
      super(inCol);
      this.colDefn = colDefn;
    }

    public ColumnMetadata metadata() { return colDefn; }

    @Override
    public String name() { return colDefn.name(); }
  }

  /**
   * The original physical plan column to which this output column
   * maps. In some cases, multiple output columns map map the to the
   * same "input" (to the projection process) column.
   */

  protected final RequestedColumn inCol;

  public UnresolvedColumn(RequestedColumn inCol) {
    this.inCol = inCol;
  }

  public abstract boolean isTuple();
  public abstract boolean isArray();
  public abstract String fullName();
  public abstract RequestedTuple mapProjection();
}