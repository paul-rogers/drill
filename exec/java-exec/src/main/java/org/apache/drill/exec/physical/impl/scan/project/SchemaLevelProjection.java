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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.TupleMetadata;

/**
 * Computes the full output schema given a table (or batch)
 * schema. Takes the original, unresolved output list from the projection
 * definition, merges it with the file, directory and table schema information,
 * and produces a partially or fully resolved output list.
 * <p>
 * A "resolved" projection list is a list of concrete columns: table
 * columns, nulls, file metadata or partition metadata. An unresolved list
 * has either table column names, but no match, or a wildcard column.
 * <p>
 * The idea is that the projection list moves through stages of resolution
 * depending on which information is available. An "early schema" table
 * provides schema information up front, and so allows fully resolving
 * the projection list on table open. A "late schema" table allows only a
 * partially resolved projection list, with the remainder of resolution
 * happening on the first (or perhaps every) batch.
 * <p>
 * Data source (table) schema can be of two forms:
 * <ul>
 * <li>Early schema: the schema is known before reading data. A JDBC data
 * source is an example, as is a CSV reader for a file with headers.</li>
 * <li>Late schema: the schema is not known until data is read, and is
 * discovered on the fly. Example: JSON, which declares values as maps
 * without an up-front schema.</li>
 * </ul>
 * These two forms give rise to distinct ways of planning the projection.
 * <p>
 * The final result of the projection is a set of "output" columns: a set
 * of columns that, taken together, defines the row (bundle of vectors) that
 * the scan operator produces. Columns are ordered: the order specified here
 * must match the order that columns appear in the result set loader and the
 * vector container so that code can access columns by index as well as name.
 *
 * @see {@link ImplicitColumnExplorer}, the class from which this class
 * evolved
 */

public class SchemaLevelProjection {

  /**
   * Schema-level projection is customizable. Implement this interface, and
   * add an instance to the scan orchestrator, to perform custom mappings
   * from unresolved columns (perhaps of an extension-specified type) to
   * final projected columns. The metadata manager, for example, implements
   * this interface to map metadata columns.
   */

  public interface SchemaProjectionResolver {
    void bind(SchemaLevelProjection projection);
    boolean resolveColumn(ColumnProjection col, ResolvedTuple tuple);
  }
  /**
   * Perform a wildcard projection. In this case, the query wants all
   * columns in the source table, so the table drives the final projection.
   * Since we include only those columns in the table, there is no need
   * to create null columns. Example: SELECT *
   */

  public static class WildcardSchemaProjection extends SchemaLevelProjection {

    public WildcardSchemaProjection(ScanLevelProjection scanProj,
        TupleMetadata tableSchema,
        ResolvedTuple rootTuple,
        List<SchemaProjectionResolver> resolvers) {
      super(tableSchema, rootTuple, resolvers);
      for (ColumnProjection col : scanProj.columns()) {
        if (col.nodeType() == UnresolvedColumn.WILDCARD) {
          for (int i = 0; i < tableSchema.size(); i++) {
            MaterializedField colSchema = tableSchema.column(i);
            rootOutputTuple.add(
                new ResolvedTableColumn(colSchema.getName(),
                    colSchema, rootOutputTuple, i));
          }
        } else {
          resolveSpecial(col);
        }
      }
    }
  }
  /**
   * Perform a schema projection for the case of an explicit list of
   * projected columns. Example: SELECT a, b, c.
   * <p>
   * An explicit projection starts with the requested set of columns,
   * then looks in the table schema to find matches. That is, it is
   * driven by the query itself.
   * <p>
   * An explicit projection may include columns that do not exist in
   * the source schema. In this case, we fill in null columns for
   * unmatched projections.
   */

  public static class ExplicitSchemaProjection extends SchemaLevelProjection {

    public ExplicitSchemaProjection(ScanLevelProjection scanProj,
        TupleMetadata tableSchema,
        ResolvedTuple rootTuple,
        List<SchemaProjectionResolver> resolvers) {
      super(tableSchema, rootTuple, resolvers);

      for (ColumnProjection col : scanProj.columns()) {
        if (col.nodeType() == UnresolvedColumn.UNRESOLVED) {
          resolveColumn((UnresolvedColumn) col);
        } else {
          resolveSpecial(col);
        }
      }
    }

    private void resolveColumn(UnresolvedColumn col) {
      int tableColIndex = tableSchema.index(col.name());
      if (tableColIndex == -1) {
        resolveNullColumn(col);
      } else {
        rootOutputTuple.add(
            new ResolvedTableColumn(col.name(),
                tableSchema.column(tableColIndex),
                rootOutputTuple, tableColIndex));
      }
    }

    /**
     * Resolve a null column. This is a projected column which does not match
     * an implicit or table column. We consider two cases: a simple top-level
     * column reference ("a", say) and an implied map reference ("a.b", say.)
     * If the column appears to be a map, determine the set of children, which
     * map appear to any depth, that were requested.
     *
     * @param col
     */

    private void resolveNullColumn(UnresolvedColumn col) {
      ResolvedColumn nullCol;
      if (col.element().isTuple()) {
        nullCol = resolveMapMembers(col.element());
      } else {
        nullCol = rootOutputTuple.nullBuilder.add(col.name());
      }
      rootOutputTuple.add(nullCol);
    }

    /**
     * A child column of a map is not projected. Recurse to determine the full
     * set of nullable child columns.
     *
     * @param projectedColumn the map column which was projected
     * @return a list of null markers for the requested children
     */

    private ResolvedColumn resolveMapMembers(RequestedColumn col) {
      NullMapColumn mapCol = new NullMapColumn(col.name(), rootOutputTuple);
      ResolvedTuple members = mapCol.members();
      for (RequestedColumn child : col.mapProjection().projections()) {
        if (child.isTuple()) {
          members.add(resolveMapMembers(child));
        } else {
          members.add(rootOutputTuple.nullBuilder.add(child.name()));
        }
      }
      return mapCol;
    }
  }

  protected final TupleMetadata tableSchema;
  protected final ResolvedTuple rootOutputTuple;
  protected final List<SchemaProjectionResolver> resolvers;

  protected SchemaLevelProjection(
        TupleMetadata tableSchema,
        ResolvedTuple rootTuple,
        List<SchemaProjectionResolver> resolvers) {
    if (resolvers == null) {
      resolvers = new ArrayList<>();
    }
    rootOutputTuple = rootTuple;
    this.tableSchema = tableSchema;
    this.resolvers = resolvers;
    for (SchemaProjectionResolver resolver : resolvers) {
      resolver.bind(this);
    }
  }

  protected void resolveSpecial(ColumnProjection col) {
    for (SchemaProjectionResolver resolver : resolvers) {
      if (resolver.resolveColumn(col, rootOutputTuple)) {
        return;
      }
    }
    throw new IllegalStateException("No resolver for column: " + col.nodeType());
  }

  public TupleMetadata tableSchema() { return tableSchema; }
  public ResolvedTuple output() { return rootOutputTuple; }
}
