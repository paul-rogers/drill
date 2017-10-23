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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.physical.impl.scan.project.NullColumnLoader.NullColumnSpec;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.Projection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.VectorSource;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.SchemaProjectionResolver;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.WildcardSchemaProjection;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;

/**
 * Implements a "schema smoothing" algorithm.
 * Schema persistence for the wildcard selection (i.e. SELECT *)
 * <p>
 * Constraints:
 * <ul>
 * <li>Adding columns causes a hard schema change.</li>
 * <li>Removing columns is allowed, use type from previous
 * schema, as long as previous mode was nullable or repeated.</li>
 * <li>Changing type or mode causes a hard schema change.</li>
 * <li>Changing column order is fine; use order from previous
 * schema.</li>
 * </ul>
 * This can all be boiled down to a simpler rule:
 * <ul>
 * <li>Schema persistence is possible if the output schema
 * from a prior schema can be reused for the current schema</li>
 * <li>Else, a hard schema change occurs and a new output
 * schema is derived from the new table schema.</li>
 * </ul>
 * The core idea here is to "unresolve" a fully-resolved table schema
 * to produce a new projection list that is the equivalent of using that
 * prior projection list in the SELECT. Then, keep that projection list only
 * if it is compatible with the next table schema, else throw it away and
 * start over from the actual scan projection list.
 * <p>
 * Algorithm:
 * <ul>
 * <li>If partitions are included in the wildcard, and the new
 * file needs more than the current one, create a new schema.</li>
 * <li>Else, treat partitions as select, fill in missing with
 * nulls.</li>
 * <li>From an output schema, construct a new select list
 * specification as though the columns in the current schema were
 * explicitly specified in the SELECT clause.</li>
 * <li>For each new schema column, verify that the column exists
 * in the generated SELECT clause and is of the same type.
 * If not, create a new schema.</li>
 * <li>Use the generated schema to plan a new projection from
 * the new schema to the prior schema.</li>
 * </ul>
 */

public class SchemaSmoother {

  /**
   * Exception thrown if the prior schema is not compatible with the
   * new table schema.
   */
  @SuppressWarnings("serial")
  public static class IncompatibleSchemaException extends Exception { }

  /**
   * Resolve a table schema against the prior schema. This works only if the
   * types match and if all columns in the table schema already appear in the
   * prior schema.
   */

  public static class SmoothingProjection extends SchemaLevelProjection {

    protected final List<NullColumnSpec> nullCols = new ArrayList<>();
    protected final List<MaterializedField> rewrittenFields = new ArrayList<>();
    protected final VectorSource nullSource;

    public SmoothingProjection(ScanLevelProjection scanProj,
        TupleMetadata tableSchema,
        VectorSource tableSource,
        VectorSource nullSource,
        List<SchemaProjectionResolver> resolvers,
        List<ResolvedColumn> priorSchema) throws IncompatibleSchemaException {

      super(tableSchema, tableSource, resolvers);
      this.nullSource = nullSource;

      for (ResolvedColumn priorCol : priorSchema) {
        switch (priorCol.nodeType()) {
        case ResolvedTableColumn.ID:
        case NullProjectedColumn.ID:
          resolveColumn(priorCol);
          break;
        default:
          resolveSpecial(priorCol);
        }
      }

      // Check if all table columns were matched. Since names are unique,
      // each column can be matched at most once. If the number of matches is
      // less than the total number of columns, then some columns were not
      // matched and we must start over.

      if (rewrittenFields.size() < tableSchema.size()) {
        throw new IncompatibleSchemaException();
      }
    }

    /**
     * Resolve a prior column against the current table schema. Resolves to
     * a table column, a null column, or throws an exception if the
     * schemas are incompatible
     *
     * @param priorCol a column from the prior schema
     * @throws IncompatibleSchemaException if the prior column exists in
     * the current table schema, but with an incompatible type
     */

    private void resolveColumn(ResolvedColumn priorCol) throws IncompatibleSchemaException {
      int tableColIndex = tableSchema.index(priorCol.name());
      if (tableColIndex == -1) {
        resolveNullColumn(priorCol);
        return;
      }
      MaterializedField tableCol = tableSchema.column(tableColIndex);
      MaterializedField priorField = priorCol.schema();
      if (! tableCol.isPromotableTo(priorField, false)) {
        throw new IncompatibleSchemaException();
      }
      resolveTableColumn(priorCol.name(), priorField, tableColIndex);
      rewrittenFields.add(priorField);
    }

    /**
     * A prior schema column does not exist in the present table column schema.
     * Create a null column with the same type as the prior column, as long as
     * the prior column was not required.
     *
     * @param priorCol the prior column to project to a null column
     * @throws IncompatibleSchemaException if the prior column was required
     * and thus cannot be null-filled
     */

    private void resolveNullColumn(ResolvedColumn priorCol) throws IncompatibleSchemaException {
      if (priorCol.schema().getType().getMode() == DataMode.REQUIRED) {
        throw new IncompatibleSchemaException();
      }
      NullProjectedColumn nullCol = new NullProjectedColumn(priorCol.name(),
          priorCol.schema().getType(),
          new Projection(nullSource, true, nullCols.size(), outputIndex()));
      output.add(nullCol);
      nullCols.add(nullCol);
    }

    @Override
    public List<NullColumnSpec> nullColumns() { return nullCols; }

    public List<MaterializedField> revisedTableSchema() { return rewrittenFields; }
  }

  private final ScanLevelProjection scanProj;
  private final VectorSource nullSource;
  private final List<SchemaProjectionResolver> resolvers;
  private List<ResolvedColumn> priorSchema;
  private int schemaVersion = 0;

  public SchemaSmoother(ScanLevelProjection scanProj,
      VectorSource nullSource,
      List<SchemaProjectionResolver> resolvers) {
    this.scanProj = scanProj;
    this.nullSource = nullSource;
    this.resolvers = resolvers;
  }

  public SchemaLevelProjection resolve(
      TupleMetadata tableSchema,
      VectorSource tableSource) {

    // If a prior schema exists, try resolving the new table using the
    // prior schema. If this works, use the projection. Else, start
    // over with the scan projection.

    if (priorSchema != null) {
      try {
        SmoothingProjection smoother = new SmoothingProjection(scanProj, tableSchema,
            tableSource, nullSource, resolvers, priorSchema);
        priorSchema = smoother.columns();
        return smoother;
      } catch (IncompatibleSchemaException e) {
        // Fall through
      }
    }

    // Can't use the prior schema. Start over with the original scan projection.
    // Type smoothing is provided by the vector cache; but a hard schema change
    // will occur because either a type has changed or a new column has appeared.
    // (Or, this is the first schema.)

    SchemaLevelProjection schemaProj = new WildcardSchemaProjection(scanProj,
        tableSchema, tableSource, resolvers);
    priorSchema = schemaProj.columns();
    schemaVersion++;
    return schemaProj;
  }

  public int schemaVersion() { return schemaVersion; }
}
