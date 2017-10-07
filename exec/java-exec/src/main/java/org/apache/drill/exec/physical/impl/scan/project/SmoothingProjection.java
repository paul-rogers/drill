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
import org.apache.drill.exec.physical.impl.scan.project.SchemaSmoother.IncompatibleSchemaException;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.TupleMetadata;

/**
 * Resolve a table schema against the prior schema. This works only if the
 * types match and if all columns in the table schema already appear in the
 * prior schema.
 */

public class SmoothingProjection extends SchemaLevelProjection {

  protected final List<MaterializedField> rewrittenFields = new ArrayList<>();

  public SmoothingProjection(ScanLevelProjection scanProj,
      TupleMetadata tableSchema,
      ResolvedTuple priorSchema,
      ResolvedTuple outputTuple,
      List<SchemaProjectionResolver> resolvers) throws IncompatibleSchemaException {

    super(resolvers);

    for (ResolvedColumn priorCol : priorSchema.columns()) {
      switch (priorCol.nodeType()) {
      case ResolvedTableColumn.ID:
      case ResolvedNullColumn.ID:
        // TODO: To fix this, the null column loader must declare
        // the column type somehow.
        resolveColumn(outputTuple, priorCol, tableSchema);
        break;
      default:
        resolveSpecial(outputTuple, priorCol, tableSchema);
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

  private void resolveColumn(ResolvedTuple outputTuple,
      ResolvedColumn priorCol, TupleMetadata tableSchema) throws IncompatibleSchemaException {
    int tableColIndex = tableSchema.index(priorCol.name());
    if (tableColIndex == -1) {
      resolveNullColumn(outputTuple, priorCol);
      return;
    }
    MaterializedField tableCol = tableSchema.column(tableColIndex);
    MaterializedField priorField = priorCol.schema();
    if (! tableCol.isPromotableTo(priorField, false)) {
      throw new IncompatibleSchemaException();
    }
    outputTuple.add(
        new ResolvedTableColumn(priorCol.name(), priorField, outputTuple, tableColIndex));
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

  private void resolveNullColumn(ResolvedTuple outputTuple,
      ResolvedColumn priorCol) throws IncompatibleSchemaException {
    if (priorCol.schema().getType().getMode() == DataMode.REQUIRED) {
      throw new IncompatibleSchemaException();
    }
    outputTuple.add(outputTuple.nullBuilder().add(priorCol.name(),
        priorCol.schema().getType()));
  }

  public List<MaterializedField> revisedTableSchema() { return rewrittenFields; }
}