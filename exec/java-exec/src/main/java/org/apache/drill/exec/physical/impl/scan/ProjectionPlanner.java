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
package org.apache.drill.exec.physical.impl.scan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.*;
import org.apache.drill.exec.physical.impl.scan.SchemaNegotiator.TableSchemaType;
import org.apache.drill.exec.physical.impl.scan.SelectionListPlan.Builder;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.ImplicitColumnExplorer;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;
import org.apache.hadoop.fs.Path;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 *
 * @see {@link ImplicitColumnExplorer}, the class from which this class
 * evolved
 */

public class ProjectionPlanner {


  private BatchSchema priorSchema;
  private MajorType nullColType;
  protected List<TableColumn> tableCols;
  private Map<String, TableColumn> tableIndex = CaseInsensitiveMap.newHashMap();

  protected List<NullColumn> nullCols = new ArrayList<>();
  protected TableSchemaType tableType;

  public ProjectionPlanner priorSchema(BatchSchema schema) {
    this.priorSchema = schema;
    return this;
  }

   public Builder setSource(Path filePath, String selectionRoot) {
    return this;
  }

  /**
   * Specify the set of table columns when known "early": during the projection
   * planning process.
   *
   * @param cols list of table columns in table schema order
   * @return this builder
   */

  public ProjectionPlanner tableColumns(List<MaterializedField> cols) {
    return tableColumns(cols);
  }

  private void selectAllPartitions() {
    if (dirPath == null) {
      return;
    }

    // If have a partition path, select the dir<n> columns as well.

    for (int i = 0; i < dirPath.length; i++) {
      OutputColumn.PartitionColumn partCol = new OutputColumn.PartitionColumn(partitionDesignator, outputCols.size(), i);
      partCol.setValue(dirPath[i]);
      partitionCols.add(partCol);
      staticCols.add(partCol);
      outputCols.add(partCol);
    }
  }

  public foo() {

    // If a prior schema build a merged mapping

    if (priorSchema != null  &&  mapPriorSchema()) {
      return;
    }

    // Map the table columns to produce the output schema.

    for (TableColumn col : tableCols) {
      col.projection = new EarlyProjectedColumn(starColumn, outputCols.size(), col);
      projectedCols.add(col.projection);
      outputCols.add(col.projection);
    }
  }
  public ProjectionPlanner tableColumns(Iterator<MaterializedField> iter) {
    tableCols = new ArrayList<>();
    while (iter.hasNext()) {
      MaterializedField col = iter.next();
      if (tableIndex.containsKey(col.getName())) {
        throw new IllegalStateException("Duplicate selection column: " + col.getName());
      }
      TableColumn dsCol = new TableColumn(tableCols.size(), col);
      tableCols.add(dsCol);
      tableIndex.put(col.getName(), dsCol);
    }
    return this;
  }

  public void tableSchema(List<TableColumn> schema) {
    tableCols = schema;
    for (TableColumn col : tableCols) {
      if (tableIndex.containsKey(col.name())) {
        throw new IllegalStateException("Duplicate selection column: " + col.name());
      }
      tableIndex.put(col.name(), col);
    }
  }

  public ProjectionPlanner tableColumns(BatchSchema schema) {
    return tableColumns(schema.iterator());
  }

  /**
   * Perform projection planning and return the final projection plan.
   * @return the finalized projection plan
   */

  public ScanProjection build() {
    mapSelectedCols();
    if (selectType == SelectType.ALL && useLegacyStarPlan) {
      selectAllPartitions();
      selectAllImplicitCols();
    }
    mapImplicitCols();
    mapPartitionCols();

    if (selectType == SelectType.ALL) {
      if (tableCols == null) {
        // SELECT *: is late schema

        tableType = TableSchemaType.LATE;
      } else {
        // Table columns provided: early schema

        tableType = TableSchemaType.EARLY;
      }
    } else if (columnsArrayCol != null) {

      // Has special `columns` column, is early schema because all columns
      // go into the one, known, array.

     tableType = TableSchemaType.EARLY;
    } else if (tableCols != null) {
      // Table columns provided: early schema

      tableType = TableSchemaType.EARLY;
    } else {
      // Is late schema.

      tableType = TableSchemaType.LATE;
    }
    return new ScanProjection(this);
  }

  private MajorType getNullColumnType() {
    if (nullColType == null) {
      nullColType = MajorType.newBuilder()
          .setMinorType(MinorType.NULL)
          .setMode(DataMode.OPTIONAL)
          .build();
    }
    return nullColType;
  }

  /**
   * Map the column into one of six categories.
   * <ol>
   * <li>Star column (to designate SELECT *)</li>
   * <li>Partition file column (dir0, dir1, etc.)</li>
   * <li>Implicit column (fqn, filepath, filename, suffix)</li>
   * <li>Special <tt>columns</tt> column which holds all columns as
   * an array.</li>
   * <li>Table column (if no schema provided or if the column matches
   * a column in the schema.</li>
   * <li>Null column for which no match can be found.</li>
   * </ol>
   *
   * @param index
   * @param inCol
   * @return
   */

  private void mapColumn(SelectColumn inCol) {
    if (inCol.isWildcard()) {

      // Star column: this is a SELECT * query.

      mapStarColumn(inCol);
      return;
    }

    if (tableCols == null) {
      // If a schema is not known, assume all columns are projected.
      // The scan operator must sort out actual schema batch-by-batch.

      mapLateProjectColumn(inCol);
      return;
    }

    TableColumn dsCol = tableIndex.get(inCol.name());
    if (dsCol != null) {
      // Early schema, found a table column match. This is a projected column.

      mapEarlyProjectedColumn(dsCol, inCol);
      return;
    } else {

      // Early schema, but not match. This is a null column.

      mapNullColumn(inCol);
      return;
    }
  }

  private void mapStarColumn(SelectColumn inCol) {
    // ...

    // If late schema, can't do more.

    if (tableCols == null) {
      return;
    }

    // If a prior schema build a merged mapping

    if (priorSchema != null  &&  mapPriorSchema()) {
      return;
    }

    // Map the table columns to produce the output schema.

    for (TableColumn col : tableCols) {
      col.projection = new EarlyProjectedColumn(starColumn, outputCols.size(), col);
      projectedCols.add(col.projection);
      outputCols.add(col.projection);
    }
  }

  private boolean mapPriorSchema() {
    // Can't match if have more table columns than prior columns,
    // new fields appeared in this table.

    if (priorSchema.getFieldCount() < tableCols.size()) {
      return false;
    }
    Map<String, MaterializedField> prior = new HashMap<>();
    for (MaterializedField field : priorSchema) {
      prior.put(field.getName(), field);
    }
    for (TableColumn col : tableCols) {
      MaterializedField priorCol = prior.get(col.name());

      // New field in this table; can't preserve schema

      if (priorCol == null) {
        return false;
      }

      // Can't preserve schema if column types differ.

      if (! priorCol.getType().equals(col.schema().getType())) {
        return false;
      }
    }

    // No need to preserve schema if the table schema is the same size as
    // the prior schema. Since we checked for extra table columns above,
    // if the schemas equal here, the columns are the same, only the types
    // (may) differ. So, nothing to preserve.

    if (tableCols.size() == priorSchema.getFieldCount()) {
      return false;
    }

    // Can't preserve schema if missing columns are required.

    for (MaterializedField field : priorSchema) {
      TableColumn col = tableIndex.get(field.getName());
      if (col == null  &&  field.getDataMode() == DataMode.REQUIRED) {
        return false;
      }
    }

    // This table schema is a subset of the prior
    // schema. Build the output schema using the prior schema as
    // the select list.

    for (MaterializedField field : priorSchema) {
      TableColumn col = tableIndex.get(field.getName());
      if (col == null) {
        NullColumn nullCol = new NullColumn(starColumn, outputCols.size(), field);
        nullCols.add(nullCol);
        outputCols.add(nullCol);
      } else {
        col.projection = new EarlyProjectedColumn(starColumn, outputCols.size(), col);
        projectedCols.add(col.projection);
        outputCols.add(col.projection);
      }
    }
    return true;
  }

  private void mapLateProjectColumn(SelectColumn inCol) {
    ProjectedColumn outCol = new LateProjectedColumn(inCol, outputCols.size());
    projectedCols.add(outCol);
    outputCols.add(outCol);
    inCol.projection = outCol;
  }

  private void mapEarlyProjectedColumn(TableColumn dsCol, SelectColumn inCol) {
    ProjectedColumn outCol = new EarlyProjectedColumn(inCol, outputCols.size(), dsCol);
    dsCol.projection = outCol;
    projectedCols.add(outCol);
    outputCols.add(outCol);
    inCol.projection = outCol;
  }

  private void mapNullColumn(SelectColumn inCol) {
    NullColumn outCol = new NullColumn(inCol, outputCols.size(), MaterializedField.create(inCol.name(), getNullColumnType()));
    nullCols.add(outCol);
    staticCols.add(outCol);
    outputCols.add(outCol);
    inCol.projection = outCol;
  }

}
