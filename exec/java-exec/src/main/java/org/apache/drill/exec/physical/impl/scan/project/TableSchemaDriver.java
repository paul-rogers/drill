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

import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.ProjectionSet;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;

/**
 * Handles schema mapping differences between early and late schema
 * tables.
 */

public abstract class TableSchemaDriver {

  /**
   *
   */
  protected final ScanProjector scanProjector;
  /**
   * The vector writer created here, and used by the reader. If the table is
   * early-schema, the schema is populated here. If late schema, the schema
   * is populated by the reader as the schema is discovered.
   */

  protected ResultSetLoader tableLoader;


  /**
   * @param scanProjector
   */
  TableSchemaDriver(ScanProjector scanProjector) {
    this.scanProjector = scanProjector;
  }

  public ResultSetLoader makeTableLoader(int batchSize) {
    OptionBuilder options = new OptionBuilder();
    options.setRowCountLimit(batchSize);
    setupProjection(options);
    // Create the table loader

    setupSchema(options);
    tableLoader = new ResultSetLoaderImpl(this.scanProjector.allocator, options.build());
    setupProjection();
    return tableLoader;
  }

  protected abstract void setupProjection(OptionBuilder options);

  protected void setupSchema(OptionBuilder options) { }

  protected void setupProjection() { }

  public int endOfBatch() {
    return tableLoader.harvest().getRecordCount();
  }

  public ResultSetLoader loader() {
    return tableLoader;
  }

  public ProjectionSet mapTableColumns(SchemaLevelProjection tableProj) {

    // Projection of table columns is from the abbreviated table
    // schema after removing unprojected columns.
    // The table columns may be projected, so we want to get the
    // vector index of the table column. Non-projected table columns
    // don't have a vector, so can't use the table column index directly.

    VectorContainer tableContainer = tableLoader.outputContainer();
    TupleMetadata tableSchema = tableLoader.writer().schema();
    ProjectionSet projSet = new ProjectionSet(tableContainer);
    int tableColCount = tableSchema.size();
    for (int i = 0; i < tableColCount; i++) {

      // Skip unprojected table columns

      if (! tableProj.projectionMap()[i]) {
        continue;
      }

      // Get the output schema position for the column

      int projIndex = tableProj.tableColumnProjectionMap()[i];

      // Get the physical vector index for the column (reflects
      // column reordering and removing unprojected columns.)

      int tableVectorIndex = tableProj.logicalToPhysicalMap()[i];

      // Project from physical table loader schema to output schema

      projSet.addExchangeProjection(tableVectorIndex, projIndex );
    }
    return projSet;
  }
}