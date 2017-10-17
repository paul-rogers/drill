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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.Builder;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.Path;

public class ScanProjector {

  final BufferAllocator allocator;
  private final ResultVectorCacheImpl vectorCache;

  final ProjectionLifecycle projectionDefn;

  private TableSchemaDriver schemaDriver;
  private MetadataColumnManager metadataColumnManager;
  private NullColumnManager nullColumnManager;
  RowBatchMerger output;

  public ScanProjector(BufferAllocator allocator, ScanLevelProjection scanProj, MetadataManager metadataProj, MajorType nullType) {
    this.allocator = allocator;
    this.projectionDefn = ProjectionLifecycle.newLifecycle(scanProj, metadataProj);
    vectorCache = new ResultVectorCacheImpl(allocator);
    nullColumnManager = new NullColumnManager(vectorCache, nullType);
    metadataColumnManager = new MetadataColumnManager(vectorCache);
  }

  public void startFile(Path filePath) {
    closeTable();
    projectionDefn.startFile(filePath);
    buildMetadataColumns();
  }

  /**
   * Create a result set loader for the case in which the table schema is
   * known and is static (does not change between batches.)
   * @param batchSize
   * @param tableSchemaType
   * @return the result set loader for this table
   */

  public ResultSetLoader makeTableLoader(TupleMetadata tableSchema, int batchSize) {

    if (projectionDefn.fileProjection() == null) {
      throw new IllegalStateException("Must start file before setting table schema");
    }

    // Optional form for late schema: pass a null table schema.

    if (tableSchema == null) {
      schemaDriver = new LateSchemaDriver(this);
    } else {
      schemaDriver = new EarlySchemaDriver(this, tableSchema);
    }

    return schemaDriver.makeTableLoader(batchSize);
  }

  public ResultSetLoader makeTableLoader(TupleMetadata tableSchema) {
    return makeTableLoader(tableSchema, ValueVector.MAX_ROW_COUNT);
  }

  /**
   * The implicit (file metadata) and partition (directory metadata)
   * columns are static: they are the same across
   * all readers. If any such columns exist, build the loader for them.
   */

  private void buildMetadataColumns() {
    metadataColumnManager.buildColumns(projectionDefn.fileProjection());
  }

  /**
   * Update table and null column mappings when the table schema changes.
   * Fills in nulls when needed, "swaps out" nulls for table columns when
   * available.
   */

  void planProjection() {
    RowBatchMerger.Builder builder = new RowBatchMerger.Builder()
        .vectorCache(vectorCache);
    buildNullColumns(builder);
    mapTableColumns(builder);
    mapMetadataColumns(builder);
    output = builder.build(allocator);
  }

  private void buildNullColumns(Builder builder) {
    builder.addProjectionSet(nullColumnManager.buildProjection(projectionDefn));
  }

  /**
   * Project selected, available table columns to their output schema positions.
   *
   * @param builder the batch merger builder
   */

  private void mapTableColumns(Builder builder) {

    builder.addProjectionSet(schemaDriver.mapTableColumns(projectionDefn.tableProjection()));
  }

  /**
   * Project implicit and partition columns into the output. Since
   * these columns are consistent across all readers, just project
   * the result set loader's own vectors; not need to do an exchange.
   * @param builder the batch merger builder
   */

  private void mapMetadataColumns(RowBatchMerger.Builder builder) {

    // Project static columns into their output schema locations

    builder.addProjectionSet(metadataColumnManager.mapMetadataColumns(projectionDefn));
  }

  public void publish() {
    int rowCount = schemaDriver.endOfBatch();
    metadataColumnManager.load(rowCount);
    nullColumnManager.load(rowCount);
    output.project(rowCount);
  }

  public VectorContainer output() {
    return output == null ? null : output.getOutput();
  }

  public void close() {
    metadataColumnManager.close();
    closeTable();
    if (output != null) {
      output.close();
    }
    vectorCache.close();
    projectionDefn.close();
  }

  private void closeTable() {
    nullColumnManager.close();
  }
}
