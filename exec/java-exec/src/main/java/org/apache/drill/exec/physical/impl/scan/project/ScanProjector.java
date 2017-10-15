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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataProjection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.Builder;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.Path;

/**
 * Performs projection of a record reader, along with a set of static
 * columns, to produce the final "public" result set (record batch)
 * for the scan operator. Primarily solve the "vector permanence"
 * problem: that the scan operator must present the same set of vectors
 * to downstream operators despite the fact that the scan operator hosts
 * a series of readers, each of which builds its own result set.
 * <p>
 * This class "publishes" a vector container that has the final, projected
 * form of a scan. The projected schema include:
 * <ul>
 * <li>Columns from the reader.</li>
 * <li>Static columns, such as implicit or partition columns.</li>
 * <li>Null columns for items in the select list, but not found in either
 * of the above two categories.</li>
 * </ul>
 * The order of columns is that set by the select list (or, by the reader for
 * a <tt>SELECT *</tt> query.
 * <p>
 * The mapping handle a variety of cases:
 * <ul>
 * <li>An early-schema table (one in which we know the schema and
 * the schema remains constant for the whole table.</li>
 * <li>A late schema table (one in which we discover the schema as
 * we read the table, and where the schema can change as the read
 * progresses.)<ul>
 * <li>Late schema table with SELECT * (we want all columns, whatever
 * they happen to be.)</li>
 * <li>Late schema with explicit select list (we want only certain
 * columns when they happen to appear in the input.)</li></ul></li>
 * </ul>
 * <p>
 * Major tasks of this class include:
 * <ul>
 * <li>Project table columns (change position and or name).</li>
 * <li>Insert static and null columns.</li>
 * <li>Schema smoothing. That is, if table A produces columns (a, b), but
 * table B produces only (a), use the type of the first table's b column for the
 * null created for the missing b in table B.</li>
 * <li>Vector persistence: use the same set of vectors across readers as long
 * as the reader schema does not cause a "hard" schema change (change in type,
 * introduction of a new column.</li>
 * <li>Detection of schema changes (change of type, introduction of a new column
 * for a <tt>SELECT *</tt> query, changing the projected schema, and reporting
 * the change downstream.</li>
 * </ul>
 * A projection is needed to:
 * <ul>
 * <li>Reorder table columns</li>
 * <li>Select a subset of table columns</li>
 * <li>Fill in missing select columns</li>
 * <li>Fill in implicit or partition columns</li>
 * </ul>
 * Creates and returns the batch merger that does the projection.
 * <p>
 * To visualize this, assume we have numbered table columns, lettered
 * implicit, null or partition columns:<pre><code>
 * [ 1 | 2 | 3 | 4 ]    Table columns in table order
 * [ A | B | C ]        Static columns
 * </code></pre>
 * Now, we wish to project them into select order.
 * Let's say that the SELECT clause looked like this, with "t"
 * indicating table columns:<pre><code>
 * SELECT t2, t3, C, B, t1, A, t2 ...
 * </code></pre>
 * Then the projection looks like this:<pre><code>
 * [ 2 | 3 | C | B | 1 | A | 2 ]
 * </code></pre>
 * Often, not all table columns are projected. In this case, the
 * result set loader presents the full table schema to the reader,
 * but actually writes only the projected columns. Suppose we
 * have:<pre><code>
 * SELECT t3, C, B, t1,, A ...
 * </code></pre>
 * Then the abbreviated table schema looks like this:<pre><code>
 * [ 1 | 3 ]</code></pre>
 * Note that table columns retain their table ordering.
 * The projection looks like this:<pre><code>
 * [ 2 | C | B | 1 | A ]
 * </code></pre>
 * <p>
 * The projector is created once per schema, then can be reused for any
 * number of batches.
 * <p>
 * Merging is done in one of two ways, depending on the input source:
 * <ul>
 * <li>For the table loader, the merger discards any data in the output,
 * then exchanges the buffers from the input columns to the output,
 * leaving projected columns empty. Note that unprojected columns must
 * be cleared by the caller.</li>
 * <li>For implicit and null columns, the output vector is identical
 * to the input vector.</li>
 */

public class ScanProjector {

  /**
   * Handles schema mapping differences between early and late schema
   * tables.
   */

  private abstract class TableSchemaDriver {

    public ResultSetLoader makeTableLoader(int batchSize) {
      if (projectionDefn.fileProjection() == null) {
        throw new IllegalStateException("Must start file before setting table schema");
      }

      OptionBuilder options = new OptionBuilder();
      options.setRowCountLimit(batchSize);
      setupProjection(options);
      // Create the table loader

      setupSchema(options);
      tableLoader = new ResultSetLoaderImpl(allocator, options.build());
      setupProjection();
      return tableLoader;
    }

    protected abstract void setupProjection(OptionBuilder options);

    protected void setupSchema(OptionBuilder options) { }

    protected void setupProjection() { }

    public void endOfBatch() { }
  }

  /**
   * Handle schema mapping for early-schema tables: the schema is
   * known before the first batch is read and stays constant for the
   * entire table. The schema can be used to populate the batch
   * loader.
   */

  private class EarlySchemaDriver extends TableSchemaDriver {

    private final TupleMetadata tableSchema;

    public EarlySchemaDriver(TupleMetadata tableSchema) {
      this.tableSchema = tableSchema;
    }

    @Override
    protected void setupProjection(OptionBuilder options) {
      projectionDefn.startSchema(tableSchema);

      // Set up a selection list if available and is a subset of
      // table columns. (If we want all columns, either because of *
      // or we selected them all, then no need to add filtering.)

      if (! projectionDefn.scanProjection().projectAll()) {
        List<SchemaPath> projection = projectionDefn.tableProjection().projectedTableColumns();
        if (projection.size() < tableSchema.size()) {
          options.setProjection(projection);
        }
      }
    }

    @Override
    protected void setupSchema(OptionBuilder options) {

      // We know the table schema. Preload it into the
      // result set loader.

      options.setSchema(tableSchema);
    }

    @Override
    protected void setupProjection() {
      planProjection();

      // Set the output container to zero rows. Required so that we can
      // send the schema downstream in the form of an empty batch.

      output.getOutput().setRecordCount(0);
    }
  }

  /**
   * Handle schema mapping for a late-schema table. The schema is not
   * known until the first batch is read, and may change after any
   * batch. All we know up front is the list of columns (if any)
   * that the query projects. But, we don't know their types.
   */

  private class LateSchemaDriver extends TableSchemaDriver {

    /**
     * Tracks the schema version last seen from the table loader. Used to detect
     * when the reader changes the table loader schema.
     */

    private int prevTableSchemaVersion = -1;

    @Override
    protected void setupProjection(OptionBuilder options) {

      // Set up a selection list if available. Since the actual columns are
      // built on the fly, we need to set up the selection ahead of time and
      // can't optimize for the "selected all the columns" case.

      if (! projectionDefn.scanProjection().projectAll()) {
        // Temporary: convert names to paths. Need to handle full paths
        // throughout.

        List<SchemaPath> paths = new ArrayList<>();
        for (ColumnProjection col : projectionDefn.scanProjection().outputCols()) {
          if (col.nodeType() == UnresolvedColumn.UNRESOLVED) {
            paths.add(((UnresolvedColumn) col).source());
          }
        }
        options.setProjection(paths);
      }
    }

    @Override
    public void endOfBatch() {
      if (prevTableSchemaVersion < tableLoader.schemaVersion()) {
        projectionDefn.startSchema(tableLoader.writer().schema());
        planProjection();
        prevTableSchemaVersion = tableLoader.schemaVersion();
      }
    }
  }

  private final BufferAllocator allocator;

  /**
   * Cache used to preserve the same vectors from one output batch to the
   * next to keep the Project operator happy (which depends on exactly the
   * same vectors.
   * <p>
   * If the Project operator ever changes so that it depends on looking up
   * vectors rather than vector instances, this cache can be deprecated.
   */

  private final ResultVectorCacheImpl vectorCache;

  /**
   * The reader-specified null type if other than the default.
   */

  private final MajorType nullType;

  private final ProjectionLifecycle projectionDefn;

  private TableSchemaDriver schemaDriver;

  /**
   * The vector writer created here, and used by the reader. If the table is
   * early-schema, the schema is populated here. If late schema, the schema
   * is populated by the reader as the schema is discovered.
   */

  private ResultSetLoader tableLoader;

  /**
   * Creates the metadata (file and directory) columns, if needed.
   */

  private ConstantColumnLoader metadataColumnLoader;

  /**
   * Creates null columns if needed.
   */

  private NullColumnLoader nullColumnLoader;

  /**
   * Assembles the table, metadata and null columns into the final output
   * batch to be sent downstream. The key goal of this class is to "smooth"
   * schema changes in this output batch by absorbing trivial schema changes
   * that occur across readers.
   */

  private RowBatchMerger output;

  public ScanProjector(BufferAllocator allocator, ScanLevelProjection scanProj, FileMetadataProjection metadataProj, MajorType nullType) {
    this.allocator = allocator;
    this.projectionDefn = ProjectionLifecycle.newLifecycle(scanProj, metadataProj);
    this.nullType = nullType;
    vectorCache = new ResultVectorCacheImpl(allocator);
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

    // Optional form for late schema: pass a null table schema.

    if (tableSchema == null) {
      schemaDriver = new LateSchemaDriver();
    } else {
      schemaDriver = new EarlySchemaDriver(tableSchema);
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
    if (! projectionDefn.fileProjection().hasMetadata()) {
      return;
    }
    metadataColumnLoader = new ConstantColumnLoader(vectorCache,
        projectionDefn.fileProjection().metadataColumns());
  }

  /**
   * Update table and null column mappings when the table schema changes.
   * Fills in nulls when needed, "swaps out" nulls for table columns when
   * available.
   */

  private void planProjection() {
    RowBatchMerger.Builder builder = new RowBatchMerger.Builder()
        .vectorCache(vectorCache);
    buildNullColumns(builder);
    mapTableColumns(builder);
    mapMetadataColumns(builder);
    output = builder.build(allocator);
  }

  /**
   * Create the list of null columns by comparing the SELECT list against the
   * columns available in the table's schema. Create null columns for those that
   * are missing. If the table is early-schema, then this work was already done
   * in the static projection plan. Else, it has to be worked out for each new
   * batch when the table schema changes. For a SELECT *, the null column check
   * only need be done if null columns were created when mapping from a prior
   * schema.
   * @return the list of null columns for this table or batch
   */

  private void buildNullColumns(Builder builder) {
    TableLevelProjection tableProj = projectionDefn.tableProjection();
    if (! tableProj.hasNullColumns()) {
      return;
    }

    nullColumnLoader = new NullColumnLoader(vectorCache, tableProj.nullColumns(), nullType);

    // Map null columns from the null column loader schema into the output
    // schema.

    VectorContainer nullsContainer = nullColumnLoader.output();
    for (int i = 0; i < tableProj.nullColumns().size(); i++) {
      int projIndex = tableProj.nullProjectionMap()[i];
      builder.addDirectProjection(nullsContainer, i, projIndex);
    }
  }

  /**
   * Project selected, available table columns to their output schema positions.
   *
   * @param builder the batch merger builder
   */

  private void mapTableColumns(Builder builder) {

    // Projection of table columns is from the abbreviated table
    // schema after removing unprojected columns.
    // The table columns may be projected, so we want to get the
    // vector index of the table column. Non-projected table columns
    // don't have a vector, so can't use the table column index directly.

    TableLevelProjection tableProj = projectionDefn.tableProjection();
    VectorContainer tableContainer = tableLoader.outputContainer();
    TupleMetadata tableSchema = tableLoader.writer().schema();
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

      builder.addExchangeProjection(tableContainer, tableVectorIndex, projIndex );
    }
  }

  /**
   * Project implicit and partition columns into the output. Since
   * these columns are consistent across all readers, just project
   * the result set loader's own vectors; not need to do an exchange.
   * @param builder the batch merger builder
   */

  private void mapMetadataColumns(RowBatchMerger.Builder builder) {

    // Project static columns into their output schema locations

    if (metadataColumnLoader == null) {
      return;
    }
    VectorContainer metadataContainer = metadataColumnLoader.output();
    int metadataMap[] = projectionDefn.tableProjection().metadataProjection();
    for (int i = 0; i < metadataContainer.getNumberOfColumns(); i++) {
      builder.addDirectProjection(metadataContainer, i, metadataMap[i]);
    }
  }

  /**
   * Build the final output batch by projecting columns from the three input sources
   * to the output batch. First, build the metadata and/or null columns for the
   * table row count. Then, merge the sources.
   */

  public void publish() {
    VectorContainer tableContainer = tableLoader.harvest();
    schemaDriver.endOfBatch();
    int rowCount = tableContainer.getRecordCount();
    if (metadataColumnLoader != null) {
      metadataColumnLoader.load(rowCount);
    }
    if (nullColumnLoader != null) {
      nullColumnLoader.load(rowCount);
    }
    output.project(rowCount);
  }

  public VectorContainer output() {
    return output == null ? null : output.getOutput();
  }

  public void close() {
    if (metadataColumnLoader != null) {
      metadataColumnLoader.close();
      metadataColumnLoader = null;
    }
    closeTable();
    if (output != null) {
      output.close();
    }
    vectorCache.close();
    projectionDefn.close();
  }

  private void closeTable() {
    if (nullColumnLoader != null) {
      nullColumnLoader.close();
      nullColumnLoader = null;
    }
  }
}
