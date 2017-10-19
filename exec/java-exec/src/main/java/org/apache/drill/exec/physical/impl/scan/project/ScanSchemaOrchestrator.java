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
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.Projection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.VectorSource;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.UnresolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.ExplicitSchemaProjection;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.SchemaProjectionResolver;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.WildcardSchemaProjection;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;

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

public class ScanSchemaOrchestrator {

  public class ReaderSchemaOrchestrator implements VectorSource {

    private int readerBatchSize;
    private ResultSetLoaderImpl tableLoader;
    private int prevTableSchemaVersion = -1;
    private SchemaLevelProjection tableProjection;

    /**
     * Assembles the table, metadata and null columns into the final output
     * batch to be sent downstream. The key goal of this class is to "smooth"
     * schema changes in this output batch by absorbing trivial schema changes
     * that occur across readers.
     */

    private RowBatchMerger batchMerger;
    private VectorContainer tableContainer;

    public ReaderSchemaOrchestrator(ScanSchemaOrchestrator scanOrchestrator) {
      readerBatchSize = scanOrchestrator.scanBatchRecordLimit;
    }

    public void setBatchSize(int size) {
      readerBatchSize = Math.min(size, scanBatchRecordLimit);
    }

    public ResultSetLoader makeResultSetLoader(TupleMetadata tableSchema) {
      OptionBuilder options = new OptionBuilder();
      options.setRowCountLimit(readerBatchSize);

      // Set up a selection list if available and is a subset of
      // table columns. (Only needed for non-wildcard queries.)
      // The projection list includes all candidate table columns
      // whether or not they exist in the up-front schema. Handles
      // the odd case where the reader claims a fixed schema, but
      // adds a column later.

      if (! scanProj.projectAll()) {
        List<SchemaPath> projectedCols = new ArrayList<>();
        for (ColumnProjection col : scanProj.outputCols()) {
          if (col.nodeType() == UnresolvedColumn.UNRESOLVED) {
            projectedCols.add(((UnresolvedColumn) col).source());
          }
        }
        options.setProjection(projectedCols);
      }
      options.setSchema(tableSchema);

      // Create the table loader

      tableLoader = new ResultSetLoaderImpl(allocator, options.build());

      // If a schema is given, create a zero-row batch to announce the
      // schema downstream downstream in the form of an empty batch.

      if (tableSchema != null) {
        tableLoader.startBatch();
        endBatch();
      }

      return tableLoader;
    }

    public void startBatch() {
      tableLoader.startBatch();
    }

    /**
     * Build the final output batch by projecting columns from the three input sources
     * to the output batch. First, build the metadata and/or null columns for the
     * table row count. Then, merge the sources.
     */

    public void endBatch() {

      // Paranoid: downstream should have consumed the vectors,
      // but clear anyway to avoid a memory leak.

      if (outputContainer != null) {
        outputContainer.zeroVectors();
      }

      // Get the batch results in a container.

      tableContainer = tableLoader.harvest();

      // If the schema changed, set up the final projection based on
      // the new (or first) schema.

      if (prevTableSchemaVersion < tableLoader.schemaVersion()) {
        reviseOutputProjection();
      }

      // Fill in the null and metadata columns.

      int rowCount = tableContainer.getRecordCount();
      metadataManager.load(rowCount);
      if (nullColumnManager != null) {
        nullColumnManager.load(rowCount);
      }

      // Once all vectors are available, the batch merger
      // can be created if it does not yet exist.

      prepareMerger();

      // Combine metadata, nulls and batch data to form the final
      // output container.

      batchMerger.project(tableContainer.getRecordCount());
    }

    private void prepareMerger() {
      if (batchMerger != null) {
        return;
      }

      // All columns are now mapped to a vector source (table data, nulls
      // or metadata). Set up the mapping from the vector source to the output
      // schema.

      List<Projection> vectorProj = new ArrayList<>();
      for (ResolvedColumn col : tableProjection.output) {
        vectorProj.add(col.projection());
      }

      // Build the output container.

      outputContainer = new VectorContainer(allocator);

      // Initialize the projection mechanism.

      batchMerger = new RowBatchMerger(outputContainer, vectorProj);

      // Build the output vectors for the final output schema.

      batchMerger.buildOutput(vectorCache);
    }

    /**
     * Create the list of null columns by comparing the SELECT list against the
     * columns available in the batch schema. Create null columns for those that
     * are missing. This is done for the first batch, and any time the schema
     * changes. (For early-schema, the projection occurs once as the schema is set
     * up-front and does not change.) For a SELECT *, the null column check
     * only need be done if null columns were created when mapping from a prior
     * schema.
     */

    private void reviseOutputProjection() {

      // To refine

      // Do the table-schema level projection; the final matching
      // of projected columns to available columns.

      TupleMetadata tableSchema = tableLoader.harvestSchema();
      if (scanProj.hasWildcard()) {

        // Query contains a wildcard. The schema-level projection includes
        // all columns provided by the reader.

        tableProjection = new WildcardSchemaProjection(scanProj,
            tableSchema, this, tableResolvers);
      } else {

        // Explicit projection: include only those columns actually
        // requested by the query, which may mean filling in null
        // columns for projected columns that don't actually exist
        // in the table.

        ExplicitSchemaProjection explicitProjection =
            new ExplicitSchemaProjection(scanProj,
                tableSchema, this,
                nullColumnManager, tableResolvers);
        nullColumnManager.define(explicitProjection.nullCols());
        tableProjection = explicitProjection;
      }

//      scanProjector.projectionDefn.startSchema(tableLoader.writer().schema());
//      scanProjector.planProjection();

      metadataManager.define();
      prevTableSchemaVersion = tableLoader.schemaVersion();
    }

    @Override
    public VectorContainer container() {
      return tableContainer;
    }

    public void close() {
      if (tableLoader != null) {
        tableLoader.close();
        tableLoader = null;
      }
      if (batchMerger != null) {
        batchMerger.close();
        batchMerger = null;
      }
      metadataManager.endFile();
    }
  }

  // Inputs

  /**
   * Projection list provided by the physical plan.
   */

  protected List<SchemaPath> projection;

  // Configuration

  /**
   * Custom null type, if provided by the operator. If
   * not set, the null type is the Drill default.
   */

  protected MajorType nullType;

  /**
   * Creates the metadata (file and directory) columns, if needed.
   */

  protected MetadataManager metadataManager;
  private final BufferAllocator allocator;
  protected int scanBatchRecordLimit = ValueVector.MAX_ROW_COUNT;

  /**
   * List of resolvers used to resolve projection columns for each
   * new schema. Allows operators to introduce custom functionality
   * as a plug-in rather than by copying code or subclassing this
   * mechanism.
   */

  List<SchemaProjectionResolver> tableResolvers = new ArrayList<>();

  // Internal state

  /**
   * Cache used to preserve the same vectors from one output batch to the
   * next to keep the Project operator happy (which depends on exactly the
   * same vectors.
   * <p>
   * If the Project operator ever changes so that it depends on looking up
   * vectors rather than vector instances, this cache can be deprecated.
   */

  protected final ResultVectorCacheImpl vectorCache;
  protected ScanLevelProjection scanProj;
  protected boolean supportsMetadata;
  private ReaderSchemaOrchestrator currentReader;

  /**
   * Creates null columns if needed.
   */

  private NullColumnManager nullColumnManager;

  // Output

  public VectorContainer outputContainer;

  public ScanSchemaOrchestrator(BufferAllocator allocator) {
    this.allocator = allocator;
    vectorCache = new ResultVectorCacheImpl(allocator);
  }

  /**
   * Specify an optional metadata manager. Metadata is a set of constant
   * columns with per-reader values. For file-based sources, this is usually
   * the implicit and partition columns; but it could be other items for other
   * data sources.
   *
   * @param metadataMgr the application-specific metadata manager to use
   * for this scan
   */

  public void withMetadata(MetadataManager metadataMgr) {
    metadataManager = metadataMgr;
    metadataManager.bind(vectorCache);
    tableResolvers.add(metadataManager.resolver());
  }

  /**
   * Specify a custom batch record count. This is the maximum number of records
   * per batch for this scan. Readers can adjust this, but the adjustment is capped
   * at the value specified here
   *
   * @param scanBatchSize maximum records per batch
   */

  public void setBatchRecordLimit(int batchRecordLimit) {
    scanBatchRecordLimit = Math.max(1,
        Math.min(batchRecordLimit, ValueVector.MAX_ROW_COUNT));
  }

  /**
   * Specify the type to use for null columns in place of the standard
   * nullable int. This type is used for all missing columns. (Readers
   * that need per-column control need a different mechanism.)
   *
   * @param nullType
   */

  public void setNullType(MajorType nullType) {
    if (nullColumnManager != null) {
      throw new IllegalStateException("Too late to set the null type.");
    }
    this.nullType = nullType;
  }

  public void build(List<SchemaPath> projection) {
    this.projection = projection;

    // If no metadata manager was provided, create a mock
    // version just to keep code simple.

    if (metadataManager == null) {
      metadataManager = new NoOpMetadataManager();
    }

    // Bind metadata manager parser to scan projector.
    // A "real" (non-mock) metadata manager will provide
    // a projection parser. Use this to tell us that this
    // setup supports metadata.

    List<ScanProjectionParser> parsers = new ArrayList<>();
    ScanProjectionParser parser = metadataManager.projectionParser();
    if (parser != null) {
      supportsMetadata = true;
      parsers.add(parser);
    }

    // Some add-one (such as for the text reader "columns"
    // column) has a custom parser. Add those.

    addParsers(parsers);

    // Parse the projection list.

    scanProj = new ScanLevelProjection(projection, parsers);

    // If this is a wildcard query, we'll need a null column manager
    // to fill in missing columns.

    if (! scanProj.hasWildcard()) {
      nullColumnManager = new NullColumnManager(vectorCache, nullType);
    }
  }

  private void addParsers(List<ScanProjectionParser> parsers) { }

  public ReaderSchemaOrchestrator startReader() {
    closeReader();
    currentReader = new ReaderSchemaOrchestrator(this);
    return currentReader;
  }

  public VectorContainer output() {
    return outputContainer;
  }

  public void closeReader() {
    if (currentReader != null) {
      currentReader.close();
      currentReader = null;
    }
  }

  public void close() {
    closeReader();
    if (outputContainer != null) {
      outputContainer.zeroVectors();
      outputContainer = null;
    }
    vectorCache.close();
    if (nullColumnManager != null) {
      nullColumnManager.close();
      nullColumnManager = null;
    }
    metadataManager.close();
  }
}
