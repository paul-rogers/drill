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
import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.ProjectedColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.SelectColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.StaticColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.TableColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjection.TableSchemaType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.VectorContainer;

/**
 * Scan projector that takes a projection plan and:
 * <ul>
 * <li>Builds a result set loader that matches the table schema
 * (if any) provided.</li>
 * <li>Adds a build-time projection to create vectors only for selected
 * columns (an explicit selection list was provided.</li>
 * <li>Builds an internal loader to populate implicit, null or
 * partition columns, if needed.</li>
 * <li>Builds a batch merger to combine the table columns with
 * the static columns.</li>
 * </ul>
 * <p>
 * Various subclasses handle specialized cases:
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
 */

public abstract class ScanProjector {

  /**
   * Populate static columns (implicit, partition or null).
   */

  public static class StaticColumnLoader {
    private final ResultSetLoader loader;
    private final String values[];

    public StaticColumnLoader(BufferAllocator allocator, List<StaticColumn> defns) {
      loader = new ResultSetLoaderImpl(allocator);
      TupleSchema schema = loader.writer().schema();
      for (StaticColumn defn : defns) {
        schema.addColumn(defn.schema());
      }
      values = new String[defns.size()];
      for (int i = 0; i < defns.size(); i++) {
        values[i] = defns.get(i).value();
      }
    }

    public void load(int rowCount) {
      loader.startBatch();
      TupleLoader writer = loader.writer();
      for (int i = 0; i < rowCount; i++) {
        loader.startRow();
        for (int j = 0; j < values.length; j++) {
          if (values[j] == null) {
            writer.column(j).setNull();
          } else {
            writer.column(j).setString(values[j]);
          }
        }
        loader.saveRow();
      }
      loader.harvest();
    }

    public VectorContainer output() {
      return loader.outputContainer();
    }

    public void close() {
      loader.close();
    }
  }

  /**
   * Build  scan projector given an projection plan.
   */

  public static class Builder {
    private BufferAllocator allocator;
    private ScanProjection projection;
    private ResultSetLoaderImpl.OptionBuilder options;
    private StaticColumnLoader staticLoader;
    private RowBatchMerger batchMerger;

    public Builder(BufferAllocator allocator, ScanProjection projection) {
      this.allocator = allocator;
      this.projection = projection;
    }

    public ScanProjector build() {
      if (projection.tableSchemaType() == TableSchemaType.EARLY) {
        return buildEarlySchema();
      } else if (projection.isSelectAll()) {
        return buildLateSchemaSelectAll();
      } else {
        return buildLatesSchemaSelectList();
      }
    }

    public ScanProjector buildEarlySchema() {
      ResultSetLoader tableLoader = makeStaticTableLoader();
      VectorContainer output;
      if (requiresProjection()) {
        output = prepareProjection(tableLoader);
      } else {
        output = tableLoader.outputContainer();
      }
      return new EarlySchemaProjectPlan(tableLoader, staticLoader, batchMerger, output);
    }

    private boolean requiresProjection() {

      // Must do projection if we have static columns (implicit, partition
      // or null.)

      if (! projection.staticCols().isEmpty()) {
        return true;
      }

      // If select all, then the table determines column order, so no projection.

      if (projection.isSelectAll()) {
        return false;
      }

      // Must do projection if the table columns are returned in an order
      // different than that of the table.

      List<ProjectedColumn> projectedCols = projection.projectedCols();
      for (int i = 1; i < projectedCols.size(); i++) {
        TableColumn prevCol = projectedCols.get(i-1).source();
        TableColumn thisCol = projectedCols.get(i).source();
        if (prevCol.index() > thisCol.index()) {
          return true;
        }
      }

      // Returning columns in same order as the table.

      return false;
    }

    private ResultSetLoader makeStaticTableLoader() {
      if (! projection.isSelectAll() &&
          projection.projectedCols().size() < projection.tableCols().size()) {
       if (options == null) {
          options = new ResultSetLoaderImpl.OptionBuilder();
        }
       List<String> selection = new ArrayList<>();
        for (SelectColumn selCol : projection.queryCols()) {
          selection.add(selCol.name());
        }
        options.setSelection(selection);
      }
      ResultSetLoaderImpl loader;
      if (options == null) {
        loader = new ResultSetLoaderImpl(allocator);
      } else {
        loader = new ResultSetLoaderImpl(allocator, options.build());
      }
      TupleSchema schema = loader.writer().schema();
      for (TableColumn tableCol : projection.tableCols()) {
        schema.addColumn(tableCol.schema());
      }
      return loader;
    }

    protected VectorContainer prepareProjection(ResultSetLoader tableLoader) {
      RowBatchMerger.Builder builder = new RowBatchMerger.Builder();
      VectorContainer tableContainer = tableLoader.outputContainer();
      List<ProjectedColumn> projections = projection.projectedCols();
      for (int i = 0; i < projections.size(); i++) {
        builder.addProjection(tableContainer, projections.get(i).source().index(), i);
      }

      List<StaticColumn> staticCols = projection.staticCols();
      if (! staticCols.isEmpty()) {
        staticLoader = new StaticColumnLoader(allocator, staticCols);
        VectorContainer staticContainer = staticLoader.output();
        for (int i = 0; i < staticCols.size(); i++) {
          builder.addProjection(staticContainer, i, staticCols.get(i).index());
        }
      }

      batchMerger = builder.build(allocator);
      return batchMerger.getOutput();
    }

    public ScanProjector buildLateSchemaSelectAll() {
      return null;
    }

    public ScanProjector buildLatesSchemaSelectList() {
      return null;
    }
  }

  /**
   * Builds the result set loader and output result set for the case in which
   * the schema is known at the start and does not change for the life of the
   * read.
   */

  public static class EarlySchemaProjectPlan extends ScanProjector {

    public EarlySchemaProjectPlan(
        ResultSetLoader tableLoader, StaticColumnLoader staticLoader,
        RowBatchMerger batchMerger, VectorContainer output) {
      super(tableLoader, staticLoader, batchMerger, output);
    }

    @Override
    public VectorContainer harvest() {
      return doMerge();
    }
  }

//  public static abstract class ExplicitSelectLateSchemaProjectionPlan extends ProjectionPlan {
//
//    int outputSchemaVersion;
//    private int lastSyncPosition;
//
//    public ExplicitSelectLateSchemaProjectionPlan(ScanProjection projection,
//        ResultSetLoader tableLoader, VectorContainer output) {
//      super(projection, tableLoader, output);
//      throw new UnsupportedOperationException("Not yet");
//    }
//
//    private void evolveOuputSchema() {
//
//      // We care about the table schema only if this is a
//      // SELECT * query.
//
//      if (! projection.isSelectAll()) {
//        return;
//      }
//  if (tableSchemaVersion < tableLoader.schemaVersion()) {
//    handleTableSchemaChange();
//  }
//
//      //
//
//      // The table loader has new columns.
//
//      TupleSchema tableSchema = tableLoader.writer().schema();
//      int total = tableSchema.columnCount();
//      for (; lastSyncPosition < total; lastSyncPosition++) {
//        if (tableSchema.selected(lastSyncPosition)) {
//          output.addOrGet(tableSchema.column(lastSyncPosition));
//        }
//      }
//    }
//
//  }
//
//  public static class SelectAllEvolvingSchemaResultSetBuilder extends ExplicitSelectLateSchemaProjectionPlan {
//
//    public SelectAllEvolvingSchemaResultSetBuilder(ScanProjection projection,
//        ResultSetLoader tableLoader, VectorContainer output) {
//      super(projection, tableLoader, output);
//      // TODO Auto-generated constructor stub
//    }
//
//    @Override
//    protected void handleTableSchemaChange() {
//      // TODO Auto-generated method stub
//
//    }
//
//  }
//
//  public static class SelectedEvolvingSchemaResultSetBuilder extends ExplicitSelectLateSchemaProjectionPlan {
//
//    public SelectedEvolvingSchemaResultSetBuilder(ScanProjection projection,
//        ResultSetLoader tableLoader, VectorContainer output) {
//      super(projection, tableLoader, output);
//      // TODO Auto-generated constructor stub
//    }
//
//    @Override
//    protected void handleTableSchemaChange() {
//      // TODO Auto-generated method stub
//
//    }
//
//  }

  protected final ResultSetLoader tableLoader;
  protected final StaticColumnLoader staticColumnLoader;
  private RowBatchMerger batchMerger;
  private VectorContainer output;

  public ScanProjector(ResultSetLoader tableLoader, StaticColumnLoader staticLoader, RowBatchMerger batchMerger, VectorContainer output) {
    this.tableLoader = tableLoader;
    staticColumnLoader = staticLoader;
    this.batchMerger = batchMerger;
    this.output = output;
  }

  public abstract VectorContainer harvest();

  protected VectorContainer doMerge() {
    VectorContainer tableBatch = tableLoader.harvest();
    if (batchMerger == null) {
      return output;
    }
    int rowCount = tableBatch.getRecordCount();
    if (staticColumnLoader != null) {
      staticColumnLoader.load(rowCount);
    }
    batchMerger.project(rowCount);
    return output;
  }

  public VectorContainer output() { return output; }

  public ResultSetLoader tableLoader() { return tableLoader; }

  public void close() {
    if (staticColumnLoader != null) {
      staticColumnLoader.close();
    }
    if (tableLoader != null) {
      tableLoader.close();
    }
  }
}
