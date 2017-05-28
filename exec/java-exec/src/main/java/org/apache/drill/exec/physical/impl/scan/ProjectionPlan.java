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
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ProjectedColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ScanProjection;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ScanProjection.TableSchemaType;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.SelectColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.StaticColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.TableColumn;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.VectorContainer;

public abstract class ProjectionPlan {

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
          writer.column(j).setString(values[j]);
        }
        loader.saveRow();
      }
      loader.harvest();
    }

    public VectorContainer output() {
      return loader.outputContainer();
    }
  }

  public static class ProjectionExecBuiler {
    private BufferAllocator allocator;
    private ScanProjection projection;
    private ResultSetLoaderImpl.OptionBuilder options;

    public ProjectionExecBuiler(BufferAllocator allocator, ScanProjection projection) {
      this.allocator = allocator;
      this.projection = projection;
    }

    public ProjectionPlan build() {
      if (projection.tableSchemaType() == TableSchemaType.EARLY) {
        return buildEarlySchema();
      } else if (projection.isSelectAll()) {
        return buildLateSchemaSelectAll();
      } else {
        return buildLatesSchemaSelectList();
      }
    }

    public ProjectionPlan buildEarlySchema() {
      ResultSetLoader tableLoader = makeStaticTableLoader();
      StaticColumnLoader staticLoader = null;
      RowBatchMerger batchMerger = null;
      VectorContainer output;
      List<StaticColumn> staticCols = projection.staticCols();
      if (staticCols.isEmpty()) {
        output = tableLoader.outputContainer();
      } else {
        staticLoader = new StaticColumnLoader(allocator, staticCols);
        batchMerger = makeBatchMerger(tableLoader, staticLoader);
        output = batchMerger.getOutput();
      }
      return new EarlySchemaProjectPlan(tableLoader, staticLoader, batchMerger, output);
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
      ResultSetLoaderImpl loader =  new ResultSetLoaderImpl(allocator, options.build());
      TupleSchema schema = loader.writer().schema();
      for (TableColumn tableCol : projection.tableCols()) {
        schema.addColumn(tableCol.schema());
      }
      return loader;
    }

    protected RowBatchMerger makeBatchMerger(ResultSetLoader tableLoader, StaticColumnLoader staticLoader) {
      RowBatchMerger.Builder builder = new RowBatchMerger.Builder();
      VectorContainer tableContainer = tableLoader.outputContainer();
      List<ProjectedColumn> projections = projection.projectedCols();
      for (int i = 0; i < projections.size(); i++) {
        builder.addProjection(tableContainer, i, projections.get(i).index());
      }

      VectorContainer staticContainer = staticLoader.output();
      List<StaticColumn> staticCols = projection.staticCols();
      for (int i = 0; i < staticCols.size(); i++) {
        builder.addProjection(staticContainer, i, staticCols.get(i).index());
      }

      return builder.build(allocator);
    }

    public ProjectionPlan buildLateSchemaSelectAll() {
      return null;
    }

    public ProjectionPlan buildLatesSchemaSelectList() {
      return null;
    }
  }

  /**
   * Builds the result set loader and output result set for the case in which
   * the schema is known at the start and does not change for the life of the
   * read.
   */

  public static class EarlySchemaProjectPlan extends ProjectionPlan {

    public EarlySchemaProjectPlan(
        ResultSetLoader tableLoader, StaticColumnLoader staticLoader,
        RowBatchMerger batchMerger, VectorContainer output) {
      super(tableLoader, staticLoader, batchMerger, output);
    }

    @Override
    public void build() {
      doMerge();
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

  public ProjectionPlan(ResultSetLoader tableLoader, StaticColumnLoader staticLoader, RowBatchMerger batchMerger, VectorContainer output) {
    this.tableLoader = tableLoader;
    staticColumnLoader = staticLoader;
    this.batchMerger = batchMerger;
    this.output = output;
  }

  public abstract void build();

  protected void doMerge() {
    tableLoader.harvest();
    if (staticColumnLoader == null) {
      return;
    }
    int rowCount = output.getRecordCount();
    staticColumnLoader.load(rowCount);
    batchMerger.project(rowCount);
  }

  public VectorContainer output() {
    return output;
  }

  public ResultSetLoader tableLoader() { return tableLoader; }
}
