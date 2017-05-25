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
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.SelectColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.StaticColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.TableColumn;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;

public abstract class ResultSetBuilder {

  /**
   * Builds the result set loader and output result set for the case in which
   * the schema is known at the start and does not change for the life of the
   * read.
   */

  public static class StaticSchemaResultSetBuilder extends ResultSetBuilder {

    public StaticSchemaResultSetBuilder(ProjectionPlanner projection,
        ResultSetLoaderImpl.OptionBuilder options, VectorContainer output) {
      super(projection, makeTableLoader(options, projection, output.getAllocator()), output);
      buildTableSchema();
      buildVectorMap();
    }

    private static ResultSetLoader makeTableLoader(ResultSetLoaderImpl.OptionBuilder options,
        ProjectionPlanner projection, BufferAllocator allocator) {
      if (options == null) {
        options = new ResultSetLoaderImpl.OptionBuilder();
      }
      List<String> selection = new ArrayList<>();
      for (SelectColumn selCol : projection.queryCols()) {
        selection.add(selCol.name());
      }
      options.setSelection(selection);
      return new ResultSetLoaderImpl(allocator, options.build());
    }

    private void buildTableSchema() {
      TupleSchema tableSchema = tableLoader.writer().schema();
      for (TableColumn projCol : projection.tableCols()) {
        tableSchema.addColumn(projCol.schema());
      }
      tableSchemaVersion = tableLoader.schemaVersion();
    }


    @Override
    protected void handleTableSchemaChange() {
      throw new IllegalStateException("Table schema cannot change when using " +
          getClass().getSimpleName());
    }

  }

  public static abstract class EvolvingSchemaResultSetBuilder extends ResultSetBuilder {

    int outputSchemaVersion;
    private int lastSyncPosition;

    public EvolvingSchemaResultSetBuilder(ProjectionPlanner projection,
        ResultSetLoader tableLoader, VectorContainer output) {
      super(projection, tableLoader, output);
      throw new UnsupportedOperationException("Not yet");
    }

    private void evolveOuputSchema() {

      // We care about the table schema only if this is a
      // SELECT * query.

      if (! projection.isSelectAll()) {
        return;
      }

      //

      // The table loader has new columns.

      TupleSchema tableSchema = tableLoader.writer().schema();
      int total = tableSchema.columnCount();
      for (; lastSyncPosition < total; lastSyncPosition++) {
        if (tableSchema.selected(lastSyncPosition)) {
          output.addOrGet(tableSchema.column(lastSyncPosition));
        }
      }
    }

  }

  public static class SelectAllEvolvingSchemaResultSetBuilder extends EvolvingSchemaResultSetBuilder {

    public SelectAllEvolvingSchemaResultSetBuilder(ProjectionPlanner projection,
        ResultSetLoader tableLoader, VectorContainer output) {
      super(projection, tableLoader, output);
      // TODO Auto-generated constructor stub
    }

    @Override
    protected void handleTableSchemaChange() {
      // TODO Auto-generated method stub

    }

  }

  public static class SelectedEvolvingSchemaResultSetBuilder extends EvolvingSchemaResultSetBuilder {

    public SelectedEvolvingSchemaResultSetBuilder(ProjectionPlanner projection,
        ResultSetLoader tableLoader, VectorContainer output) {
      super(projection, tableLoader, output);
      // TODO Auto-generated constructor stub
    }

    @Override
    protected void handleTableSchemaChange() {
      // TODO Auto-generated method stub

    }

  }

  protected ProjectionPlanner projection;
  protected ResultSetLoader tableLoader;
  protected ResultSetLoader staticColumnLoader;
  protected VectorContainer output;
  protected ValueVector sourceVectors[];
  protected int rowCount;
  protected VectorContainer sourceContainer;
  protected VectorContainer staticColumnContainer;
  protected int tableSchemaVersion;


  public ResultSetBuilder(ProjectionPlanner projection, ResultSetLoader tableLoader, VectorContainer output) {
    this.projection = projection;
    this.tableLoader = tableLoader;
    this.output = output;
  }

  public void build() {
    rowCount = tableLoader.rowCount();
    sourceContainer = tableLoader.harvest();
    if (tableSchemaVersion < tableLoader.schemaVersion()) {
      handleTableSchemaChange();
    }
    populateStaticColumns();

    output.zeroVectors();
    mergeResults();
    sourceContainer.zeroVectors();
    staticColumnContainer.zeroVectors();
  }

  protected abstract void handleTableSchemaChange();

  protected void buildVectorMap() {
    int colCount = projection.outputCols().size();
    sourceVectors = new ValueVector[colCount];
    for (ProjectedColumn projCol : projection.projectedCols()) {
      sourceVectors[projCol.index()] = sourceContainer.getValueVector(projCol.source().index()).getValueVector();
    }
    if (staticColumnLoader == null) {
      return;
    }
    int i = 0;
    for (StaticColumn staticCol : projection.staticCols()) {
       sourceVectors[staticCol.index()] = staticColumnContainer.getValueVector(i++).getValueVector();
    }
  }

//  private void harvestTableColumns() {
//    if (projection.tableCols().isEmpty()) {
//      return;
//    }
//  }
//
  private void populateStaticColumns() {
    if (projection.nullCols().isEmpty() && projection.implicitCols().isEmpty() && projection.partitionCols().isEmpty()) {
      return;
    }
    if (staticColumnLoader == null) {
      return;
    }

    List<StaticColumn> staticCols = projection.staticCols();
    String values[] = new String[staticCols.size()];
    for (int i = 0; i < staticCols.size(); i++) {
      values[i] = staticCols.get(i).value();
    }
    staticColumnLoader.startBatch();
    TupleLoader writer = staticColumnLoader.writer();
    for (int i = 0; i < rowCount; i++) {
      staticColumnLoader.startRow();
      for (int j = 0; j < values.length; j++) {
        writer.column(j).setString(values[j]);
      }
      staticColumnLoader.saveRow();
    }
    staticColumnContainer = staticColumnLoader.harvest();
  }

  private void mergeResults() {
    for (int i = 0; i < sourceVectors.length; i++) {
      output.getValueVector(i).getValueVector().exchange(sourceVectors[i]);
    }
  }

  public ResultSetLoader tableLoader() { return tableLoader; }
}
