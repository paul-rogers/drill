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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.physical.rowSet.impl.ColumnState.MapArrayColumnState;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.MapColumnState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.RowState;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.ModelVisitor;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;

/**
 * Visitors used to touch each column in the vector hierarchy. For simplicity,
 * we just walk the model hierarchy using the associated visitor class. In
 * theory, we could flatten columns into a list for purposes of these actions if
 * doing so was helpful for performance. In that case, we would need to append
 * each new column to the flat list.
 */

public class LoaderVisitors {

  /**
   * Given a column model build from an up-front schema, attach the loader state
   * to each node in the vector model tree.
   */

  public static class BuildStateVisitor
      extends ModelVisitor<Void, Void> {

    private final ResultSetLoaderImpl rsLoader;

    public BuildStateVisitor(ResultSetLoaderImpl rsLoader) {
      this.rsLoader = rsLoader;
    }

    public void apply(SingleRowSetModel rowModel) {
      rowModel.visit(this, null);
    }

    @Override
    protected Void visitRow(SingleRowSetModel row, Void arg) {
      row.bindCoordinator(new RowState(rsLoader, row));
      row.visitChildren(this,  arg);
      return null;
    }

    @Override
    protected Void visitPrimitiveColumn(PrimitiveColumnModel column, Void arg) {
      column.bindCoordinator(PrimitiveColumnState.newSimplePrimitive(rsLoader, column));
      return null;
    }

    @Override
    protected Void visitPrimitiveArrayColumn(PrimitiveColumnModel column, Void arg) {
      column.bindCoordinator(PrimitiveColumnState.newPrimitiveArray(rsLoader, column));
      return null;
    }

    @Override
    protected Void visitMapColumn(MapColumnModel column, Void arg) {
      column.bindCoordinator(new MapColumnState(rsLoader, column));
      column.mapModelImpl().bindCoordinator(new MapState(rsLoader, column));
      column.mapModelImpl().visitChildren(this, arg);
      return null;
    }

    @Override
    protected Void visitMapArrayColumn(MapColumnModel column, Void arg) {
      column.bindCoordinator(new MapArrayColumnState(rsLoader, column));
      column.mapModelImpl().bindCoordinator(new MapState(rsLoader, column));
      column.mapModelImpl().visitChildren(this, arg);
      return null;
    }
  }

  /**
   * Start a new batch by shifting the overflow buffers back into the main
   * write vectors and updating the writers.
   */

  public static class UpdateCardinalityVisitor extends ModelVisitor<Void, Integer> {

    public void apply(SingleRowSetModel rowModel, int rowCount) {
      rowModel.visit(this, rowCount);
    }

    @Override
    protected Void visitMapColumn(MapColumnModel column, Integer cardinality) {
      visitMap(column, cardinality);
      return column.mapModelImpl().visitChildren(this, cardinality);
    }

    @Override
    protected Void visitMapArrayColumn(MapColumnModel column, Integer cardinality) {
      visitMap(column, cardinality);
      int childCardinality = cardinality * column.schema().expectedElementCount();
      return column.mapModelImpl().visitChildren(this, childCardinality);
    }

    private void visitMap(MapColumnModel column, int cardinality) {
      ColumnState state = column.coordinator();
      state.setCardinality(cardinality);
      MapState mapState = column.mapModelImpl().coordinator();
      mapState.setCardinality(cardinality);
    }

    @Override
    protected Void visitColumn(AbstractSingleColumnModel column, Integer cardinality) {
      PrimitiveColumnState state = column.coordinator();
      state.setCardinality(cardinality);
      return null;
    }
  }

  /**
   * A column within the row batch overflowed. Prepare to absorb the rest of the
   * in-flight row by rolling values over to a new vector, saving the complete
   * vector for later. This column could have a value for the overflow row, or
   * for some previous row, depending on exactly when and where the overflow
   * occurs.
   *
   * @param overflowIndex
   *          the index of the row that caused the overflow, the values of which
   *          should be copied to a new "look-ahead" vector
   */

  public static class RollOverVisitor extends ModelVisitor<Void, Integer> {

    public void apply(SingleRowSetModel rowModel, int overflowIndex) {
      rowModel.visit(this, overflowIndex);
    }

    @Override
    protected Void visitMapColumn(MapColumnModel column,
        Integer overflowIndex) {
      return column.mapModelImpl().visitChildren(this, overflowIndex);
    }

    @Override
    protected Void visitMapArrayColumn(MapColumnModel column,
        Integer overflowIndex) {
      MapArrayColumnState colState = column.coordinator();
      colState.rollOver(overflowIndex);
      int arrayStartOffset = colState.offsetAt(overflowIndex);
      return column.mapModelImpl().visitChildren(this, arrayStartOffset);
    }

    /**
     * Roll over a simple scalar or array of scalars.
     *
     * @param column the scalar column to roll-over
     * @param context rollover point and cardinality for the new vector
     * @return null
     */

    @Override
    protected Void visitColumn(AbstractSingleColumnModel column, Integer overflowIndex) {
      PrimitiveColumnState state = column.coordinator();
      state.rollOver(overflowIndex);
      return null;
    }
  }

  /**
   * Writing of a row batch is complete, and an overflow occurred. Prepare the
   * vector for harvesting to send downstream. Set aside the look-ahead vector
   * and put the full vector buffer back into the active vector.
   */

  public static class HarvestOverflowVisitor extends ModelVisitor<Void, Void> {

    public void apply(SingleRowSetModel rowModel) {
      rowModel.visit(this, null);
    }

    @Override
    protected Void visitMapColumn(MapColumnModel column, Void arg) {
      visitMap(column);
      return null;
    }

    @Override
    protected Void visitMapArrayColumn(MapColumnModel column, Void arg) {
      visitMap(column);
      return null;
    }

    private void visitMap(MapColumnModel column) {
      column.mapModelImpl().visitChildren(this, null);
    }

    @Override
    protected Void visitColumn(AbstractSingleColumnModel column, Void arg) {
      PrimitiveColumnState state = column.coordinator();
      state.harvestWithLookAhead();
      return null;
    }
  }

  /**
   * Start a new batch by shifting the overflow buffers back into the main
   * write vectors and updating the writers.
   */

  public static class StartBatchVisitor extends ModelVisitor<Void, Void> {

    public void apply(SingleRowSetModel rowModel) {
      rowModel.visit(this, null);
    }

    @Override
    protected Void visitMapColumn(MapColumnModel column, Void arg) {
      visitMap(column);
      return null;
    }

    @Override
    protected Void visitMapArrayColumn(MapColumnModel column, Void arg) {
      visitMap(column);
      return null;
    }

    private void visitMap(MapColumnModel column) {
      ColumnState state = column.coordinator();
      state.startBatch();
      column.mapModelImpl().visitChildren(this, null);
    }

    @Override
    protected Void visitColumn(AbstractSingleColumnModel column, Void arg) {
      PrimitiveColumnState state = column.coordinator();
      state.startBatch();
      return null;
    }
  }

  /**
   * Clean up state (such as backup vectors) associated with the state
   * for each vector.
   */

  public static class ResetVisitor extends ModelVisitor<Void, Void> {

    public void apply(SingleRowSetModel rowModel) {
      rowModel.visit(this, null);
    }

    @Override
    protected Void visitColumn(AbstractSingleColumnModel column, Void arg) {
      PrimitiveColumnState state = column.coordinator();
      state.reset();
      return null;
    }
  }
}
