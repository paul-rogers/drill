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

import org.apache.drill.exec.physical.rowSet.impl.ColumnState.MapColumnState;
import org.apache.drill.exec.physical.rowSet.impl.LoaderTupleCoordinator.MapCoordinator;
import org.apache.drill.exec.physical.rowSet.impl.LoaderTupleCoordinator.RowCoordinator;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.ModelVisitor;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;

/**
 * Visitors used to touch each column in the vector hierarchy. For simplicity,
 * we just walk the model hierarchy using the associated visitor class. In
 * theory, we could flatten columns into a list for purposes of these actions if
 * doing so was helpful for performance. In that case, we would need to append
 * each new column to the flat list.
 */

public class LoaderVisitors {

  public static class BuildStateVisitor
      extends ModelVisitor<Void, ResultSetLoaderImpl> {

    public void apply(SingleRowSetModel rowModel,
        ResultSetLoaderImpl rsLoader) {
      rowModel.visit(this, rsLoader);
    }

    @Override
    protected Void visitRow(SingleRowSetModel row,
        ResultSetLoaderImpl rsLoader) {
      row.bindCoordinator(new RowCoordinator(rsLoader, row));
      return visitTuple(row, rsLoader);
    }

    @Override
    protected Void visitMap(MapModel map, ResultSetLoaderImpl rsLoader) {
      map.bindCoordinator(new MapCoordinator(rsLoader, map));
      return visitTuple(map, rsLoader);
    }

    @Override
    protected Void visitPrimitiveColumn(PrimitiveColumnModel column,
        ResultSetLoaderImpl rsLoader) {
      column.bindCoordinator(new PrimitiveColumnState(rsLoader, column));
      return null;
    }

    @Override
    protected Void visitPrimitiveArrayColumn(PrimitiveColumnModel column,
        ResultSetLoaderImpl rsLoader) {
      return visitPrimitiveColumn(column, rsLoader);
    }

    @Override
    protected Void visitMapColumn(MapColumnModel column,
        ResultSetLoaderImpl rsLoader) {
      column.bindCoordinator(new MapColumnState(rsLoader));
      return null;
    }

    @Override
    protected Void visitMapArrayColumn(MapColumnModel column,
        ResultSetLoaderImpl rsLoader) {
      return visitMapColumn(column, rsLoader);
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
    protected Void visitColumn(AbstractSingleColumnModel column, Void arg) {
      PrimitiveColumnState state = column.coordinator();
      state.harvestWithOverflow();
      return null;
    }
  }

  /**
   * Start a new batch by shifting the overflow buffers back into the main
   * write vectors. The writers are updated as a separate step.
   */

  public static class StartOverflowBatchVisitor extends ModelVisitor<Void, Void> {

    public void apply(SingleRowSetModel rowModel) {
      rowModel.visit(this, null);
    }

    @Override
    protected Void visitColumn(AbstractSingleColumnModel column, Void arg) {
      PrimitiveColumnState state = column.coordinator();
      state.startOverflowBatch();
      return null;
    }
  }

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
