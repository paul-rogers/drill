package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.physical.rowSet.model.single.ModelVisitor;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.exec.physical.rowSet.impl.LoaderState2.PrimitiveColumnState;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.VectorContainerBuilder;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;

public class LoaderVisitors {

//void rollOver(int overflowIndex);
//void resetBatch();
//void harvest();
//void buildContainer(VectorContainerBuilder containerBuilder);
//void reset();
//void reset(int index);
//void close();

  public interface ColumnAction {
    void apply(AbstractSingleColumnModel state);
  }

  public static class RollOverAction implements ColumnAction {
    private int overflowIndex;

    public RollOverAction(int overflowIndex) {
      this.overflowIndex = overflowIndex;
    }

    @Override
    public void apply(AbstractSingleColumnModel state) {
      state.rollOver(overflowIndex);
    }
  }

  public static class PrimitiveColumnVisitor extends ModelVisitor<Void, ColumnAction> {

    @Override
    protected Void visitColumn(AbstractSingleColumnModel column, ColumnAction action) {
      action.apply(column);
      return null;
    }
  }

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

  public static class BuildContainerVisitor extends ModelVisitor<Void, VectorContainerBuilder> {

    public void apply(SingleRowSetModel rowModel, VectorContainerBuilder builder) {
      rowModel.visit(this, builder);
    }

  }
}
