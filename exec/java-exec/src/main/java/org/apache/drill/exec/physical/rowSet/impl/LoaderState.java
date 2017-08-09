package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.VectorContainerBuilder;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;
import org.apache.drill.exec.vector.accessor.writer.TupleConstructor;

public interface LoaderState {

  public interface TupleState extends LoaderState, TupleConstructor {

  }

  public interface RootState extends TupleState {

  }

  public interface MapState extends TupleState {

  }

  public interface ColumnState extends LoaderState {

  }

  public interface PrimitiveColumnState extends ColumnState {

  }

  public interface MapColumnState extends ColumnState {

  }

  public interface StateVisitor {
    void visitRoot(RootState root);
    void visitMap(MapState map);
    void visitProjectedPrimitive(PrimitiveColum)
  }

//  void rollOver(int overflowIndex);
//  void resetBatch();
//  void harvest();
//  void buildContainer(VectorContainerBuilder containerBuilder);
//  void reset();
//  void reset(int index);
//  void close();

}
