package org.apache.drill.exec.physical.rowSet.model.simple;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.*;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.accessor.writer.ObjectArrayWriter;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

public abstract class WriterBuilderVisitor extends ModelVisitor<Void, WriterBuilderVisitor.Context> {

  public static class Context {
    protected MapColumnModel mapColumn;
    protected AbstractObjectWriter mapWriter;
    protected List<AbstractObjectWriter> childWriters;
  }

  @Override
  protected Void visitMap(MapModel map, Context context) {
    context.mapWriter = MapWriter.build(context.mapColumn.schema(), (MapVector) map.vector(), buildTuple(map));
    return null;
  }

  @Override
  protected Void visitMapArray(MapModel map, Context context) {
    context.mapWriter = MapWriter.build(context.mapColumn.schema(), (RepeatedMapVector) map.vector(), buildTuple(map));
    return null;
  }

  protected List<AbstractObjectWriter> buildTuple(SimpleTupleModelImpl tuple) {
    Context context = new Context();
    context.childWriters = new ArrayList<>();
    tuple.visitChildren(this, context);
    return context.childWriters;
  }

  @Override
  protected Void visitPrimitiveColumn(PrimitiveColumnModel column, Context context) {
    return visitScalarColumn(column, context);
  }

  @Override
  protected Void visitPrimitiveArrayColumn(PrimitiveColumnModel column, Context context) {
    return visitScalarColumn(column, context);
  }

  private Void visitScalarColumn(PrimitiveColumnModel column, Context context) {
    context.childWriters.add(ColumnAccessorFactory.buildColumnWriter(column.vector()));
    return null;
  }

  @Override
  protected Void visitMapColumn(MapColumnModel column, Context context) {
    context.childWriters.add(buildMap(column));
    return null;
  }

  @Override
  protected Void visitMapArrayColumn(MapColumnModel column, Context context) {
    context.childWriters.add(ObjectArrayWriter.build((RepeatedMapVector) column.vector(), buildMap(column)));
    return null;
  }

  protected AbstractObjectWriter buildMap(MapColumnModel column) {
    Context context = new Context();
    context.mapColumn = column;
    column.mapModelImpl().visit(this, context);
    return context.mapWriter;
  }
}
