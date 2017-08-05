package org.apache.drill.exec.physical.rowSet.model.simple;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.*;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.ObjectArrayReader;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

public abstract class ReaderBuilderVisitor extends ModelVisitor<Void, ReaderBuilderVisitor.Context> {

  public static class Context {
    protected MapColumnModel mapColumn;
    protected AbstractObjectReader mapReader;
    protected List<AbstractObjectReader> childReaders;
  }

  protected AbstractObjectReader[] buildTuple(SimpleTupleModelImpl tuple) {
    Context context = new Context();
    context.childReaders = new ArrayList<>();
    tuple.visitChildren(this, context);
    AbstractObjectReader readers[] = new AbstractObjectReader[context.childReaders.size()];
    return context.childReaders.toArray(readers);
  }

  @Override
  protected Void visitPrimitiveColumn(PrimitiveColumnModel column, Context context ) {
    return visitScalarColumn(column, context);
  }

  @Override
  protected Void visitPrimitiveArrayColumn(PrimitiveColumnModel column, Context context ) {
    return visitScalarColumn(column, context);
  }

  private Void visitScalarColumn(PrimitiveColumnModel column, Context context) {
    context.childReaders.add(ColumnAccessorFactory.buildColumnReader(column.vector()));
    return null;
  }

  @Override
  protected Void visitMapColumn(MapColumnModel column, Context context ) {
    context.childReaders.add(buildMap(column));
    return null;
  }

  @Override
  protected Void visitMapArrayColumn(MapColumnModel column, Context context ) {
    context.childReaders.add(ObjectArrayReader.build((RepeatedMapVector) column.vector(), buildMap(column)));
    return null;
  }

  protected AbstractObjectReader buildMap(MapColumnModel column) {
    return MapReader.build(column.schema(), buildTuple(column.mapModelImpl()));
  }
}
