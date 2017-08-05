package org.apache.drill.exec.physical.rowSet.model.simple;

import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.*;
import org.apache.drill.exec.physical.rowSet.model.simple.SimpleTupleModelImpl.SimpleColumnModelImpl;

public class ModelVisitor<R, A> {

    protected R visitRow(RowSetModelImpl row, A arg) {
      return visitTuple(row, arg);
    }

    protected R visitMap(MapModel map, A arg) {
      return visitTuple(map, arg);
    }

    protected R visitMapArray(MapModel map, A arg) {
      return visitTuple(map, arg);
    }

    protected R visitTuple(SimpleTupleModelImpl tuple, A arg) {
      return tuple.visitChildren(this, arg);
    }

    protected R visitPrimitiveColumn(PrimitiveColumnModel column, A arg) {
      return visitColumn(column, arg);
    }

    protected R visitPrimitiveArrayColumn(PrimitiveColumnModel column, A arg) {
      return visitColumn(column, arg);
    }

    protected R visitMapColumn(MapColumnModel column, A arg) {
      visitColumn(column, arg);
      return column.mapModelImpl().visit(this, arg);
    }

    protected R visitMapArrayColumn(MapColumnModel column, A arg) {
      visitColumn(column, arg);
      return column.mapModelImpl().visit(this, arg);
    }

//    protected void visitListColumn(T column) {
//      visitColumn(column);
//    }

    protected R visitColumn(SimpleColumnModelImpl column, A arg) { return null; }
  }