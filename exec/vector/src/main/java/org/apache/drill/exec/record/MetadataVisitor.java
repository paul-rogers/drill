package org.apache.drill.exec.record;

import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;

public class MetadataVisitor<R, A> {

  protected R applyToRow(TupleMetadata row, A arg) {
    return visitRow(row, arg);
  }

  protected R applyToMap(TupleMetadata row, A arg) {
    return visitMap(row, arg);
  }

  protected R visitRow(TupleMetadata row, A arg) {
    return visitTuple(row, arg);
  }

  protected R visitMap(TupleMetadata map, A arg) {
    return visitTuple(map, arg);
  }

  protected R visitTuple(TupleMetadata tuple, A arg) {
    return visitChildren(tuple, arg);
  }

  protected R visitChildren(TupleMetadata tuple, A arg) {
    for (int i = 0; i < tuple.size(); i++) {
      apply(tuple.metadata(i), arg);
    }
    return null;
  }

  public R apply(ColumnMetadata col, A arg) {
    if (col.isMap()) {
      return visitMapColumn(col, arg);
    } else {
      return visitPrimitiveColumn(col, arg);
    }
  }

  protected R visitPrimitiveColumn(ColumnMetadata row, A arg) {
    return visitColumn(row, arg);
  }


  protected R visitMapColumn(ColumnMetadata row, A arg) {
    return visitColumn(row, arg);
  }

  private R visitColumn(ColumnMetadata row, A arg) {
    return null;
  }
}
