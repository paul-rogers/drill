package org.apache.drill.exec.physical.rowSet.model.simple;

import org.apache.drill.exec.physical.rowSet.model.simple.RowSetModelImpl.*;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.RepeatedFixedWidthVectorLike;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedVariableWidthVectorLike;

/**
 * Walk the row set tree to allocate new vectors according to a given
 * row count and the size information provided in column metadata.
 * <p>
 * @see {@link AllocationHelper} - the class which this one replaces
 * @see {@link VectorInitializer} - an earlier cut at implementation
 * based on data from the {@link RecordBatchSizer}
 */

// TODO: Does not yet handle lists; lists are a simple extension
// of the array-handling logic below.

public class AllocationVisitor extends ModelVisitor<Void, Integer> {

  public void allocate(RowSetModelImpl rowModel, int rowCount) {
    rowModel.visit(this, rowCount);
  }

  @SuppressWarnings("resource")
  @Override
  public Void visitPrimitiveColumn(PrimitiveColumnModel column, Integer valueCount) {
    ColumnMetadata schema = column.schema();
    ValueVector vector = column.vector();
    if (schema.isVariableWidth()) {
      final int byteCount = valueCount * schema.expectedWidth();
      ((VariableWidthVector) vector).allocateNew(byteCount, valueCount);
    } else {
      ((FixedWidthVector) vector).allocateNew(valueCount);
    }
    return null;
  }

  @SuppressWarnings("resource")
  @Override
  protected Void visitPrimitiveArrayColumn(PrimitiveColumnModel column, Integer valueCount) {
    ColumnMetadata schema = column.schema();
    ValueVector vector = column.vector();
    int expectedElementCount = schema.expectedElementCount();
    if (schema.isVariableWidth()) {
      final int byteCount = expectedElementCount * schema.expectedWidth();
      ((RepeatedVariableWidthVectorLike) vector).allocateNew(byteCount, valueCount, expectedElementCount);
    } else {
      ((RepeatedFixedWidthVectorLike) vector).allocateNew(valueCount, expectedElementCount);
    }
    return null;
  }

  @Override
  protected Void visitMapArrayColumn(MapColumnModel column, Integer valueCount) {
    int expectedValueCount = valueCount = column.schema().expectedElementCount();
    ((RepeatedMapVector) column.vector()).getOffsetVector().allocateNew(expectedValueCount);
    column.mapModelImpl().visit(this, expectedValueCount);
    return null;
  }
}
