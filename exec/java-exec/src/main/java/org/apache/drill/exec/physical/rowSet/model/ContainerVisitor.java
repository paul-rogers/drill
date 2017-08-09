package org.apache.drill.exec.physical.rowSet.model;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

public class ContainerVisitor<R, A> {

  public R apply(VectorContainer container, A arg) {
    return visitContainer(container, arg);
  }

  private R visitContainer(VectorContainer container, A arg) {
    return visitChildren(container, arg);
  }

  public R visitChildren(VectorContainer container, A arg) {
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      apply(vector, arg);
    }
    return null;
  }

  protected R apply(ValueVector vector, A arg) {
    MaterializedField schema = vector.getField();
    MajorType majorType = schema.getType();
    MinorType type = majorType.getMinorType();
    DataMode mode = majorType.getMode();
    switch (type) {
    case MAP:
      if (mode == DataMode.REPEATED) {
        return visitRepeatedMap((RepeatedMapVector) vector, arg);
      } else {
        return visitMap((AbstractMapVector) vector, arg);
      }
    case LIST:
      if (mode == DataMode.REPEATED) {
        return visitRepeatedList((RepeatedListVector) vector, arg);
      } else {
        return visitList((ListVector) vector, arg);
      }
    default:
      if (mode == DataMode.REPEATED) {
        return visitRepeatedPrimitive((BaseRepeatedValueVector) vector, arg);
      } else {
        return visitPrimitive(vector, arg);
      }
    }
  }

  protected R visitRepeatedMap(RepeatedMapVector vector, A arg) {
    visitChildren(vector, arg);
    return visitVector(vector, arg);
  }

  protected R visitMap(AbstractMapVector vector, A arg) {
    visitChildren(vector, arg);
    return visitVector(vector, arg);
  }

  private R visitChildren(AbstractMapVector vector, A arg) {
    for (int i = 0; i < vector.size(); i++) {
      apply(vector.getChildByOrdinal(i), arg);
    }
    return null;
  }

  protected R visitRepeatedList(RepeatedListVector vector, A arg) {
    return visitVector(vector, arg);
  }

  protected R visitList(ListVector vector, A arg) {
    return visitVector(vector, arg);
  }

  protected R visitRepeatedPrimitive(BaseRepeatedValueVector vector, A arg) {
    return visitVector(vector, arg);
  }

  protected R visitPrimitive(ValueVector vector, A arg) {
    return visitVector(vector, arg);
  }

  protected R visitVector(ValueVector vector, A arg) {
    return null;
  }

}
