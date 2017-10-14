package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection.TypedColumn;

/**
 * Represents a selected column which does not match a
 * table column, and so has a null value.
 */

public class NullColumn extends TypedColumn {


  public NullColumn(String name, MajorType type, ColumnProjection source) {
    super(name, type, ID);
  }

  public ColumnProjection unresolve() {
    return TypedColumn.newContinued(schema());
  }

  @Override
  public boolean resolved() { return true; }
}
