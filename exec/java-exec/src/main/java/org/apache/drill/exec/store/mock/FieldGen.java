package org.apache.drill.exec.store.mock;

import org.apache.drill.exec.vector.ValueVector;

public interface FieldGen {
  void setup(ColumnDef colDef);
  void setValue( ValueVector v, int index );
}
