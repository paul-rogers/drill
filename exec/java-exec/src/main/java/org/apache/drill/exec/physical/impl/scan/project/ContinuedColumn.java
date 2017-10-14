package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection.TypedColumn;
import org.apache.drill.exec.record.MaterializedField;

public class ContinuedColumn extends TypedColumn {

  public static final int ID = 7;

  public ContinuedColumn(MaterializedField schema) {
    super(schema, ID, false);
  }
}
