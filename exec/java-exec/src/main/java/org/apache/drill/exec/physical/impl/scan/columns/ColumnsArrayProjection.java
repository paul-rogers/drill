package org.apache.drill.exec.physical.impl.scan.columns;

import org.apache.drill.common.types.TypeProtos.MajorType;

public class ColumnsArrayProjection {

  public static final String COLUMNS_COL = "columns";

  private final boolean columnsIndexes[];
  private final MajorType columnsArrayType;

  public ColumnsArrayProjection(ColumnsArrayParser builder) {
    columnsArrayType = builder.columnsArrayType;
    if (builder.columnsIndexes == null) {
      columnsIndexes = null;
    } else {
      columnsIndexes = new boolean[builder.maxIndex];
      for (int i = 0; i < builder.columnsIndexes.size(); i++) {
        columnsIndexes[builder.columnsIndexes.get(i)] = true;
      }
    }
  }

  public boolean[] columnsArrayIndexes() { return columnsIndexes; }

  public MajorType columnsArrayType() { return columnsArrayType; }
}