package org.apache.drill.exec.physical.rowSet.model;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.VectorContainer;

public interface TupleModel {

  public interface ColumnModel {
    ColumnMetadata schema();
    TupleModel mapModel();
  }

  public interface RowSetModel extends TupleModel {
    VectorContainer container();
  }

  TupleMetadata schema();
  int size();
  ColumnModel column(int index);
  ColumnModel column(String name);
  ColumnModel add(ColumnMetadata schema);
  ColumnModel add(MaterializedField field);
}
