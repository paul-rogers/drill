package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.record.MaterializedField;

public class ContinuedColumn implements ColumnProjection {

  public static final int ID = 7;

  private final MaterializedField schema;
  private final SchemaPath source;

  public ContinuedColumn(MaterializedField schema, SchemaPath source) {
    this.schema = schema;
    this.source = source;
  }

  @Override
  public String name() { return schema.getName(); }

  @Override
  public boolean resolved() { return false; }

  @Override
  public int nodeType() { return ID; }

  public MajorType type() { return schema.getType(); }

  @Override
  public SchemaPath source() { return source; }
}
