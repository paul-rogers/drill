package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.NullColumnLoader.NullColumnSpec;

/**
 * Represents a selected column which does not match a
 * table column, and so has a null value.
 */

public class NullColumn extends ResolvedColumn implements NullColumnSpec {

  public static final int ID = 5;

  public NullColumn(String name, MajorType type, SchemaPath source) {
    super(name, type, source);
  }

  @Override
  public int nodeType() { return ID; }

  @Override
  public ColumnProjection unresolve() {
    return new ContinuedColumn(schema(), source());
  }
}
