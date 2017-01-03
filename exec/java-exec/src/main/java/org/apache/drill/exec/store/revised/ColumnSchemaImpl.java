package org.apache.drill.exec.store.revised;

import java.util.LinkedHashSet;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchema;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;

public class ColumnSchemaImpl implements ColumnSchema {

  private final int index;
  private final int globalIndex;
  private final String name;
  private final String path;
  private final MajorType type;
  private final RowSchema mapSchema;

  public ColumnSchemaImpl( int localIndex, int globalIndex, String prefix, String name, MajorType type, RowSchema mapSchema ) {
    this.index = localIndex;
    this.globalIndex = globalIndex;
    this.name = name;
    path = prefix + name;
    this.type = type;
    this.mapSchema = mapSchema;
  }

  @Override
  public int index() { return index; }

  @Override
  public int flatIndex() { return globalIndex; }

  @Override
  public String name() { return name; }

  @Override
  public String path() { return path; }

  @Override
  public MinorType type() { return type.getMinorType(); }

  @Override
  public DataMode cardinality() { return type.getMode(); }

  @Override
  public MajorType majorType() { return type; }

  @Override
  public RowSchema map() { return mapSchema; }

  @Override
  public MaterializedField materializedField() {
    MaterializedField field = MaterializedField.create(path, type);
    if (mapSchema != null) {
      for (int i = 0; i < mapSchema.size(); i++) {
        field.addChild(mapSchema.column(i).materializedField());
      }
    }
    return field;
  }
}
