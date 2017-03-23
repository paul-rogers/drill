package org.apache.drill.test.rowSet;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Builder of a row set schema expressed as a list of materialized
 * fields.
 */

public class SchemaBuilder {

  /**
   * Internal structure for building a map. A map is just a schema,
   * but one that is part of a parent column.
   */

  public static class MapBuilder extends SchemaBuilder {
    private final SchemaBuilder parent;
    private final String memberName;

    public MapBuilder(SchemaBuilder parent, String memberName) {
      this.parent = parent;
      this.memberName = memberName;
    }

    @Override
    public BatchSchema build() {
      throw new IllegalStateException("Cannot build for a nested schema");
    }

    @Override
    public SchemaBuilder buildMap() {
      MaterializedField col = MaterializedField.create(memberName,
          MajorType.newBuilder()
            .setMinorType(MinorType.MAP)
            .setMode(DataMode.REQUIRED)
            .build());
      for (MaterializedField childCol : columns) {
        col.addChild(childCol);
      }
      parent.finishMap(col);
      return parent;
    }
  }

  protected List<MaterializedField> columns = new ArrayList<>( );

  public SchemaBuilder() { }

  public SchemaBuilder add(String pathName, MajorType type) {
    MaterializedField col = MaterializedField.create(pathName, type);
    columns.add(col);
    return this;
  }

  public SchemaBuilder add(String pathName, MinorType type, DataMode mode) {
    return add(pathName, MajorType.newBuilder()
        .setMinorType(type)
        .setMode(mode)
        .build());
  }

  public SchemaBuilder add(String pathName, MinorType type) {
    return add(pathName, type, DataMode.REQUIRED);
  }

  public SchemaBuilder addNullable(String pathName, MinorType type) {
    return add(pathName, type, DataMode.OPTIONAL);
  }

  /**
   * Add a map column. The returned schema builder is for the nested
   * map. Building that map, using {@link MapBuilder#buildMap()},
   * will return the original schema builder.
   *
   * @param pathName the name of the map column
   * @return a builder for the map
   */

  public MapBuilder addMap(String pathName) {
    return new MapBuilder(this, pathName);
  }

  public BatchSchema build() {
    return new BatchSchema(SelectionVectorMode.NONE, columns);
  }

  void finishMap(MaterializedField map) {
    columns.add(map);
  }

  public SchemaBuilder buildMap() {
    throw new IllegalStateException("Cannot build map for a top-level schema");
  }
}
