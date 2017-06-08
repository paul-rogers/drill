/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.test.rowSet;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.MaterializedSchema;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Builder of a row set schema expressed as a list of materialized
 * fields. Optimized for use when creating schemas by hand in tests.
 * <p>
 * Example usage to create the following schema: <br>
 * <tt>(c: INT, a: MAP(b: VARCHAR, d: INT, e: MAP(f: VARCHAR), g: INT), h: BIGINT)</tt>
 * <p>
 * Code:<pre><code>
 *     BatchSchema batchSchema = new SchemaBuilder()
 *        .add("c", MinorType.INT)
 *        .addMap("a")
 *          .addNullable("b", MinorType.VARCHAR)
 *          .add("d", MinorType.INT)
 *          .addMap("e")
 *            .add("f", MinorType.VARCHAR)
 *            .buildMap()
 *          .add("g", MinorType.INT)
 *          .buildMap()
 *        .addArray("h", MinorType.BIGINT)
 *        .build();
 * </code</pre>
 */

public class SchemaBuilder {

  public static class ColumnBuilder {
    private String name;
    private MajorType.Builder typeBuilder;

    public ColumnBuilder(String name, MinorType type) {
      this.name = name;
      typeBuilder = MajorType.newBuilder()
          .setMinorType(type);
    }

    public ColumnBuilder setMode(DataMode mode) {
      typeBuilder.setMode(mode);
      return this;
    }

    public ColumnBuilder setWidth(int width) {
//      typeBuilder.setWidth(width); // Legacy
      typeBuilder.setPrecision(width); // Since DRILL-5419
      return this;
    }

    public ColumnBuilder setScale(int scale, int precision) {
      typeBuilder.setScale(scale);
      typeBuilder.setPrecision(precision);
      return this;
    }

    public MaterializedField build() {
      return MaterializedField.create(name, typeBuilder.build());
    }
  }

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

  public SchemaBuilder(BatchSchema baseSchema) {
    for (MaterializedField field : baseSchema) {
      columns.add(field);
    }
  }

  public SchemaBuilder add(String pathName, MajorType type) {
    return add(MaterializedField.create(pathName, type));
  }

  public SchemaBuilder add(MaterializedField col) {
    columns.add(col);
    return this;
  }

  public static MaterializedField columnSchema(String pathName, MinorType type, DataMode mode) {
    return MaterializedField.create(pathName,
        MajorType.newBuilder()
          .setMinorType(type)
          .setMode(mode)
          .build());
  }

  public SchemaBuilder add(String pathName, MinorType type, DataMode mode) {
    return add(columnSchema(pathName, type, mode));
  }

  public SchemaBuilder add(String pathName, MinorType type) {
    return add(pathName, type, DataMode.REQUIRED);
  }

  public SchemaBuilder add(String pathName, MinorType type, int width) {
    MaterializedField field = new SchemaBuilder.ColumnBuilder(pathName, type)
        .setMode(DataMode.REQUIRED)
        .setWidth(width)
        .build();
    return add(field);
  }

  public SchemaBuilder addNullable(String pathName, MinorType type) {
    return add(pathName, type, DataMode.OPTIONAL);
  }

  public SchemaBuilder addNullable(String pathName, MinorType type, int width) {
    MaterializedField field = new SchemaBuilder.ColumnBuilder(pathName, type)
        .setMode(DataMode.OPTIONAL)
        .setWidth(width)
        .build();
    return add(field);
  }

  public SchemaBuilder addArray(String pathName, MinorType type) {
    return add(pathName, type, DataMode.REPEATED);
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

  public MaterializedSchema buildSchema() {
    MaterializedSchema schema = new MaterializedSchema();
    for (MaterializedField col : columns) {
      schema.add(col);
    }
    return schema;
  }
}
