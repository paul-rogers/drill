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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.metadata.AbstractColumnMetadata;
import org.apache.drill.exec.record.metadata.MapColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.record.metadata.VariantColumnMetadata;
import org.apache.drill.exec.record.metadata.VariantSchema;
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
 *          .addMap("e") // or .addMapArray("e")
 *            .add("f", MinorType.VARCHAR)
 *            .buildMap()
 *          .add("g", MinorType.INT)
 *          .buildMap()
 *        .addUnion("h") // or .addList("h")
 *          .addType(MinorType.INT)
 *          .addMap()
 *            .add("h1", MinorType.INT)
 *            .buildNested()
 *          .addList()
 *            .addType(MinorType.BIGINT)
 *            .buildNested()
 *          .build()
 *        .addArray("i", MinorType.BIGINT)
 *        .build();
 * </code</pre>
 */

public class SchemaBuilder {

  /**
   * Build a column schema (AKA "materialized field") based on name and a
   * variety of schema options. Every column needs a name and (minor) type,
   * some may need a mode other than required, may need a width, may
   * need scale and precision, and so on.
   */

  public static class ColumnBuilder {
    private final String name;
    private final MajorType.Builder typeBuilder;

    public ColumnBuilder(String name, MinorType type) {
      this.name = name;
      typeBuilder = MajorType.newBuilder()
          .setMinorType(type)
          .setMode(DataMode.REQUIRED);
    }

    public ColumnBuilder setMode(DataMode mode) {
      typeBuilder.setMode(mode);
      return this;
    }

    public ColumnBuilder setWidth(int width) {
      return setPrecision(width);
    }

    public ColumnBuilder setPrecision(int precision) {
      typeBuilder.setPrecision(precision);
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
    private final UnionBuilder union;
    private final String memberName;
    private final DataMode mode;

    public MapBuilder(SchemaBuilder parent, String memberName, DataMode mode) {
      this.parent = parent;
      union = null;
      this.memberName = memberName;
      // Optional maps not supported in Drill
      assert mode != DataMode.OPTIONAL;
      this.mode = mode;
    }

    public MapBuilder(UnionBuilder unionBuilder) {
      parent = null;
      union = unionBuilder;
      memberName = MinorType.MAP.toString();
      mode = DataMode.OPTIONAL;
    }

    @Override
    public BatchSchema build() {
      throw new IllegalStateException("Cannot build for a nested schema");
    }

    private MapColumnMetadata buildCol() {
      return new MapColumnMetadata(memberName, mode, schema);
    }

    @Override
    public SchemaBuilder buildMap() {
      if (parent == null) {
        throw new IllegalStateException("Call buildNested() inside of a union");
      }
      parent.finish(buildCol());
      return parent;
    }

    @Override
    public UnionBuilder buildNested() {
      // TODO: Use the map schema directly rather than
      // rebuilding it as is done here.

      if (union == null) {
        throw new IllegalStateException("Call buildMap() inside of a tuple");
      }
      union.finishMap(buildCol());
      return union;
    }

    @Override
    public SchemaBuilder withSVMode(SelectionVectorMode svMode) {
      throw new IllegalStateException("Cannot set SVMode for a nested schema");
    }
  }

  public static class UnionBuilder {
    private final SchemaBuilder parentSchema;
    private final UnionBuilder parentUnion;
    private final String name;
    private final DataMode mode;
    private final VariantSchema union;

    public UnionBuilder(SchemaBuilder schemaBuilder, String name,
        DataMode mode) {
      this.parentSchema = schemaBuilder;
      parentUnion = null;
      this.name = name;
      this.mode = mode;
      union = new VariantSchema();
    }

    public UnionBuilder(UnionBuilder unionBuilder, DataMode mode) {
      parentSchema = null;
      parentUnion = unionBuilder;
      this.name = (mode == DataMode.REPEATED)
          ? MinorType.LIST.name() : MinorType.UNION.name();
      this.mode = mode;
      union = new VariantSchema();
    }

    private void checkType(MinorType type) {
      if (union.hasType(type)) {
        throw new IllegalArgumentException("Duplicate type: " + type);
      }
    }

    public UnionBuilder addType(MinorType type) {
      checkType(type);
      union.addType(type);
      return this;
    }

    public MapBuilder addMap() {
      checkType(MinorType.MAP);
      return new MapBuilder(this);
    }

    protected void finishMap(MapColumnMetadata mapCol) {
      union.addMap(mapCol);
    }

    protected void finishList(VariantColumnMetadata listCol) {
      union.addList(listCol);
    }

    public UnionBuilder addList() {
      checkType(MinorType.LIST);
      return new UnionBuilder(this, DataMode.REPEATED);
    }

    private VariantColumnMetadata buildCol() {
      MinorType dataType =
          mode == DataMode.REPEATED
          ? MinorType.LIST : MinorType.UNION;
      return new VariantColumnMetadata(name, dataType, union);
    }

    public SchemaBuilder build() {
      if (parentSchema == null) {
        throw new IllegalStateException("Call buildNested() instead");
      }
      parentSchema.finish(buildCol());
      return parentSchema;
    }

    public UnionBuilder buildNested() {
      if (parentUnion == null) {
        throw new IllegalStateException("Call build() instead");
      }
      parentUnion.finishList(buildCol());
      return parentUnion;
    }
  }

  /**
   * Builder for a repeated list. Drill's metadata represents a repeated
   * list as a chain of materialized fields and that is the pattern used
   * here. It would certainly be cleaner to have a single field, with the
   * number of dimensions as a property, but that is not how Drill evolved.
   */

  public static class RepeatedListBuilder {

    private final String name;
    private final SchemaBuilder parentTuple;
    private final RepeatedListBuilder parentList;
    private MaterializedField child;

    public RepeatedListBuilder(SchemaBuilder parent, String name) {
      this.name = name;
      parentTuple = parent;
      parentList = null;
    }

    public RepeatedListBuilder(RepeatedListBuilder parent) {
      this.name = parent.name;
      parentList = parent;
      parentTuple = null;
    }

    public RepeatedListBuilder addDimension() {
      return new RepeatedListBuilder(this);
    }

    public RepeatedListBuilder addArray(MinorType type) {
      // Existing code uses the repeated list name as the name of
      // the vector within the list.

      child = columnSchema(name, type, DataMode.REPEATED);
      return this;
    }

    private MaterializedField buildField() {
      MaterializedField field = columnSchema(name, MinorType.LIST, DataMode.REPEATED);
      if (child != null) {
        field.addChild(child);
      }
      return field;
    }

    public RepeatedListBuilder endDimension() {
      parentList.child = buildField();
      return parentList;
    }

    public SchemaBuilder build() {
      parentTuple.add(buildField());
      return parentTuple;
    }
  }

  protected TupleSchema schema = new TupleSchema();
  private SelectionVectorMode svMode = SelectionVectorMode.NONE;

  public SchemaBuilder() { }

  /**
   * Create a new schema starting with the base schema. Allows appending
   * additional columns to an additional schema.
   */

  public SchemaBuilder(BatchSchema baseSchema) {
    for (MaterializedField field : baseSchema) {
      add(field);
    }
  }

  public SchemaBuilder add(String name, MajorType type) {
    return add(MaterializedField.create(name, type));
  }

  public SchemaBuilder add(MaterializedField col) {
    schema.add(col);
    return this;
  }

  /**
   * Create a column schema using the "basic three" properties of name, type and
   * cardinality (AKA "data mode.") Use the {@link ColumnBuilder} for to set
   * other schema attributes. Name is relative to the enclosing map or tuple;
   * it is not the fully qualified path name.
   */

  public static MaterializedField columnSchema(String name, MinorType type, DataMode mode) {
    return MaterializedField.create(name,
        MajorType.newBuilder()
          .setMinorType(type)
          .setMode(mode)
          .build());
  }

  public SchemaBuilder add(String name, MinorType type, DataMode mode) {
    return add(columnSchema(name, type, mode));
  }

  public SchemaBuilder add(String name, MinorType type) {
    return add(name, type, DataMode.REQUIRED);
  }

  public SchemaBuilder add(String name, MinorType type, int width) {
    MaterializedField field = new SchemaBuilder.ColumnBuilder(name, type)
        .setMode(DataMode.REQUIRED)
        .setWidth(width)
        .build();
    return add(field);
  }

  public SchemaBuilder addNullable(String name, MinorType type) {
    return add(name, type, DataMode.OPTIONAL);
  }

  public SchemaBuilder addNullable(String name, MinorType type, int width) {
    MaterializedField field = new SchemaBuilder.ColumnBuilder(name, type)
        .setMode(DataMode.OPTIONAL)
        .setWidth(width)
        .build();
    return add(field);
  }

  public SchemaBuilder addArray(String name, MinorType type) {
    return add(name, type, DataMode.REPEATED);
  }

//  public SchemaBuilder addArray(String name, MinorType type, int dims) {
//    return add(name, type, DataMode.REPEATED);
//  }

  /**
   * Add a map column. The returned schema builder is for the nested
   * map. Building that map, using {@link MapBuilder#buildMap()},
   * will return the original schema builder.
   *
   * @param pathName the name of the map column
   * @return a builder for the map
   */

  public MapBuilder addMap(String name) {
    return new MapBuilder(this, name, DataMode.REQUIRED);
  }

  public MapBuilder addMapArray(String name) {
    return new MapBuilder(this, name, DataMode.REPEATED);
  }

  public UnionBuilder addUnion(String name) {
    return new UnionBuilder(this, name, DataMode.OPTIONAL);
  }

  public UnionBuilder addList(String name) {
    return new UnionBuilder(this, name, DataMode.OPTIONAL);
  }

  public RepeatedListBuilder addRepeatedList(String name) {
    return new RepeatedListBuilder(this, name);
  }

  public SchemaBuilder withSVMode(SelectionVectorMode svMode) {
    this.svMode = svMode;
    return this;
  }

  public BatchSchema build() {
    return schema.toBatchSchema(svMode);
  }

  void finish(AbstractColumnMetadata col) {
    schema.add(col);
  }

  public SchemaBuilder buildMap() {
    throw new IllegalStateException("Cannot build map for a top-level schema");
  }

  public UnionBuilder buildNested() {
    throw new IllegalStateException("Not inside a union");
  }

  public TupleMetadata buildSchema() {
    return schema;
  }
}
