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
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Schema for a row set expressed as a list of materialized columns.
 * Allows easy creation of multiple row sets from the same schema.
 * Each schema is immutable, which is fine for tests in which we
 * want known inputs and outputs.
 */

public class RowSetSchema {

  public static class SchemaBuilder {
    private List<ColumnSchema> columns = new ArrayList<>( );

    public SchemaBuilder() {
    }

    public SchemaBuilder(RowSetSchema base) {
      columns.addAll(base.columns);
    }

    public SchemaBuilder add(String pathName, MajorType type) {
      MaterializedField col = MaterializedField.create(pathName, type);
      columns.add(new ColumnSchema(col));
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

    public RowSetSchema build() {
      return new RowSetSchema(columns);
    }

    private void finishMap(String memberName, RowSetSchema mapSchema) {
      MaterializedField col = MaterializedField.create(memberName,
          MajorType.newBuilder()
            .setMinorType(MinorType.MAP)
            .setMode(DataMode.REQUIRED)
            .build());
      for (ColumnSchema childCol : mapSchema.columns) {
        col.addChild(childCol.getField());
      }
      columns.add(new ColumnSchema(col, mapSchema));
    }

    public SchemaBuilder buildMap() {
      throw new IllegalStateException("Cannot build map for a top-level schema");
    }
  }

  public static class MapBuilder extends SchemaBuilder {
    private final SchemaBuilder parent;
    private final String memberName;

    public MapBuilder(SchemaBuilder parent, String memberName) {
      this.parent = parent;
      this.memberName = memberName;
    }

    @Override
    public RowSetSchema build() {
      throw new IllegalStateException("Cannot build for a nested schema");
    }

    @Override
    public SchemaBuilder buildMap() {
      RowSetSchema mapSchema = super.build();
      parent.finishMap(memberName, mapSchema);
      return parent;
    }
  }

  public static class ColumnSchema {
    MaterializedField field;
    RowSetSchema members;

    public ColumnSchema(MaterializedField field) {
      this.field = field;
      if (field.getType().getMinorType() == MinorType.MAP) {
        members = new RowSetSchema(field.getChildren());
      }
    }

    public ColumnSchema(MaterializedField field, RowSetSchema mapSchema) {
      this.field = field;
      members = mapSchema;
    }

    public MaterializedField getField() { return field; }
    public RowSetSchema getMembers() { return members; }
  }

  private final List<ColumnSchema> columns;

  public RowSetSchema(Iterable<MaterializedField> columns) {
    this.columns = new ArrayList<>( );
    for (MaterializedField field : columns) {
      this.columns.add(new ColumnSchema(field));
    }
  }

  public RowSetSchema(List<ColumnSchema> columns) {
    this.columns = columns;
  }

  public int count() { return columns.size(); }
  public MaterializedField get(int i) { return columns.get(i).getField(); }
  public ColumnSchema getColumn(int i) { return columns.get(i); }

  public static SchemaBuilder builder() {
    return new SchemaBuilder();
  }

  public SchemaBuilder revise() {
    return new SchemaBuilder(this);
  }

  public BatchSchema toBatchSchema(SelectionVectorMode selectionVector) {
    List<MaterializedField> fields = new ArrayList<>();
    for (ColumnSchema col : columns) {
      fields.add(col.field);
    }
    return new BatchSchema(selectionVector, fields);
  }
}