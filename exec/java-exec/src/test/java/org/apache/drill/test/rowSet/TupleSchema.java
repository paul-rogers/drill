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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Schema for a tuple expressed as a list of materialized columns.
 * Allows easy creation of multiple row sets from the same schema.
 * Each schema is immutable, which is fine for tests in which we
 * want known inputs and outputs.
 * <p>
 * Drill supports two kinds of tuples represented by two subclasses:
 * {@link RowSetSchema} that represents the schema of a row set (AKA
 * record batch), and {@link MapSchema} that represents a Drill map
 * type (which is really a nested tuple.)
 */

public abstract class TupleSchema {

  /**
   * Metadata description of a column. Includes the full name,
   * the index within the tuple, the materialized field and, if
   * this is a map, the schema of the nested tuple.
   */

  public static class ColumnSchema {
    public final String fullName;
    public final int index;
    public final MaterializedField field;

    /**
     * Schema of the map. Includes only those fields directly within
     * the map; does not include fields from nested tuples.
     */

    public MapSchema mapSchema;

    public ColumnSchema(int index, String fullName, MaterializedField field) {
      this.index = index;
      this.fullName = fullName;
      this.field = field;
    }
  }

  /**
   * A row set schema holds all fields in the row, including those from
   * maps. If a row contains fields (a, b(c, d), e) then the field listed
   * here are (a, b, b.c, b.d, e).
   */

  public static class RowSetSchema extends TupleSchema {

    final BatchSchema batchSchema;

    public RowSetSchema(BatchSchema schema) {
      batchSchema = schema;
      for (MaterializedField field : schema) {
        ColumnSchema colSchema = addColumn("", field);
        if (field.getType().getMinorType() == MinorType.MAP) {
          colSchema.mapSchema = new MapSchema(this, field.getName() + ".", field.getChildren());
        }
      }
    }

    public BatchSchema getSchema() { return batchSchema; }

    public BatchSchema toBatchSchema(SelectionVectorMode selectionVector) {

      List<MaterializedField> fields = new ArrayList<>();
      for (MaterializedField field : batchSchema) {
        fields.add(field);
      }
      return new BatchSchema(selectionVector, fields);
    }
  }

  /**
   * Schema of a map field. Contains fields directly within the map.
   * Maps can be nested. In this case, the map column will contain
   * another map schema to describe the nested map.
   */

  public static class MapSchema extends TupleSchema {

    public MapSchema(RowSetSchema rowSchema, String prefix,
        Collection<MaterializedField> members) {
      for (MaterializedField field : members) {
        rowSchema.addColumn(prefix, field);
        ColumnSchema colSchema = addColumn("", field);
        if (field.getType().getMinorType() == MinorType.MAP) {
          colSchema.mapSchema = new MapSchema(rowSchema, prefix + field.getName() + ".", field.getChildren());
        }
      }
    }
  }

  private final Map<String,Integer> nameSpace = new HashMap<>();
  private final List<ColumnSchema> columns = new ArrayList<>( );

  public int count() { return columns.size(); }
  public MaterializedField get(int i) { return columns.get(i).field; }
  public ColumnSchema getColumn(int i) { return columns.get(i); }

  public int getIndex(String name) {
    Integer index = nameSpace.get(name);
    if (index == null) {
      return -1;
    }
    return index;
  }

  public MaterializedField get(String name) {
    Integer index = getIndex(name);
    if (index == -1) {
      return null;
    }
    return get(index);
  }

  public static SchemaBuilder builder() {
    return new SchemaBuilder();
  }

  protected ColumnSchema addColumn(String prefix, MaterializedField field) {
    String name = prefix + field.getName();
    ColumnSchema colSchema = new ColumnSchema(columns.size(), name, field);
    nameSpace.put(name, colSchema.index);
    columns.add(colSchema);
    return colSchema;
  }
}