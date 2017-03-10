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
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

/**
 * Schema for a row set expressed as a list of materialized columns.
 * Allows easy creation of multiple row sets from the same schema.
 * Each schema is immutable, which is fine for tests in which we
 * want known inputs and outputs.
 */

public class RowSetSchema {

  public static class SchemaBuilder {
    private List<MaterializedField> columns = new ArrayList<>( );

    public SchemaBuilder() {
    }

    public SchemaBuilder(RowSetSchema base) {
      columns.addAll(base.columns);
    }

    public SchemaBuilder add(String pathName, MajorType type) {
      MaterializedField col = MaterializedField.create(pathName, type);
      columns.add(col);
      return this;
    }

    public SchemaBuilder add(String pathName, MinorType type, DataMode mode) {
      return add(pathName, MajorType.newBuilder()
          .setMinorType(type)
          .setMode(mode)
          .build()
          );
    }

    public SchemaBuilder add(String pathName, MinorType type) {
      return add(pathName, type, DataMode.REQUIRED);
    }

    public SchemaBuilder addNullable(String pathName, MinorType type) {
      return add(pathName, type, DataMode.OPTIONAL);
    }

    public RowSetSchema build() {
      return new RowSetSchema(columns);
    }
  }

  private List<MaterializedField> columns = new ArrayList<>( );

  public RowSetSchema(List<MaterializedField> columns) {
    this.columns.addAll(columns);
  }

  public RowSetSchema(BatchSchema schema) {
    for (MaterializedField field : schema) {
      columns.add(field);
    }
  }

  public int count( ) { return columns.size(); }
  public MaterializedField get(int i) { return columns.get(i); }

  public static SchemaBuilder builder( ) {
    return new SchemaBuilder();
  }

  public SchemaBuilder revise() {
    return new SchemaBuilder(this);
  }

  public BatchSchema toBatchSchema(SelectionVectorMode selectionVector) {
    List<MaterializedField> fields = new ArrayList<>();
    fields.addAll(columns);
    return new BatchSchema(selectionVector, fields);
  }

}