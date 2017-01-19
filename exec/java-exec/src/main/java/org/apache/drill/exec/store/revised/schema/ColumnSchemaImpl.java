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
package org.apache.drill.exec.store.revised.schema;

import java.util.LinkedHashSet;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.revised.Sketch;
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
