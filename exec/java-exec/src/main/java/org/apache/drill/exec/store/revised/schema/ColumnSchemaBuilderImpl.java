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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.revised.Sketch;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchema;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchemaBuilder;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;
import org.apache.drill.exec.store.revised.Sketch.RowSchemaBuilder;

public class ColumnSchemaBuilderImpl implements ColumnSchemaBuilder {

  private final RowSchemaBuilderImpl parent;
  private final String name;
  private MinorType type;
  private DataMode cardinality;
  private RowSchemaBuilderImpl mapSchema;

  public ColumnSchemaBuilderImpl( RowSchemaBuilderImpl parent, String name ) {
    this.parent = parent;
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public ColumnSchemaBuilder type(MinorType type) {
    this.type = type;
    return this;
  }

  @Override
  public MinorType getType() {
    return type;
  }

  @Override
  public ColumnSchemaBuilder cardinality(DataMode mode) {
    this.cardinality = mode;
    return this;
  }

  @Override
  public DataMode getCardinality() {
    return cardinality;
  }

  @Override
  public ColumnSchemaBuilder majorType(MajorType type) {
    this.type = type.getMinorType();
    cardinality = type.getMode();
    return this;
  }

  @Override
  public RowSchemaBuilder map( ) {
    type(MinorType.MAP);
    cardinality(DataMode.REQUIRED);
    mapSchema = new RowSchemaBuilderImpl( parent );
    build();
    return mapSchema;
  }

  @Override
  public RowSchemaBuilder getMapSchema() {
    return mapSchema;
  }

  protected ColumnSchema doBuild( int localIndex, int globalIndex, String prefix ) {
    MajorType majorType = MajorType.newBuilder()
        .setMinorType( type )
        .setMode( cardinality )
        .build();
    RowSchema mapSchemaDef = null;
    if ( mapSchema != null ) {
      mapSchemaDef = mapSchema.doBuild( prefix + name + ColumnSchema.NAME_SEPARATOR );
    }
    return new ColumnSchemaImpl( localIndex, globalIndex, prefix, name, majorType, mapSchemaDef );
  }

  @Override
  public void build() {
    parent.buildColumn(this);
  }
}
