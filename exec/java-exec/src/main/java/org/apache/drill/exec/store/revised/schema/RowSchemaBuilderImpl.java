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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.revised.Sketch;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchema;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchemaBuilder;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;
import org.apache.drill.exec.store.revised.Sketch.RowSchemaBuilder;

public class RowSchemaBuilderImpl implements RowSchemaBuilder {

  public interface Listener {
    void onBuild( RowSchema schema );
  }

  private final RowSchemaBuilderImpl parent;
  private final boolean caseSensitive;
  private Listener listener;
  private List<ColumnSchemaBuilderImpl> columns = new ArrayList<>( );
  private Map<String, ColumnSchemaBuilderImpl> nameIndex = new HashMap<>( );
  private int globalIndex;

  public RowSchemaBuilderImpl( ) {
    this( false );
  }

  public RowSchemaBuilderImpl( boolean caseSensitive ) {
    this.caseSensitive = caseSensitive;
    parent = null;
  }

  public RowSchemaBuilderImpl( RowSchemaBuilderImpl parent ) {
    this.caseSensitive = parent.caseSensitive;
    this.parent = parent;
  }

  public RowSchemaBuilderImpl(RowSchema schema) {
    this.caseSensitive = schema.isCaseSensitive( );
    parent = null;
    assert false;
  }

  public void listener( Listener listener ) {
    this.listener = listener;
  }

  @Override
  public ColumnSchemaBuilder column( String name ) {
    String key = caseSensitive ? name : name.toLowerCase();
    if ( nameIndex.containsKey( key ) ) {
      throw new IllegalArgumentException( "Duplicate column: " + name );
    }
    return new ColumnSchemaBuilderImpl( this, name );
  }

  @Override
  public RowSchemaBuilder column( String name, MajorType type ) {
    if ( type.getMinorType() == MinorType.MAP ) {
      throw new IllegalArgumentException( "Map types require a schema" );
    }
    column( name )
        .majorType( type )
        .build();
    return this;
  }

  @Override
  public RowSchemaBuilder column( String name, MinorType type, DataMode cardinality ) {
    MajorType majorType = MajorType.newBuilder()
        .setMinorType( type )
        .setMode( cardinality )
        .build();
    return column( name, majorType );
  }

  @Override
  public RowSchema build( ) {
    if ( parent != null ) {
      throw new IllegalStateException( "Can only build the root schema" );
    }
    globalIndex = 0;
    return doBuild( "" );
  }

  protected int nextGlobalIndex( ) {
    if (parent == null) {
      return globalIndex++;
    }
    return parent.nextGlobalIndex();
  }

  protected RowSchema doBuild( String prefix ) {
    List<ColumnSchema> cols = new ArrayList<ColumnSchema>( );
    int localIndex = 0;
    for ( ColumnSchemaBuilderImpl colBuilder : columns ) {
      colBuilder.doBuild( localIndex++ , nextGlobalIndex( ), prefix );
    }
    return new RowSchemaImpl( caseSensitive, cols );
  }

  @Override
  public ColumnSchemaBuilder reviseColumn(String name) {
    return nameIndex.get(RowSchemaImpl.key(caseSensitive, name));
  }

  @Override
  public RowSchemaBuilder remove(String name) {
    ColumnSchemaBuilderImpl col = nameIndex.remove(RowSchemaImpl.key(caseSensitive, name));
    if (col != null) {
      columns.remove(col);
    }
    return this;
  }

  public void buildColumn(ColumnSchemaBuilderImpl columnBuilder) {
    String key = columnBuilder.name();
    key = ! caseSensitive ? key : key.toLowerCase();
    if ( nameIndex.containsKey( key ) ) {
      throw new IllegalArgumentException( "Duplicate column: " + columnBuilder.name() );
    }
    nameIndex.put(key, columnBuilder);
    columns.add(columnBuilder);
  }
}
