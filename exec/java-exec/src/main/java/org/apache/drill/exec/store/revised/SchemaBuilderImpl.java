package org.apache.drill.exec.store.revised;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchema;
import org.apache.drill.exec.store.revised.Sketch.ColumnSchemaBuilder;
import org.apache.drill.exec.store.revised.Sketch.RowSchema;
import org.apache.drill.exec.store.revised.Sketch.SchemaBuilder;

public class SchemaBuilderImpl implements SchemaBuilder {

  public interface Listener {
    void onBuild( RowSchema schema );
  }

  private final SchemaBuilderImpl parent;
  private final boolean caseSensitive;
  private Listener listener;
  private List<ColumnSchemaBuilderImpl> columns = new ArrayList<>( );
  private Map<String, ColumnSchemaBuilderImpl> nameIndex = new HashMap<>( );
  private int globalIndex;

  public SchemaBuilderImpl( ) {
    this( false );
  }

  public SchemaBuilderImpl( boolean caseSensitive ) {
    this.caseSensitive = caseSensitive;
    parent = null;
  }

  public SchemaBuilderImpl( SchemaBuilderImpl parent ) {
    this.caseSensitive = parent.caseSensitive;
    this.parent = parent;
  }

  public SchemaBuilderImpl(RowSchema schema) {
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
    ColumnSchemaBuilderImpl colBuilder = new ColumnSchemaBuilderImpl( this, name );
    nameIndex.put(key, colBuilder);
    columns.add( colBuilder );
    return colBuilder;
  }

  @Override
  public SchemaBuilder column( String name, MajorType type ) {
    if ( type.getMinorType() == MinorType.MAP ) {
      throw new IllegalArgumentException( "Map types require a schema" );
    }
    ColumnSchemaBuilder colBuilder = column( name );
    colBuilder.majorType( type );
    return this;
  }

  @Override
  public SchemaBuilder column( String name, MinorType type, DataMode cardinality ) {
    MajorType majorType = MajorType.newBuilder()
        .setMinorType( type )
        .setMode( cardinality )
        .build();
    return column( name, majorType );
  }

  @Override
  public RowSchema build( ) {
    if ( parent != null )
      throw new IllegalStateException( "Can only build the root schema" );
    globalIndex = 0;
    return doBuild( "" );
  }

  protected int nextGlobalIndex( ) {
    if (parent == null)
      return globalIndex++;
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
  public SchemaBuilder remove(String name) {
    ColumnSchemaBuilderImpl col = nameIndex.remove(RowSchemaImpl.key(caseSensitive, name));
    if (col != null) {
      columns.remove(col);
    }
    return this;
  }
}
