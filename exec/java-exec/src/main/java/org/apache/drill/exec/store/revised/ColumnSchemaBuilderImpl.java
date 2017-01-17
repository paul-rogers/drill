package org.apache.drill.exec.store.revised;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
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
    return mapSchema;
  }

  @Override
  public RowSchemaBuilder getMapSchema() {
    return mapSchema;
  }

  protected ColumnSchema doBuild( int localIndex, int globalIndex, String prefix ) {
    MajorType type = MajorType.newBuilder()
        .setMinorType( MinorType.MAP )
        .setMode( DataMode.REQUIRED )
        .build();
    RowSchema mapSchemaDef = null;
    if ( mapSchema != null ) {
      mapSchemaDef = mapSchema.doBuild( prefix + name + ColumnSchema.NAME_SEPARATOR );
    }
    return new ColumnSchemaImpl( localIndex, globalIndex, prefix, name, type, mapSchemaDef );
  }
}
