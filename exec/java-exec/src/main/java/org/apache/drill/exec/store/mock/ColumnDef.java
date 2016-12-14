package org.apache.drill.exec.store.mock;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.store.mock.MockGroupScanPOP.MockColumn;

public class ColumnDef {
  public MockColumn mockCol;
  public String name;
  public int width;
  public FieldGen generator;

  public ColumnDef( MockColumn mockCol ) {
    this.mockCol = mockCol;
    name = mockCol.getName();
    width = TypeHelper.getSize(mockCol.getMajorType());
    makeGenerator( );
  }

  private void makeGenerator( ) {
    String genName = mockCol.getGenerator( );
    if ( genName != null ) {
      if ( ! genName.contains(".") ) {
        genName = "org.apache.drill.exec.store.mock." + genName;
      }
      try {
        ClassLoader cl = getClass( ).getClassLoader();
        Class<?> genClass = cl.loadClass(genName);
        generator = (FieldGen) genClass.newInstance( );
      } catch (ClassNotFoundException | InstantiationException
          | IllegalAccessException | ClassCastException e) {
        throw new IllegalArgumentException( "Generator " + genName + " is undefined for mock field " + name );
      }
      generator.setup( this );
      return;
    }

    makeDefaultGenerator( );
  }

  private void makeDefaultGenerator( ) {

    MinorType minorType = mockCol.getMinorType();
    switch ( minorType ) {
    case BIGINT:
      break;
    case BIT:
      break;
    case DATE:
      break;
    case DECIMAL18:
      break;
    case DECIMAL28DENSE:
      break;
    case DECIMAL28SPARSE:
      break;
    case DECIMAL38DENSE:
      break;
    case DECIMAL38SPARSE:
      break;
    case DECIMAL9:
      break;
    case FIXED16CHAR:
      break;
    case FIXEDBINARY:
      break;
    case FIXEDCHAR:
      break;
    case FLOAT4:
      break;
    case FLOAT8:
      generator = new DoubleGen( );
      break;
    case GENERIC_OBJECT:
      break;
    case INT:
      generator = new IntGen( );
      break;
    case INTERVAL:
      break;
    case INTERVALDAY:
      break;
    case INTERVALYEAR:
      break;
    case LATE:
      break;
    case LIST:
      break;
    case MAP:
      break;
    case MONEY:
      break;
    case NULL:
      break;
    case SMALLINT:
      break;
    case TIME:
      break;
    case TIMESTAMP:
      break;
    case TIMESTAMPTZ:
      break;
    case TIMETZ:
      break;
    case TINYINT:
      break;
    case UINT1:
      break;
    case UINT2:
      break;
    case UINT4:
      break;
    case UINT8:
      break;
    case UNION:
      break;
    case VAR16CHAR:
      break;
    case VARBINARY:
      break;
    case VARCHAR:
      generator = new StringGen( );
      break;
    default:
      break;
    }
    if ( generator == null ) {
      throw new IllegalArgumentException( "No default column generator for column " + name + " of type " + minorType );
    }
    generator.setup(this);
  }

  public ColumnDef( MockColumn mockCol, int rep ) {
    this( mockCol );
    name = name += Integer.toString(rep);
  }

  public MockColumn getConfig() {
    return mockCol;
  }

  public String getName() {
    return name;
  }
}