package org.apache.drill.exec.store.mock;

import java.util.Random;

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;

public class StringGen implements FieldGen {

  Random rand = new Random( );
  int length;

  @Override
  public void setup( ColumnDef colDef ) {
    length = colDef.width;
  }

  public String value( ) {
    String c = Character.toString( (char) (rand.nextInt(26) + 'A') );
    StringBuilder buf = new StringBuilder( );
    for ( int i = 0;  i < length;  i++ ) {
      buf.append( c );
    }
    return buf.toString();
  }

  @Override
  public void setValue( ValueVector v, int index ) {
    VarCharVector vector = (VarCharVector) v;
    vector.getMutator().setSafe(index, value().getBytes());
  }
}
