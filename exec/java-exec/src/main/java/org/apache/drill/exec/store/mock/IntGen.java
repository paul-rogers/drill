package org.apache.drill.exec.store.mock;

import java.util.Random;

import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;

public class IntGen implements FieldGen {

  Random rand = new Random( );

  @Override
  public void setup(ColumnDef colDef) { }

  public int value( ) {
    return rand.nextInt();
  }

  @Override
  public void setValue( ValueVector v, int index ) {
    IntVector vector = (IntVector) v;
    vector.getMutator().set(index, value());
  }

}
