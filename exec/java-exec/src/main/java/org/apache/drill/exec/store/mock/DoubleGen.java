package org.apache.drill.exec.store.mock;

import java.util.Random;

import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.ValueVector;

public class DoubleGen implements FieldGen {

  Random rand = new Random( );

  @Override
  public void setup(ColumnDef colDef) { }

  public double value( ) {
    return rand.nextDouble() * 1_000_000;
  }

  @Override
  public void setValue( ValueVector v, int index ) {
    Float8Vector vector = (Float8Vector) v;
    vector.getMutator().set(index, value());
  }

}
