package org.apache.drill.exec.store.mock;

import java.util.Random;

import org.apache.drill.exec.physical.rowSet.ColumnLoader;

public abstract class AbstractFieldGen implements FieldGen {

  protected ColumnDef colDef;
  protected ColumnLoader colWriter;
  protected final Random rand = new Random();

  @Override
  public void setup(ColumnDef colDef, ColumnLoader colLoader) {
    this.colDef = colDef;
    this.colWriter = colLoader;
  }

}
