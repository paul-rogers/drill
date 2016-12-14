package org.apache.drill.exec.store.mock;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;

public class DateGen implements FieldGen {

  Random rand = new Random( );
  long baseTime;
  DateTimeFormatter fmt = DateTimeFormatter.ISO_LOCAL_DATE;

  public DateGen( ) {
    baseTime = System.currentTimeMillis() - 365 * 24 * 60 * 60 * 1000;
  }

  @Override
  public void setup(ColumnDef colDef) { }

  public long value( ) {
    return baseTime + rand.nextInt( 365 ) * 24 * 60 * 60 * 1000;
  }

  @Override
  public void setValue( ValueVector v, int index ) {
    VarCharVector vector = (VarCharVector) v;
    Instant instant = Instant.ofEpochMilli( value( ) );
    String str = fmt.format( LocalDateTime.ofInstant(instant, ZoneId.systemDefault()) );
    vector.getMutator().setSafe(index, str.getBytes());
  }
}
