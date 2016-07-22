package org.apache.drill.yarn.core;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Test the dynamic function call mechanism for the two cases we need:
 * no arguments with return value, and with one scalar type, no return
 * value. Also test the cases that the method both exists and does not
 * exist to simulate distributions both with and without the methods.
 */

public class TestDynamicCall {

  private double value;

  public void setDisks( double d ) {
    value = d;
  }

  public double getDisks( ) {
    return value;
  }

  @Test
  public void test() {

    // No method called foo, no args.

    Object ret = DoYUtil.dynamicCall( this, "foo", null, null );
    assertNull( ret );

    value = 10;

    // Existing method, no args.

    ret = DoYUtil.dynamicCall( this, "getDisks", null, null );
    assertNotNull( ret );
    assertEquals( 10.0, (double) (Double) ret, 0.001 );

    // No method called, foo, with args

    ret = DoYUtil.dynamicCall( this, "foo", new Object[] { new Double( 20.0 ) }, new Class<?>[] { Double.TYPE } );
    assertNull( ret );

    // Existing method, with args.

    ret = DoYUtil.dynamicCall( this, "setDisks", new Object[] { new Double( 20.0 ) }, new Class<?>[] { Double.TYPE } );
    assertNull( ret );
    assertEquals( 20.0, value, 0.001 );
    
    // Repeat, using the wrapped methods.
    
    assertEquals( 20.0, DoYUtil.callGetDiskIfExists( this ), 0.001 );
    DoYUtil.callSetDiskIfExists( this, 5 );
    assertEquals( 5.0, value, 0.001 );
  }

}
