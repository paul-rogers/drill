package org.apache.drill.exec.expr.contrib.udfExample;

public class FunctionImpl {
  private static final double LOG_2 = Math.log(2.0D);

  public static final double log2x(double x) {
    return Math.log(x) / LOG_2;
  }
}
