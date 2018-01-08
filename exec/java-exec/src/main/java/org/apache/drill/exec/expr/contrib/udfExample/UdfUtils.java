package org.apache.drill.exec.expr.contrib.udfExample;

import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.joda.time.Period;

import com.google.common.base.Charsets;

import io.netty.buffer.DrillBuf;

/**
 * Convenience functions for UDF implementation.
 * <p>
 * <b>Important Caveat:</b> The use of methods that take a holder
 * as a parameter can have a performance impact on Drill queries.
 * Drill seeks to use "scalar replacement" to replace holder objects
 * with the use of local variables. This works only if you follow
 * two rules:
 * <ul>
 * <li>Call no methods on holders.</li>
 * <li>Do not pass holders to other functions.</li>
 * </ul>
 * Clearly, the use of the methods here violate the second guideline.
 * The code will still work, but your code may suffer a slight performance
 * penalty as a result.
 * <p>
 * Suggestion: start by using the methods here since they make your
 * code much simpler and more reliable. Then, do a performance test. If
 * you see an advantage of avoiding the methods, then you can manually
 * "in-line" the code from here into your own function. (Presumably, this
 * is what the Java JIT compiler will do on its own, but much of Drill's
 * design is based on the dubious assumption that Drill can do a better
 * job than Java can...)
 */

public class UdfUtils {

  /**
   * Convert a non-nullable VarChar input to a Java string, assuming UTF-8 encoding.
   *
   * @param input the VarChar holder for the input parameter
   * @return Java String that holds the value
   */

  public static String varCharToString(VarCharHolder input) {
    return org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(input);
  }

  /**
   * Convert a nullable VarChar input to a Java string, assuming UTF-8 encoding.
   *
   * @param input the VarChar holder for the input parameter
   * @return Java String that holds the value, or null if the input
   * is null
   */

  public static String varCharToString(NullableVarCharHolder input) {
    if (input.isSet == 0) {
      return null;
    }
    return org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(input);
  }

  /**
   * Convert a non-nullable VarBinary to a Java string, assuming that the
   * VarBinary is actually a UTF-8 encoded string, as is often the case
   * with HBase.
   *
   * @param input the input VarBinary parameter
   * @return a Java string
   */

  public static String varBinaryToString(VarBinaryHolder input) {
    int len = input.end - input.start;
    byte buf[] = new byte[len];
    input.buffer.getBytes(input.start, buf);
    return new String(buf, Charsets.UTF_8);
  }

  /**
   * Convert a nullable VarBinary to a Java string, assuming that the
   * VarBinary is actually a UTF-8 encoded string, as is often the case
   * with HBase.
   *
   * @param input the input VarBinary parameter
   * @return a Java string, or null if the parameter was NULL
   */

  public static String varBinaryToString(NullableVarBinaryHolder input) {
    if (input.isSet == 0) {
      return null;
    }
    int len = input.end - input.start;
    byte buf[] = new byte[len];
    input.buffer.getBytes(input.start, buf);
    return new String(buf, Charsets.UTF_8);
  }

  /**
   * Convert a Java string to a Drill non-nullable output. Encodes the
   * Java string into a set of UTF-8 bytes. Resizes the working
   * buffer if needed. For this reason, you <b>must</b> assign the result of
   * this function to your <tt>@Inject</tt> <tt>DrillBuf</tt>. That is:
   * <pre><code>
   * {@literal @}Output VarCharHolder output;
   * {@literal @}Inject DrillBuf outputBuf;
   * ...
   *   String result = ...
   *   outputBuf = varCharOutput(result, outputBuf, output);
   * </code></pre>
   *
   * @param result the (non-null) string value to return
   * @param outputBuf the output buffer identified by the
   * {@literal @}Inject annotation
   * @param output the non-nullable VarChar holder for the function output
   * identified by the {@literal @}Output annotation
   * @return the (possibly new) output buffer
   */

  public static DrillBuf varCharOutput(String result, DrillBuf outputBuf, VarCharHolder output) {
    byte outBytes[] = result.toString().getBytes(com.google.common.base.Charsets.UTF_8);
    outputBuf = outputBuf.reallocIfNeeded(outBytes.length);
    outputBuf.setBytes(0, outBytes);
    output.buffer = outputBuf;
    output.start = 0;
    output.end = outBytes.length;
    return outputBuf;
  }

  /**
   * Convert a Java string to an UTF-8 nullable VarChar buffer. The output will be
   * null if the input is null. Else, this function works like the non-nullable
   * version.
   *
   * @param result the (possibly null) result string
   * @param outputBuf the output buffer identified by the
   * {@literal @}Inject annotation
   * @param output the nullable VarChar holder for the function output
   * identified by the {@literal @}Output annotation
   * @return the (possibly new) output buffer
   */

  public static DrillBuf varCharOutput(String result, DrillBuf outputBuf, NullableVarCharHolder output) {
    if (result == null) {
      output.buffer = outputBuf;
      output.isSet = 0;
      return outputBuf;
    }
    byte outBytes[] = result.toString().getBytes(com.google.common.base.Charsets.UTF_8);
    outputBuf = outputBuf.reallocIfNeeded(outBytes.length);
    outputBuf.setBytes(0, outBytes);
    output.isSet = 1;
    output.buffer = outputBuf;
    output.start = 0;
    output.end = outBytes.length;
    return outputBuf;
  }

  public static Period intervalDayToPeriod(int days, int millis) {
    final Period p = new Period();
    return p.plusDays(days).plusMillis(millis);
  }

  public static Period intervalYearToPeriod(int value) {
    final int years  = (value / DateUtility.yearsToMonths);
    final int months = (value % DateUtility.yearsToMonths);
    final Period p = new Period();
    return p.plusYears(years).plusMonths(months);
  }

  public static Period intervalToPeriod(int months, int days, int millis) {
    final Period p = new Period();
    return p.plusMonths(months).plusDays(days).plusMillis(millis);
  }
}
