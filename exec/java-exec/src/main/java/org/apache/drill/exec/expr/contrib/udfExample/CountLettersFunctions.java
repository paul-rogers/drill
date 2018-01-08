package org.apache.drill.exec.expr.contrib.udfExample;

import javax.inject.Inject;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import io.netty.buffer.DrillBuf;

public class CountLettersFunctions {

  @FunctionTemplate(
      name = "countLettersAscii",
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL)

  // INT-REQUIRED countLettersAscii(VARCHAR-REQUIRED)

  public static class CountLettersAsciiFunction implements DrillSimpleFunc {
    @Param VarCharHolder input;
    @Output IntHolder output;

    @Override
    public void setup() { }

    @Override
    public void eval() {
      int len = input.end - input.start;
      output.value = 0;
      for (int i = 0; i < len; i++) {
        int c = (input.buffer.getByte(input.start + i) & 0xFF);
        if (Character.isAlphabetic(c)) {
          output.value++;
        }
      }
    }
  }

  @FunctionTemplate(
      name = "countLetters",
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL)

  // INT-REQUIRED countLettersAscii(VARCHAR-REQUIRED)

  public static class CountLettersFunction implements DrillSimpleFunc {
    @Param VarCharHolder input;
    @Output IntHolder output;

    @Override
    public void setup() { }

    @Override
    public void eval() {
      String value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(input);
      output.value = 0;
      for (int i = 0; i < value.length(); i++) {
        if (Character.isAlphabetic(value.charAt(i))) {
          output.value++;
        }
      }
    }
  }

  @FunctionTemplate(
      name = "extractLetters",
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL)

  // VARCHAR-REQUIRED extractLetters(VARCHAR-REQUIRED)

  public static class ExtractLettersFunction implements DrillSimpleFunc {
    @Param VarCharHolder input;
    @Output VarCharHolder output;
    @Inject DrillBuf outputBuf;

    @Override
    public void setup() { }

    @Override
    public void eval() {

      // Convert the input to a string.

      String value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(input);

      // Build up the output string.

      StringBuilder result = new StringBuilder();
      for (int i = 0; i < value.length(); i++) {
        char c = value.charAt(i);
        if (Character.isAlphabetic(c)) {
          result.append(c);
        }
      }

      // Convert to the output buffer.

      byte outBytes[] = result.toString().getBytes(com.google.common.base.Charsets.UTF_8);
      System.out.println(outputBuf.capacity());
      outputBuf = outputBuf.reallocIfNeeded(outBytes.length);
      outputBuf.setBytes(0, outBytes);
      output.buffer = outputBuf;
      output.start = 0;
      output.end = outBytes.length;
    }
  }


  @FunctionTemplate(
      name = "dupit",
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL)

  // VARCHAR-REQUIRED dupit(VARCHAR-REQUIRED, INT-REQUIRED)

  public static class DupItFunction implements DrillSimpleFunc {
    @Param VarCharHolder input;
    @Param IntHolder count;
    @Output VarCharHolder output;
    @Inject DrillBuf outputBuf;

    @Override
    public void setup() { }

    @Override
    public void eval() {

      // Convert the input to a string.

      String value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(input);

      // Build up the output string.

      StringBuilder result = new StringBuilder();
      for (int i = 0; i < count.value; i++) {
        result.append(value);
      }

      // Convert to the output buffer.

      byte outBytes[] = result.toString().getBytes(com.google.common.base.Charsets.UTF_8);
      outputBuf = outputBuf.reallocIfNeeded(outBytes.length);
      outputBuf.setBytes(0, outBytes);
      output.buffer = outputBuf;
      output.start = 0;
      output.end = outBytes.length;
    }
  }

}
