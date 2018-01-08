package org.apache.drill.exec.expr.contrib.udfExample;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.ObjectHolder;

public class WeightedAverageImpl {

  @FunctionTemplate(
      name = "myAvg",
      scope = FunctionScope.POINT_AGGREGATE,
      nulls = NullHandling.INTERNAL)

  public static class MyAvgFunc implements DrillAggFunc {

    @Param NullableFloat8Holder input;
    @Workspace Float8Holder sum;
    @Workspace IntHolder count;
    @Output Float8Holder output;

    @Override
    public void setup() { }

    @Override
    public void reset() {
      sum.value = 0;
      count.value = 0;
    }

    @Override
    public void add() {
      if (input.isSet == 1) {
        count.value++;
        sum.value += input.value;
      }
    }

    @Override
    public void output() {
      if (count.value == 0) {
        output.value = 0;
      } else {
        output.value = sum.value / count.value;
      }
    }
  }

  @FunctionTemplate(
      name = "wtAvg",
      scope = FunctionScope.POINT_AGGREGATE,
      nulls = NullHandling.INTERNAL)

  public static class WeightedAvgFunc implements DrillAggFunc {

    @Param NullableFloat8Holder input;
    @Param NullableFloat8Holder weight;
    @Workspace Float8Holder sum;
    @Workspace Float8Holder totalWeights;
    @Output Float8Holder output;

    @Override
    public void setup() { }

    @Override
    public void reset() {
      sum.value = 0;
      totalWeights.value = 0;
    }

    @Override
    public void add() {
      if (input.isSet == 1 && weight.isSet == 1) {
        totalWeights.value += weight.value;
        sum.value += input.value * weight.value;
      }
    }

    @Override
    public void output() {
      if (totalWeights.value == 0) {
        output.value = 0;
      } else {
        output.value = sum.value / totalWeights.value;
      }
    }
  }

  public static class WeightedAvgImpl {
    double sum;
    double totalWeights;

    public void reset() {
      sum = 0;
      totalWeights = 0;
    }

    public void add(double input, double weight) {
      totalWeights += weight;
      sum += input * weight;
    }

    public double output() {
      if (totalWeights == 0) {
        return 0;
      } else {
        return sum / totalWeights;
      }
    }
  }

  @FunctionTemplate(
      name = "wtAvg2",
      scope = FunctionScope.POINT_AGGREGATE,
      nulls = NullHandling.INTERNAL)

  public static class WeightedAvgWrapper implements DrillAggFunc {

    @Param NullableFloat8Holder input;
    @Param NullableFloat8Holder weight;
    @Workspace ObjectHolder impl;
    @Output Float8Holder output;

    @Override
    public void setup() {
      impl.obj = new org.apache.drill.exec.expr.contrib.udfExample.WeightedAverageImpl.WeightedAvgImpl();
    }

    @Override
    public void reset() {
      ((org.apache.drill.exec.expr.contrib.udfExample.WeightedAverageImpl.WeightedAvgImpl) impl.obj).reset();
    }

    @Override
    public void add() {
      if (input.isSet == 1 && weight.isSet == 1) {
        ((org.apache.drill.exec.expr.contrib.udfExample.WeightedAverageImpl.WeightedAvgImpl) impl.obj).add(
            input.value, weight.value);
      }
    }

    @Override
    public void output() {
      output.value = ((org.apache.drill.exec.expr.contrib.udfExample.WeightedAverageImpl.WeightedAvgImpl) impl.obj).output();
    }
  }

}
