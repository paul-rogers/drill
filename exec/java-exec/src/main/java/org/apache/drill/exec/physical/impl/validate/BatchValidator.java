/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.validate;

import java.lang.reflect.Method;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.drill.exec.physical.impl.protocol.OperatorRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SimpleVectorWrapper;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.RepeatedBitVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Validate a batch of value vectors. It is not possible to validate the
 * data, but we can validate the structure, especially offset vectors.
 * Only handles single (non-hyper) vectors at present. Current form is
 * self-contained. Better checks can be done by moving checks inside
 * vectors or by exposing more metadata from vectors.
 */

public class BatchValidator {
  private static final Logger logger = LoggerFactory.getLogger(BatchValidator.class);

  public static final boolean LOG_TO_STDOUT = true;
  public static final int MAX_ERRORS = 100;

  public interface ErrorReporter {
    void error(String name, ValueVector vector, String msg);
    void warn(String name, ValueVector vector, String msg);
    void error(String msg);
    int errorCount();
  }

  public abstract static class BaseErrorReporter implements ErrorReporter {

    private final String opName;
    private int errorCount;

    public BaseErrorReporter(String opName) {
      this.opName = opName;
    }

    protected boolean startError() {
      if (errorCount == 0) {
        warn("Found one or more vector errors from " + opName);
      }
      errorCount++;
      if (errorCount >= MAX_ERRORS) {
        return false;
      }
      return true;
    }

    @Override
    public void error(String name, ValueVector vector, String msg) {
      error(String.format("%s - %s: %s",
            name, vector.getClass().getSimpleName(), msg));
    }

    @Override
    public void warn(String name, ValueVector vector, String msg) {
      warn(String.format("%s - %s: %s",
            name, vector.getClass().getSimpleName(), msg));
    }

    public abstract void warn(String msg);

    @Override
    public int errorCount() { return errorCount; }
  }

  private static class StdOutReporter extends BaseErrorReporter {

    public StdOutReporter(String opName) {
      super(opName);
    }

    @Override
    public void error(String msg) {
      if (startError()) {
        System.out.println(msg);
      }
    }

    @Override
    public void warn(String msg) {
      System.out.println(msg);
    }
  }

  private static class LogReporter extends BaseErrorReporter {

    public LogReporter(String opName) {
      super(opName);
    }

    @Override
    public void error(String msg) {
      if (startError()) {
        logger.error(msg);
      }
    }

    @Override
    public void warn(String msg) {
      logger.error(msg);
    }
  }

  private enum CheckMode { COUNTS, ALL };

  private static final Map<Class<? extends RecordBatch>, CheckMode> checkRules = buildRules();

  private final ErrorReporter errorReporter;

  public BatchValidator(ErrorReporter errorReporter) {
    this.errorReporter = errorReporter;
  }

  /**
   * At present, most operators will not pass the checks here. The following
   * table identifies those that should be checked, and the degree of check.
   * Over time, this table should include all operators, and thus become
   * unnecessary.
   */
  private static Map<Class<? extends RecordBatch>, CheckMode> buildRules() {
    final Map<Class<? extends RecordBatch>, CheckMode> rules = new IdentityHashMap<>();
    rules.put(OperatorRecordBatch.class, CheckMode.ALL);
    return rules;
  }

  public static boolean validate(RecordBatch batch) {
    final CheckMode checkMode = checkRules.get(batch.getClass());

    // If no rule, don't check this batch.

    if (checkMode == null) {

      // As work proceeds, might want to log those batches not checked.
      // For now, there are too many.

      return true;
    }

    // All batches that do any checks will at least check counts.

    final ErrorReporter reporter = errorReporter(batch);
    final int rowCount = batch.getRecordCount();
    int valueCount = rowCount;
    final VectorContainer container = batch.getContainer();
    if (!container.hasRecordCount()) {
      reporter.error(String.format(
          "%s: Container record count not set",
          batch.getClass().getSimpleName()));
    } else {
      // Row count will = container count for most operators.
      // Row count <= container count for the filter operator.

      final int containerRowCount = container.getRecordCount();
      valueCount = containerRowCount;
      switch (batch.getSchema().getSelectionVectorMode()) {
      case FOUR_BYTE:
        final int sv4Count = batch.getSelectionVector4().getCount();
        if (sv4Count != rowCount) {
          reporter.error(String.format(
              "Mismatch between %s record count = %d, SV4 record count = %d",
              batch.getClass().getSimpleName(),
              rowCount, sv4Count));
        }
        // TODO: Don't know how to check SV4 batches
        return true;
      case TWO_BYTE:
        final int sv2Count = batch.getSelectionVector2().getCount();
        if (sv2Count != rowCount) {
          reporter.error(String.format(
              "Mismatch between %s record count = %d, SV2 record count = %d",
              batch.getClass().getSimpleName(),
              rowCount, sv2Count));
        }
        if (sv2Count > containerRowCount) {
          reporter.error(String.format(
              "Mismatch between %s container count = %d, SV2 record count = %d",
              batch.getClass().getSimpleName(),
              containerRowCount, sv2Count));
        }
        final int svTotalCount = batch.getSelectionVector2().getBatchActualRecordCount();
        if (svTotalCount != containerRowCount) {
          reporter.error(String.format(
              "Mismatch between %s container count = %d, SV2 total count = %d",
              batch.getClass().getSimpleName(),
              containerRowCount, svTotalCount));
        }
        break;
      default:
        if (rowCount != containerRowCount) {
          reporter.error(String.format(
              "Mismatch between %s record count = %d, container record count = %d",
              batch.getClass().getSimpleName(),
              rowCount, containerRowCount));
        }
        break;
      }
    }
    if (checkMode == CheckMode.ALL) {
      new BatchValidator(reporter).validateBatch(batch, valueCount);
    }
    return reporter.errorCount() == 0;
  }

  public static boolean validate(VectorAccessible batch) {
    final ErrorReporter reporter = errorReporter(batch);
    new BatchValidator(reporter).validateBatch(batch, batch.getRecordCount());
    return reporter.errorCount() == 0;
  }

  private static ErrorReporter errorReporter(VectorAccessible batch) {
    final String opName = batch.getClass().getSimpleName();
    if (LOG_TO_STDOUT) {
      return new StdOutReporter(opName);
    } else {
      return new LogReporter(opName);
    }
  }

  public void validateBatch(VectorAccessible batch, int rowCount) {
    for (final VectorWrapper<? extends ValueVector> w : batch) {
      validateWrapper(rowCount, w);
    }
  }

  private void validateWrapper(int rowCount, VectorWrapper<? extends ValueVector> w) {
    if (w instanceof SimpleVectorWrapper) {
      validateVector(rowCount, w.getValueVector());
    }
  }

  private void validateVector(int expectedCount, ValueVector vector) {
    final int valueCount = vector.getAccessor().getValueCount();
    if (valueCount != expectedCount) {
      error(vector.getField().getName(), vector,
          String.format("Row count = %d, but value count = %d",
              expectedCount, valueCount));
    }
    validateVector(vector.getField().getName(), vector);
  }

  private void validateVector(String name, ValueVector vector) {
    if (vector instanceof BitVector) {
      validateBitVector(name, (BitVector) vector);
    } else if (vector instanceof RepeatedBitVector) {
      validateRepeatedBitVector(name, (RepeatedBitVector) vector);
    } else if (vector instanceof NullableVector) {
      validateNullableVector(name, (NullableVector) vector);
    } else if (vector instanceof VariableWidthVector) {
      validateVariableWidthVector(name, (VariableWidthVector) vector);
    } else if (vector instanceof FixedWidthVector) {
      validateFixedWidthVector(name, (FixedWidthVector) vector);
    } else if (vector instanceof BaseRepeatedValueVector) {
      validateRepeatedVector(name, (BaseRepeatedValueVector) vector);
    } else {
      logger.debug("Don't know how to validate vector: {}  of class {}",
          name, vector.getClass().getSimpleName());
    }
  }

  private void validateNullableVector(String name, NullableVector vector) {
    final int outerCount = vector.getAccessor().getValueCount();
    final ValueVector valuesVector = vector.getValuesVector();
    final int valueCount = valuesVector.getAccessor().getValueCount();
    if (valueCount != outerCount) {
      error(name, vector, String.format(
          "Outer value count = %d, but inner value count = %d",
          outerCount, valueCount));
    }
    verifyIsSetVector(vector, (UInt1Vector) vector.getBitsVector());
    validateVector(name + "-values", valuesVector);
  }

  private void validateVariableWidthVector(String name, VariableWidthVector vector) {

    // Offsets are in the derived classes. Handle only VarChar for now.

    if (vector instanceof VarCharVector) {
      validateVarCharVector(name, (VarCharVector) vector);
    } else {
      logger.debug("Don't know how to validate vector: {}  of class {}",
          name, vector.getClass().getSimpleName());
    }
  }

  private void validateVarCharVector(String name, VarCharVector vector) {
    final int valueCount = vector.getAccessor().getValueCount();

    // Disabled because a large number of operators
    // set up offset vectors wrongly.
    if (valueCount == 0) {
      return;
    }

    final int dataLength = vector.getBuffer().writerIndex();
    validateOffsetVector(name + "-offsets", vector.getOffsetVector(), false, valueCount, dataLength);
  }

//  private void validateRepeatedBitVector(String name, BaseRepeatedValueVector vector) {
//    int dataLength = ((BaseDataValueVector) vector.getDataVector()).getBuffer().writerIndex();
//    int valueCount = vector.getAccessor().getValueCount();
//    int itemCount = validateOffsetVector(name + "-offsets", vector.getOffsetVector(), true, valueCount * 8, dataLength);
//    ValueVector dataVector = vector.getDataVector();
//    if (dataVector.getAccessor().getValueCount() != itemCount) {
//      error(name, vector, String.format(
//          "Bit vector has %d values, but offset vector labels %d values",
//          valueCount, itemCount));
//    }
//  }

  private void validateRepeatedVector(String name, BaseRepeatedValueVector vector) {
    final ValueVector dataVector = vector.getDataVector();
    final int dataLength = dataVector.getAccessor().getValueCount();
    final int valueCount = vector.getAccessor().getValueCount();
    final int itemCount = validateOffsetVector(name + "-offsets", vector.getOffsetVector(),
        true, valueCount, dataLength);

    if (dataLength != itemCount) {
      error(name, vector, String.format(
          "Data vector has %d values, but offset vector has %d values",
          dataLength, itemCount));
    }

    // Special handling of repeated VarChar vectors
    // The nested data vectors are not quite exactly like top-level vectors.

    if (dataVector instanceof VariableWidthVector) {
      validateVariableWidthVector(name + "-data", (VariableWidthVector) dataVector);
    }
  }

  private void validateRepeatedBitVector(String name, RepeatedBitVector vector) {
    final int valueCount = vector.getAccessor().getValueCount();
    final int maxBitCount = valueCount * 8;
    final int elementCount = validateOffsetVector(name + "-offsets",
        vector.getOffsetVector(), true, valueCount, maxBitCount);
    final BitVector dataVector = vector.getDataVector();
    if (dataVector.getAccessor().getValueCount() != elementCount) {
      error(name, vector, String.format(
          "Bit vector has %d values, but offset vector labels %d values",
          valueCount, elementCount));
    }
    validateBitVector(name + "-data", dataVector);
  }

  private void validateFixedWidthVector(String name, FixedWidthVector vector) {
    // Not much to do. The only item to check is the vector
    // count itself, which was already done. There is no inner
    // structure to check.
  }

  private void validateBitVector(String name, BitVector vector) {
    final BitVector.Accessor accessor = vector.getAccessor();
    final int valueCount = accessor.getValueCount();
    final int dataLength = vector.getBuffer().writerIndex();
    final int expectedLength = BitVector.getSizeFromCount(valueCount);
    if (dataLength != expectedLength) {
      error(name, vector, String.format(
          "Bit vector has %d values, buffer has length %d, expected %d",
          valueCount, dataLength, expectedLength));
    }
  }

  private int validateOffsetVector(String name, UInt4Vector offsetVector,
      boolean repeated, int valueCount, int maxOffset) {
    final UInt4Vector.Accessor accessor = offsetVector.getAccessor();
    final int offsetCount = accessor.getValueCount();
    // TODO: Disabled because a large number of operators
    // set up offset vectors incorrectly.
//    if (!repeated && offsetCount == 0) {
//      System.out.println(String.format(
//          "Offset vector for %s: [0] has length 0, expected 1+",
//          name));
//      return false;
//    }
    if (valueCount == 0 && offsetCount > 1 || valueCount > 0 && offsetCount != valueCount + 1) {
      error(name, offsetVector, String.format(
          "Outer vector has %d values, but offset vector has %d, expected %d",
          valueCount, offsetCount, valueCount + 1));
    }
    if (valueCount == 0) {
      return 0;
    }

    // First value must be zero in current version.

    int prevOffset = accessor.get(0);
    if (prevOffset != 0) {
      error(name, offsetVector, "Offset (0) must be 0 but was " + prevOffset);
    }

    for (int i = 1; i < offsetCount; i++) {
      final int offset = accessor.get(i);
      if (offset < prevOffset) {
        error(name, offsetVector, String.format(
            "Offset vector [%d] contained %d, expected >= %d",
            i, offset, prevOffset));
      } else if (offset > maxOffset) {
        error(name, offsetVector, String.format(
            "Invalid offset at index %d: %d exceeds maximum of %d",
            i, offset, maxOffset));
      }
      prevOffset = offset;
    }
    return prevOffset;
  }

  private void error(String name, ValueVector vector, String msg) {
    errorReporter.error(name, vector, msg);
  }

  private void verifyIsSetVector(ValueVector parent, UInt1Vector bv) {
    final String name = String.format("%s (%s)-bits",
        parent.getField().getName(),
        parent.getClass().getSimpleName());
    final int rowCount = parent.getAccessor().getValueCount();
    final int bitCount = bv.getAccessor().getValueCount();
    if (bitCount != rowCount) {
      error(name, bv, String.format(
          "Value count = %d, but bit count = %d",
          rowCount, bitCount));
    }
    final UInt1Vector.Accessor ba = bv.getAccessor();
    for (int i = 0; i < bitCount; i++) {
      final int value = ba.get(i);
      if (value != 0 && value != 1) {
        error(name, bv, String.format(
            "%s %s: bit vector[%d] = %d, expected 0 or 1",
            i, value));
      }
    }
  }

  /**
   * Print a record batch. Uses code only available in a test build.
   * Classes are not visible to the compiler; must load dynamically.
   * Does nothing if the class is not available.
   */

  public static void print(RecordBatch batch) {
    try {
      final Class<?> helper = Class.forName("org.apache.drill.test.rowSet.RowSetUtilities");
      final Method print = helper.getMethod("print", VectorContainer.class);
      System.out.println(batch.getClass().getSimpleName());
      print.invoke(null, batch.getContainer());
    } catch (final Exception e) {
      System.out.println(e.getMessage());
      // Ignore
    }
  }
}
