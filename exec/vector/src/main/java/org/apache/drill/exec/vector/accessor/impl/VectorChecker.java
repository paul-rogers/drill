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
package org.apache.drill.exec.vector.accessor.impl;

import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;

/**
 * Handy tool for checking vector invariants when something
 * seems amiss.
 */

public class VectorChecker {

  /**
   * Verify an offset vector. Use this when the vector printer suggests
   * that something is amiss.
   */

  public static boolean verifyOffsets(String name, UInt4Vector vector) {
    UInt4Vector.Accessor va = vector.getAccessor();
    int valueCount = va.getValueCount();
    if (valueCount == 0) {
      return true;
    }
    int prev = va.get(0);
    if (prev != 0) {
      System.out.println(String.format(
          "Offset vector for %s: [0] contained %d, expected 0",
          name, prev));
      VectorPrinter.printOffsets(vector, 0, valueCount);
      return false;
    }
    for (int i = 1; i < valueCount; i++) {
      int offset = va.get(i);
      if (offset < prev) {
        System.out.println(String.format(
            "Offset vector for %s: [%d] contained %d, expected >= %d",
            name, i, offset, prev));
        return false;
      }
      prev = offset;
    }
    return true;
  }

  public static boolean verifyNullableVarChar(NullableVarCharVector vector) {
    if (! verifyVarChar(vector.getValuesVector())) {
      return false;
    }
    int rowCount = vector.getAccessor().getValueCount();
    int valueCount = vector.getValuesVector().getAccessor().getValueCount();
    if (valueCount != rowCount) {
      System.out.println(String.format(
          "NullableVarCharVector %s: outer value count = %d, but inner value count = %d",
          vector.getField().getName(), rowCount, valueCount));
      return false;
    }
    return verifyIsSetVector(vector, vector.getBitsVector());
  }

  public static boolean verifyRepeatedVarChar(RepeatedVarCharVector vector) {
    if (! verifyOffsets(vector.getField().getName(), vector.getOffsetVector())) {
      return false;
    }
    int rowCount = vector.getAccessor().getValueCount();
    int offsetCount = vector.getOffsetVector().getAccessor().getValueCount();
    if (offsetCount != rowCount + 1) {
      System.out.println(String.format(
          "RepeatedVarCharVector %s: value count = %d, but offset count = %d",
          vector.getField().getName(), rowCount, offsetCount));
      return false;
    }
    int lastOffset = vector.getOffsetVector().getAccessor().get(rowCount);
    int valueCount = vector.getDataVector().getAccessor().getValueCount();
    if (valueCount != lastOffset) {
      System.out.println(String.format(
          "RepeatedVarCharVector %s: offset vector count = %d, but value count = %d",
          vector.getField().getName(), lastOffset, valueCount));
      return false;
    }
    if (rowCount == 0) {
      return true;
    }
    return verifyVarChar(vector.getDataVector());
  }

  public static boolean verifyIsSetVector(ValueVector parent, UInt1Vector bv) {
    int rowCount = parent.getAccessor().getValueCount();
    int bitCount = bv.getAccessor().getValueCount();
    if (bitCount != rowCount) {
      System.out.println(String.format(
          "%s %s: value count = %d, but bit count = %d",
          parent.getClass().getSimpleName(),
          parent.getField().getName(), rowCount, bitCount));
      return false;
    }
    UInt1Vector.Accessor ba = bv.getAccessor();
    for (int i = 0; i < bitCount; i++) {
      int value = ba.get(i);
      if (value != 0 && value != 1) {
        System.out.println(String.format(
            "%s %s: bit vector[%d] = %d, expected 0 or 1",
            parent.getClass().getSimpleName(),
            parent.getField().getName(), i, value));
        return false;
      }
    }
    return true;
  }

  public static boolean verifyVarChar(VarCharVector vector) {
    int size = vector.getAccessor().getValueCount();

    // A pre-serialized VarChar has a one-item offset vector.
    // A deserialized VarChar has a zero-item offset vector.

    if (size == 0) {
      return true;
    }
    if (! verifyOffsets(vector.getField().getName(), vector.getOffsetVector())) {
      return false;
    }
    int offsetSize = vector.getOffsetVector().getAccessor().getValueCount();
    if (offsetSize != size + 1) {
      System.out.println(String.format(
          "VarChar vector %s: size = %d, but offset vector size is %d, expected %d",
          vector.getField().getName(), size,
          offsetSize, size + 1));
      return false;
    }
    int lastOffset = vector.getOffsetVector().getAccessor().get(offsetSize - 1);
    int dataSize = vector.getBuffer().writerIndex();
    if (dataSize != lastOffset) {
      System.out.println(String.format(
          "VarChar vector %s: writer index = %d, but last offset is %d",
          vector.getField().getName(), dataSize, lastOffset));
      return false;
    }
    return true;
  }

  public static boolean verifyBitVector(BitVector vector) {
    int size = vector.getAccessor().getValueCount();
    int expectedByteCount = (size + 7) / 8;
    if (vector.getBuffer().writerIndex() != expectedByteCount) {
      System.out.println(String.format(
          "BitVector %s: size = %d, expected bytes = %d, actual = %d",
          vector.getField().getName(), size,
          expectedByteCount, vector.getBuffer().writerIndex()));
    }
    return true;
  }

  public static boolean verifyNullableBitVector(NullableBitVector vector) {
    int size = vector.getAccessor().getValueCount();
    BitVector dv = vector.getValuesVector();
    if (size != dv.getAccessor().getValueCount()) {
      System.out.println(String.format(
          "NullableBitVector %s: size = %d, but data vector size = %d",
          vector.getField().getName(), size,
          dv.getAccessor().getValueCount()));
    }
    if (! verifyBitVector(dv)) {
      return false;
    }
    return verifyIsSetVector(vector, vector.getBitsVector());
  }

  public static boolean verify(ValueVector vector) {

    // Verifies a subset of vectors: those that have had issues.
    // Feel free to add others.

    if (vector instanceof VarCharVector) {
      return verifyVarChar((VarCharVector) vector);
    }
    if (vector instanceof NullableVarCharVector) {
      return verifyNullableVarChar((NullableVarCharVector) vector);
    }
    if (vector instanceof RepeatedVarCharVector) {
      return verifyRepeatedVarChar((RepeatedVarCharVector) vector);
    }
    if (vector instanceof BitVector) {
      return verifyBitVector((BitVector) vector);
    }
    if (vector instanceof NullableBitVector) {
      return verifyNullableBitVector((NullableBitVector) vector);
    }
    return true;
  }
}
