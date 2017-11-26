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
package org.apache.drill.exec.vector.accessor.reader;

import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Collection of hyper-vector accessors. These are needed to handle the
 * indirections in the hyper-vector case.
 * <p>
 * For a required vector:<br>
 * reader index --> hyper vector --> required vector
 * <p>
 * For a nullable vector:<br>
 * reader index --> hyper vector --> nullable vector<br>
 *   nullable vector --> bits vector<br>
 *                   --> values vector
 * <p>
 * For a repeated vector:<br>
 * reader index --> hyper vector --> repeated vector<br>
 *   repeated vector --> offset vector<br>
 *                   --> values vector
 * <p>
 * And so on. In each case, we must start with a top-level
 * vector as indicated the row index, indirected through the
 * SV4. That is done by the reader index. That points to a
 * top-level vector in the hyper-vector.
 * <p>
 * Most of the vectors needed are nested. These inner vectors
 * are not part of a hyper-vector list. Instead, we must get the
 * top-level vector, then navigate down from that vector to the
 * desired vector.
 * <p>
 * Sometimes the navigation is static (the "bits" vector for
 * a nullable vector.) Other times, it is a bit more dynamic: a
 * member of a map (given by index) or the member of a union
 * (given by type.)
 * <p>
 * These accessors can be chained to handle deeply-nested
 * structures such as an array of maps that contains a list of
 * unions.
 * <p>
 * Because the navigation is required on every access, the use of hyper
 * vectors is very slow. Since hyper-vectors are seldom used, we
 * optimize for the single-batch case by caching vectors at each
 * stage. Thus, for the single-batch case, we use different accessor
 * implementations. To keep the rest of the code simple, both the
 * hyper and single batch cases use the same API, but they use
 * entirely different implementations. The methods here choose
 * the correct implementation for the single and hyper cases.
 */

public class VectorAccessors {

  public static class SingleVectorAccessor implements VectorAccessor {

    private final ValueVector vector;

    public SingleVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    @Override
    public boolean isHyper() { return false; }

    @Override
    public void bind(ColumnReaderIndex index) { }

    @Override
    public MajorType type() { return vector.getField().getType(); }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() { return (T) vector; }
  }

  /**
   * Vector accessor used by the column accessors to obtain the vector for
   * each column value. That is, position 0 might be batch 4, index 3,
   * while position 1 might be batch 1, index 7, and so on.
   */

  public static abstract class BaseHyperVectorAccessor implements VectorAccessor {

    protected final MajorType type;

    public BaseHyperVectorAccessor(MajorType type) {
      this.type = type;
    }

    @Override
    public boolean isHyper() { return true; }

    @Override
    public void bind(ColumnReaderIndex index) { }

    @Override
    public MajorType type() { return type; }
  }

  public static class ArrayOffsetHyperVectorAccessor extends BaseHyperVectorAccessor {

    private VectorAccessor repeatedVectorAccessor;

    public ArrayOffsetHyperVectorAccessor(VectorAccessor va) {
      super(Types.required(MinorType.UINT4));
      repeatedVectorAccessor = va;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      RepeatedValueVector vector = repeatedVectorAccessor.vector();
      return (T) vector.getOffsetVector();
    }
  }

  public static class VarWidthOffsetHyperVectorAccessor extends BaseHyperVectorAccessor {

    private VectorAccessor varWidthVectorAccessor;

    public VarWidthOffsetHyperVectorAccessor(VectorAccessor va) {
      super(Types.required(MinorType.UINT4));
      varWidthVectorAccessor = va;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      VariableWidthVector vector = varWidthVectorAccessor.vector();
      return (T) vector.getOffsetVector();
    }
  }

  public static class ArrayDataHyperVectorAccessor implements VectorAccessor {

    private VectorAccessor repeatedVectorAccessor;

    private ArrayDataHyperVectorAccessor(VectorAccessor va) {
      repeatedVectorAccessor = va;
    }

    @Override
    public boolean isHyper() { return true; }

    @Override
    public MajorType type() { return repeatedVectorAccessor.type(); }

    @Override
    public void bind(ColumnReaderIndex index) { }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      RepeatedValueVector vector = repeatedVectorAccessor.vector();
      return (T) vector.getDataVector();
    }
  }

  public static class NullableValuesHyperVectorAccessor implements VectorAccessor {

    private VectorAccessor nullableAccessor;

    private NullableValuesHyperVectorAccessor(VectorAccessor va) {
      nullableAccessor = va;
    }

    @Override
    public boolean isHyper() { return true; }

    @Override
    public MajorType type() { return nullableAccessor.type(); }

    @Override
    public void bind(ColumnReaderIndex index) { }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      NullableVector vector = nullableAccessor.vector();
      return (T) vector.getValuesVector();
    }
  }

  /**
   * Extract null state from the a nullable vector's bits vector.
   */

  public static class NullableBitsHyperVectorStateReader extends BaseHyperVectorAccessor {

    public final VectorAccessor nullableAccessor;

    public NullableBitsHyperVectorStateReader(VectorAccessor nullableAccessor) {
      super(Types.required(MinorType.UINT1));
      this.nullableAccessor = nullableAccessor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      NullableVector vector = nullableAccessor.vector();
      return (T) vector.getBitsVector();
    }
  }

  /**
   * Extract null state from a list vector's bits vector.
   */

  public static class ListBitsHyperVectorStateReader extends BaseHyperVectorAccessor {

    public final VectorAccessor listAccessor;

    public ListBitsHyperVectorStateReader(VectorAccessor listAccessor) {
      super(Types.required(MinorType.UINT1));
      this.listAccessor = listAccessor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      ListVector vector = listAccessor.vector();
      return (T) vector.getBitsVector();
    }
  }

  private static class HyperMemberVectorAccessor extends BaseHyperVectorAccessor {

    private final VectorAccessor mapAccessor;
    private final int index;

    private HyperMemberVectorAccessor(VectorAccessor va, int index, MajorType memberType) {
      super(memberType);
      mapAccessor = va;
      this.index = index;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      AbstractMapVector vector = mapAccessor.vector();
      return (T) vector.getChildByOrdinal(index);
    }
  }

  public static VectorAccessor arrayOffsetVectorAccessor(VectorAccessor repeatedAccessor) {
    if (repeatedAccessor.isHyper()) {
      return new ArrayOffsetHyperVectorAccessor(repeatedAccessor);
    } else {
      RepeatedValueVector vector = repeatedAccessor.vector();
      return new SingleVectorAccessor(vector.getOffsetVector());
    }
  }

  public static VectorAccessor arrayDataAccessor(VectorAccessor repeatedAccessor) {
    if (repeatedAccessor.isHyper()) {
      return new ArrayDataHyperVectorAccessor(repeatedAccessor);
    } else {
      RepeatedValueVector vector = repeatedAccessor.vector();
      return new SingleVectorAccessor(
          vector.getDataVector());
    }
  }

  public static VectorAccessor varWidthOffsetVectorAccessor(VectorAccessor varWidthAccessor) {
    if (varWidthAccessor.isHyper()) {
      return new VarWidthOffsetHyperVectorAccessor(varWidthAccessor);
    } else {
      VariableWidthVector vector = varWidthAccessor.vector();
      return new SingleVectorAccessor(vector.getOffsetVector());
    }
  }

  public static VectorAccessor nullableValuesAccessor(VectorAccessor nullableAccessor) {
    if (nullableAccessor.isHyper()) {
      return new NullableValuesHyperVectorAccessor(nullableAccessor);
    } else {
      NullableVector vector = nullableAccessor.vector();
      return new SingleVectorAccessor(
          vector.getValuesVector());
    }
  }

  public static VectorAccessor nullableBitsAccessor(VectorAccessor nullableAccessor) {
    if (nullableAccessor.isHyper()) {
      return new NullableBitsHyperVectorStateReader(nullableAccessor);
    } else {
      NullableVector vector = nullableAccessor.vector();
      return new SingleVectorAccessor(
          vector.getBitsVector());
    }
  }

  public static VectorAccessor listBitsAccessor(VectorAccessor nullableAccessor) {
    if (nullableAccessor.isHyper()) {
      return new ListBitsHyperVectorStateReader(nullableAccessor);
    } else {
      ListVector vector = nullableAccessor.vector();
      return new SingleVectorAccessor(
          vector.getBitsVector());
    }
  }

  public static VectorAccessor memberHyperAccessor(VectorAccessor mapAccessor, int index, MajorType memberType) {
    if (mapAccessor.isHyper()) {
      return new HyperMemberVectorAccessor(mapAccessor, index, memberType);
    } else {
      AbstractMapVector vector = mapAccessor.vector();
      return new SingleVectorAccessor(
          vector.getChildByOrdinal(index));
    }
  }
}
