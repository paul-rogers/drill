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

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.UInt4Vector.Accessor;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.reader.AbstractArrayReader.ArrayObjectReader;
import org.apache.drill.exec.vector.accessor.reader.AbstractArrayReader.ElementReaderIndex;
import org.apache.drill.exec.vector.accessor.reader.ListReaderImpl.ListWrapper;
import org.apache.drill.exec.vector.accessor.reader.NullStateReader.BitsVectorStateReader;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.joda.time.Period;

/**
 * Implementation of a list vector over a single scalar vector. Causes the
 * list to appear identical to a scalar array, with the addition of per-element
 * null bits.
 */

public class ScalarListReader extends ScalarArrayReader {

  public static class ElementVectorAccessor implements VectorAccessor {

    private final VectorAccessor listAccessor;
    private ColumnReaderIndex index;

    public ElementVectorAccessor(VectorAccessor listAccessor) {
      this.listAccessor = listAccessor;
    }

    @Override
    public void bind(ColumnReaderIndex index) {
      this.index = index;
    }

    @Override
    public ValueVector vector() {
      ((ListVector) listAccessor.vector()).getDataVector();
    }

  }

  private static class ScalarListElementReader extends BaseElementReader {
    private final BaseScalarReader baseReader;

    public ScalarListElementReader(BaseScalarReader scalarReader) {
      this.baseReader = scalarReader;
      nullStateReader = new NullStateReader() {

        @Override
        public void bindIndex(ColumnReaderIndex rowIndex) { }

        @Override
        public void bindVector(ValueVector vector) { }

        @Override
        public void bindVectorAccessor(VectorAccessor va) { }

        @Override
        public boolean isNull() {
          return baseReader.isNull();
        }
      };
    }

    @Override
    public ValueType valueType() { return baseReader.valueType(); }

    @Override
    public void bindVector(ValueVector vector) {
      baseReader.bindVector(vector);
    }

    @Override
    public void bindVectorAccessor(MajorType majorType, VectorAccessor va) {
      super.bindVectorAccessor(majorType, va);
      baseReader.bindVectorAccessor(majorType, va);
    }

    @Override
    public void bindIndex(ColumnReaderIndex rowIndex) {
      super.bindIndex(rowIndex);
      baseReader.bindIndex(rowIndex);
    }

    @Override
    public boolean isNull(int index) {
      setPosn(index);
      return nullStateReader.isNull();
    }

    @Override
    public int getInt(int index) {
      setPosn(index);
      return baseReader.getInt();
    }

    @Override
    public long getLong(int index) {
      setPosn(index);
      return baseReader.getLong();
    }

    @Override
    public double getDouble(int index) {
      setPosn(index);
      return baseReader.getDouble();
    }

    @Override
    public String getString(int index) {
      setPosn(index);
      return baseReader.getString();
    }

    @Override
    public byte[] getBytes(int index) {
      setPosn(index);
      return baseReader.getBytes();
    }

    @Override
    public BigDecimal getDecimal(int index) {
      setPosn(index);
      return baseReader.getDecimal();
    }

    @Override
    public Period getPeriod(int index) {
      setPosn(index);
      return baseReader.getPeriod();
    }
  }

  private final ListWrapper listWrapper;

  protected ScalarListReader(BaseElementReader elementReader) {
    super(elementReader);
    listWrapper = new ListWrapper(elementReader);
    nullStateReader = listWrapper.nullStateReader();
  }

  public static ArrayObjectReader buildRepeated(ListVector vector,
      BaseScalarReader baseElementReader) {
    ScalarListElementReader elementReader = new ScalarListElementReader(baseElementReader);
    elementReader.bindVector(vector.getDataVector());
    ScalarArrayReader arrayReader = new ScalarArrayReader(elementReader);
    arrayReader.bindVector(vector);
    return new ArrayObjectReader(arrayReader);
  }

  public static ArrayObjectReader buildHyperRepeated(
      MajorType majorType, VectorAccessor va,
      BaseScalarReader baseElementReader) {
    ScalarListElementReader elementReader = new ScalarListElementReader(baseElementReader);
    elementReader.bindVectorAccessor(
        Types.optional(majorType.getSubType(0)), va);
    ScalarArrayReader arrayReader = new ScalarArrayReader(elementReader);
    arrayReader.bindVectorAccessor(majorType, va);
    return new ArrayObjectReader(arrayReader);
  }

  @Override
  public void bindIndex(ColumnReaderIndex index) {
    super.bindIndex(index);
    listWrapper.bindIndex(elementIndex);
  }

  @Override
  public void bindVector(ValueVector vector) {
    listWrapper.bindVector(vector);
  }

  @Override
  public void bindVectorAccessor(MajorType majorType, VectorAccessor va) {
    super.bindVectorAccessor(majorType, va);
    listWrapper.bindVectorAccessor(va);
  }

  @Override
  public void setPosn(int index) {
    elementIndex.set(index);
    reposition();
  }
}
