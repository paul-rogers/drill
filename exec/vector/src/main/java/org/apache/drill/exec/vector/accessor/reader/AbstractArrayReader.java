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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.UInt4Vector.Accessor;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor.SingleVectorAccessor;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Reader for an array-valued column. This reader provides access to specific
 * array members via an array index. This is an abstract base class;
 * subclasses are generated for each repeated value vector type.
 */

public abstract class AbstractArrayReader implements ArrayReader {

  /**
   * Object representation of an array reader.
   */

  public static class ArrayObjectReader extends AbstractObjectReader {

    private AbstractArrayReader arrayReader;

    public ArrayObjectReader(AbstractArrayReader arrayReader) {
      this.arrayReader = arrayReader;
    }

    @Override
    public void bindIndex(ColumnReaderIndex index) {
      arrayReader.bindIndex(index);
    }

    @Override
    public ObjectType type() {
      return ObjectType.ARRAY;
    }

    @Override
    public ArrayReader array() {
      return arrayReader;
    }

    @Override
    public ScalarElementReader elements() {
      return arrayReader.elements();
    }

    @Override
    public Object getObject() {
      return arrayReader.getObject();
    }

    @Override
    public String getAsString() {
      return arrayReader.getAsString();
    }

    @Override
    public void reposition() {
      arrayReader.reposition();
    }
  }

  /**
   * Index into the vector of elements for a repeated vector.
   * Indexes elements relative to the array. That is, if an array
   * has five elements, the index here tracks elements 0..4.
   * The actual vector index is given as the start offset plus the
   * offset into the array.
   * <p>
   * Indexes allow random or sequential access. Random access is more
   * typical for scalar arrays, while sequential access can be more convenient
   * for tuple arrays.
   */

  public static class ElementReaderIndex implements ColumnReaderIndex {
    protected final ColumnReaderIndex base;
    protected int startOffset;
    protected int length;
    protected int posn;

    public ElementReaderIndex(ColumnReaderIndex base) {
      this.base = base;
    }

    @Override
    public int batchIndex() {
      return base.batchIndex();
    }

    /**
     * Reposition this array index for a new array given the array start
     * offset and length.
     *
     * @param startOffset first location within the array's
     * offset vector
     * @param length number of offset vector locations associated with this
     * array
     */

    public void reset(int startOffset, int length) {
      assert length >= 0;
      assert startOffset >= 0;
      this.startOffset = startOffset;
      this.length = length;
      posn = -1;
    }

    @Override
    public int size() { return length; }

    /**
     * Given a 0-based index relative to the current array, return an absolute offset
     * vector location for the array value.
     *
     * @param index 0-based index into the current array
     * @return absolute offset vector location for the array value
     */

    public int vectorIndex(int index) {
      if (index < 0 || length <= index) {
        throw new IndexOutOfBoundsException("Index = " + index + ", length = " + length);
      }
      return startOffset + index;
    }

    @Override
    public boolean next() {
      if (++posn < length) {
        return true;
      }
      posn = length;
      return false;
    }

    /**
     * Set the current iterator location to the given index offset.
     *
     * @param index 0-based index into the current array
     */

    public void set(int index) {
      if (index < 0 ||  length <= index) {
        throw new IndexOutOfBoundsException("Index = " + index + ", length = " + length);
      }
      posn = index;
    }

    public int posn() { return posn; }

    @Override
    public int vectorIndex() {
      return vectorIndex(posn);
    }
  }

  private interface OffsetVectorAccessor {
    Accessor accessor();
  }

  private static class SingleOffsetVectorAccessor implements OffsetVectorAccessor {
    private final Accessor accessor;

    private SingleOffsetVectorAccessor(VectorAccessor va) {
      RepeatedValueVector vector = (RepeatedValueVector) va.vector();
      accessor = vector.getOffsetVector().getAccessor();
    }

    @Override
    public Accessor accessor() { return accessor; }
  }

  private static class HyperOffsetVectorAccessor implements OffsetVectorAccessor {

    private VectorAccessor repeatedVectorAccessor;

    private HyperOffsetVectorAccessor(VectorAccessor va) {
      repeatedVectorAccessor = va;

    }
    @Override
    public Accessor accessor() {
      return ((RepeatedValueVector) repeatedVectorAccessor.vector())
          .getOffsetVector().getAccessor();
    }
  }

  private static class HyperDataVectorAccessor implements VectorAccessor {

    private VectorAccessor repeatedVectorAccessor;

    private HyperDataVectorAccessor(VectorAccessor va) {
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
      return (T) ((RepeatedValueVector) repeatedVectorAccessor.vector()).getDataVector();
    }
  }

  private final OffsetVectorAccessor accessor;
  private final VectorAccessor vectorAccessor;
  protected ColumnReaderIndex baseIndex;
  protected ElementReaderIndex elementIndex;

  public AbstractArrayReader(VectorAccessor va) {
    if (va.isHyper()) {
      accessor = new HyperOffsetVectorAccessor(va);
    } else {
      accessor = new SingleOffsetVectorAccessor(va);
    }
    vectorAccessor = va;
  }

  public static VectorAccessor dataAccessor(VectorAccessor va) {
    if (va.isHyper()) {
      return new HyperDataVectorAccessor(va);
    } else {
      return new SingleVectorAccessor(
          ((RepeatedValueVector) va.vector()).getDataVector());
    }
  }

  public void bindIndex(ColumnReaderIndex index) {
    baseIndex = index;
    vectorAccessor.bind(index);
  }

  public void reposition() {
    final int index = baseIndex.vectorIndex();
    final Accessor curAccesssor = accessor.accessor();
    final int startPosn = curAccesssor.get(index);
    elementIndex.reset(startPosn, curAccesssor.get(index + 1) - startPosn);
  }

  @Override
  public int size() { return elementIndex.size(); }

  @Override
  public ScalarElementReader elements() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ObjectReader entry(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TupleReader tuple(int index) {
    return entry(index).tuple();
  }

  @Override
  public ArrayReader array(int index) {
    return entry(index).array();
  }

  @Override
  public ObjectReader entry() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TupleReader tuple() {
    return entry().tuple();
  }

  @Override
  public ArrayReader array() {
    return entry().array();
  }
}
