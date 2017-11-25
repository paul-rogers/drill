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

import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.VariantReader;

/**
 * Reader for an array-valued column. This reader provides access to specific
 * array members via an array index. This is an abstract base class;
 * subclasses are generated for each repeated value vector type.
 */

public abstract class AbstractArrayReader implements ArrayReader, ReaderEvents {

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

    @Override
    protected ReaderEvents events() { return arrayReader; }
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
    protected int position;

    public ElementReaderIndex(ColumnReaderIndex base) {
      this.base = base;
    }

    @Override
    public int hyperVectorIndex() { return 0; }

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
      position = -1;
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

    @Override
    public int offset() {
      if (position < 0 || length <= position) {
        throw new IndexOutOfBoundsException("Index = " + position + ", length = " + length);
      }
      return startOffset + position;
    }

    @Override
    public boolean next() {
      if (hasNext()) {
        position++;
        return true;
      }
      return false;
    }

    /**
     * Set the current iterator location to the given index offset.
     *
     * @param index 0-based index into the current array
     */

    public void set(int index) {
      if (index < 0 ||  length < index) {
        throw new IndexOutOfBoundsException("Index = " + index + ", length = " + length);
      }

      // Auto-increment, so set the index to one before the target.

      position = index - 1;
    }

    @Override
    public int logicalIndex() { return position; }

    @Override
    public boolean hasNext() {
      return position < length - 1;
    }

    @Override
    public int nextOffset() { return offset(); }
  }

  private final VectorAccessor arrayAccessor;
  private final OffsetVectorReader offsetReader;
  protected ColumnReaderIndex baseIndex;
  protected ElementReaderIndex elementIndex;
  protected NullStateReader nullStateReader;

  public AbstractArrayReader(VectorAccessor va) {
    arrayAccessor = va;
    offsetReader = new OffsetVectorReader(
        VectorAccessors.arrayOffsetVectorAccessor(va));
  }

  @Override
  public void bindIndex(ColumnReaderIndex index) {
    baseIndex = index;
    arrayAccessor.bind(index);
    offsetReader.bindIndex(index);
  }

  @Override
  public void bindNullState(NullStateReader nullStateReader) {
    this.nullStateReader = nullStateReader;
  }

  @Override
  public NullStateReader nullStateReader() { return nullStateReader; }

  @Override
  public boolean isNull() { return nullStateReader.isNull(); }

  public void reposition() {
    long entry = offsetReader.getEntry();
    elementIndex.reset((int) (entry >> 32), (int) (entry & 0xFFFF_FFFF));
  }

  @Override
  public boolean hasNext() { return elementIndex.hasNext(); }

  @Override
  public int size() { return elementIndex.size(); }

  @Override
  public void setPosn(int posn) { elementIndex.set(posn); }

  @Override
  public ObjectReader entry() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScalarReader scalar() {
    return entry().scalar();
  }

  @Override
  public TupleReader tuple() {
    return entry().tuple();
  }

  @Override
  public ArrayReader array() {
    return entry().array();
  }

  @Override
  public VariantReader variant() {
    return entry().variant();
  }
}
