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

import org.apache.drill.exec.vector.UInt4Vector.Accessor;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
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
  }

//  /**
//   * Column reader that provides access to an array column by returning a
//   * separate reader specifically for that array. That is, reading a column
//   * is a two-part process:<pre><code>
//   * tupleReader.column("arrayCol").array().getInt(2);</code></pre>
//   * This pattern is used to avoid overloading the column reader with
//   * both scalar and array access. Also, this pattern mimics the way
//   * that nested tuples (Drill maps) are handled.
//   */
//
//  public static class ArrayColumnReader extends AbstractScalarReader {
//
//    private final AbstractArrayReader arrayReader;
//
//    public ArrayColumnReader(AbstractArrayReader arrayReader) {
//      this.arrayReader = arrayReader;
//    }
//
//    @Override
//    public ValueType valueType() {
//       return ValueType.ARRAY;
//    }
//
//    @Override
//    public void bind(ColumnReaderIndex rowIndex, ValueVector vector) {
//      arrayReader.bind(rowIndex, vector);
//      vectorIndex = rowIndex;
//    }
//
//    @Override
//    public ArrayReader array() {
//      return arrayReader;
//    }
//  }

  public static class BaseElementIndex {
    private final ColumnReaderIndex base;
    private int startOffset;
    private int length;

    public BaseElementIndex(ColumnReaderIndex base) {
      this.base = base;
    }

    public int batchIndex() {
      return base.batchIndex();
    }

    public void reset(int startOffset, int length) {
      this.startOffset = startOffset;
      this.length = length;
    }

    public int size() { return length; }

    public int elementIndex(int index) {
      if (index < 0 ||  length <= index) {
        throw new IndexOutOfBoundsException("Index = " + index + ", length = " + length);
      }
      return startOffset + index;
    }
  }

  protected final ColumnReaderIndex baseIndex;
  protected final BaseElementIndex baseElementIndex;
  private Accessor accessor;

  public AbstractArrayReader(ColumnReaderIndex baseIndex, RepeatedValueVector vector, BaseElementIndex baseElementIndex) {
    this.baseIndex = baseIndex;
    accessor = vector.getOffsetVector().getAccessor();
    this.baseElementIndex = baseElementIndex;
  }

  public void reposition() {
    final int index = baseIndex.vectorIndex();
    final int startPosn = accessor.get(index);
    baseElementIndex.reset(startPosn, accessor.get(index + 1) - startPosn);
  }

  @Override
  public int size() { return baseElementIndex.size(); }

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
    throw new UnsupportedOperationException();
//    elementIndex.set(index);
//    return entry(index).tuple();
  }

  @Override
  public ArrayReader array(int index) {
    throw new UnsupportedOperationException();
//    elementIndex.set(index);
//    return entry(index).array();
  }
}
