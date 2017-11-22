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

import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.UInt4Vector.Accessor;
import org.apache.drill.exec.vector.accessor.ColumnAccessors.UInt1ColumnReader;
import org.apache.drill.exec.vector.accessor.reader.NullStateReader.BitsVectorStateReader;
import org.apache.drill.exec.vector.accessor.reader.UnionReaderImpl.MemberNullStateReader;
import org.apache.drill.exec.vector.complex.ListVector;

/**
 * The list vector is a complex, somewhat ad-hoc structure. It can
 * take the place of repeated vectors, with some extra features.
 * The four "modes" of list vector, and thus list reader, are:
 * <ul>
 * <li>Similar to a scalar array.</li>
 * <li>Similar to a map (tuple) array.</li>
 * <li>The only way to represent an array of unions.</li>
 * <li>The only way to represent an array of lists.</li>
 * </ul>
 * Lists add an extra feature compared to the "regular" scalar or
 * map arrays. Each array entry can be either null or empty (regular
 * arrays can only be empty.)
 * <p>
 * When working with unions, this introduces an ambiguity: both the
 * list and the union have a null flag. Here, we assume that the
 * list flag has precedence, and that if the list entry is not null
 * then the union must also be not null. (Experience will show whether
 * existing code does, in fact, follow that convention.)
 */

public class ListReaderImpl extends ObjectArrayReader {

  /**
   * List implementation for a list with a single type. Clients can treat
   * such a list as the equivalent of a repeated type, but with the
   * addition of a per-element null flag.
   */

  public static class ScalarListReader extends ListReaderImpl {

    private SimpleListReader(ListVector vector, BaseElementReader elements) {
      super(vector, elementReader);
    }
  }

//  /**
//   * List implementation for a list with multiple types. The list is
//   * presented as an array of unions.
//   */
//
//  public static class UnionListReader extends ListReaderImpl {
//
//    private UnionListReader(ListVector vector, AbstractObjectReader elementReader) {
//      super(vector, elementReader);
//    }
//  }

  public static class ListWrapper {
    private final UInt1ColumnReader bitsReader;
    private final NullStateReader nullStateReader;
    private final ReaderEvents elementReader;
    private ColumnReaderIndex baseIndex;
    private ElementReaderIndex elementIndex;

    public ListWrapper(ReaderEvents elementReader) {
      bitsReader = new UInt1ColumnReader();
      bitsReader.bindNullState(NullStateReader.REQUIRED_STATE_READER);
      nullStateReader = new BitsVectorStateReader(bitsReader);
      this.elementReader = elementReader;
    }

    public void bindVector(ValueVector vector) {
      ListVector listVector = (ListVector) vector;
      bitsReader.bindVector(listVector.getBitsVector());
      rebindMemberNullState();
    }

    public void bindVectorAccessor(VectorAccessor va) {
      bitsReader.bindVectorAccessor(null, va);
      nullStateReader.bindVectorAccessor(va);
      rebindMemberNullState();
    }

    /**
     * Replace the null state reader in the child with one
     * that does a check of the list's null state as well
     * as the element's null state. Required because lists have a null vector,
     * as does the union in the list, as do the members of the union.
     * Yes, a bit of a mess.
     */

    private void rebindMemberNullState() {
      elementReader.bindNullState(
          new MemberNullStateReader(nullStateReader,
              elementReader.nullStateReader()));
    }

    public void bindIndex(ColumnReaderIndex elementIndex) {
      bitsReader.bindIndex(elementIndex);
    }

    public NullStateReader nullStateReader() {
      return nullStateReader;
    }

    public boolean isNull() {
      return bitsReader.getInt() == 0;
    }

    public void reposition() {

      // All F***ed up: how to get the offset?
      // How to get the vector?
      final int index = baseIndex.vectorIndex();
      final Accessor curAccesssor = accessor();
      final int startPosn = curAccesssor.get(index);
      elementIndex.reset(startPosn, curAccesssor.get(index + 1) - startPosn);
    }
  }

  private final ListWrapper listWrapper;

  private ListReaderImpl(AbstractObjectReader elementReader) {
    super(elementReader);
    listWrapper = new ListWrapper(elementReader.events());
    nullStateReader = listWrapper.nullStateReader;
  }

  public static ArrayObjectReader build(ListVector vector,
      AbstractObjectReader elementReader) {
    ListReaderImpl listReader = new ListReaderImpl(elementReader);
    listReader.bindVector(vector);
    return new ArrayObjectReader(listReader);
  }

  @Override
  public void bindVector(ValueVector vector) {
    listWrapper.bindVector(vector);
  }

  @Override
  public void bindVectorAccessor(MajorType majorType, VectorAccessor va) {
    listWrapper.bindVectorAccessor(va);
  }

  @Override
  public void bindIndex(ColumnReaderIndex index) {
    super.bindIndex(index);
    listWrapper.bindIndex(elementIndex);
  }
}
