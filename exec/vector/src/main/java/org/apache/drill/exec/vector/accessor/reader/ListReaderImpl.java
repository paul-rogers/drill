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
import org.apache.drill.exec.record.ColumnMetadata.StructureType;
import org.apache.drill.exec.vector.accessor.ColumnAccessors.UInt1ColumnReader;
import org.apache.drill.exec.vector.accessor.reader.AbstractArrayReader.ArrayObjectReader;
import org.apache.drill.exec.vector.accessor.reader.AbstractArrayReader.ElementReaderIndex;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

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

public abstract class ListReaderImpl extends ObjectArrayReader {

  /**
   * List implementation for a list with a single type. Clients can treat
   * such a list as the equivalent of a repeated type, but with the
   * addition of a per-element null flag.
   */

  public static class SimpleListReader extends ListReaderImpl {

    private SimpleListReader(ListVector vector, AbstractObjectReader elementReader) {
      super(vector, elementReader);
    }
  }

  /**
   * List implementation for a list with multiple types. The list is
   * presented as an array of unions.
   */

  public static class UnionListReader extends ListReaderImpl {

    private UnionListReader(ListVector vector, AbstractObjectReader elementReader) {
      super(vector, elementReader);
    }

  }

  private final ListVector listVector;
  private final UInt1ColumnReader bitsReader;

  private ListReaderImpl(ListVector vector, AbstractObjectReader elementReader) {
    super(vector, elementReader);
    listVector = vector;
    bitsReader = new UInt1ColumnReader(vector.getBitsVector());
  }

  public static ArrayObjectReader build(ListVector vector,
      AbstractObjectReader elementReader) {
    ListReaderImpl listReader;
    if (elementReader.type() == ObjectType.VARIANT) {
      listReader = new UnionListReader(vector, elementReader);
    } else {
      listReader = new SimpleListReader(vector, elementReader);
    }
    return new ArrayObjectReader(listReader);
  }

  @Override
  public void bindIndex(ColumnReaderIndex index) {
    super.bindIndex(index);
    bitsReader.bindIndex(elementIndex);
  }

  @Override
  public boolean isNull() {
    return bitsReader.getInt() == 0;
  }

}
