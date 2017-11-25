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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarReader;

public class ScalarArrayReader extends AbstractArrayReader {

  public static class ScalarElementIndex extends ElementReaderIndex {

    public ScalarElementIndex(ColumnReaderIndex base) {
      super(base);
    }

    @Override
    public int nextOffset() {
      if (! next()) {
        throw new IllegalStateException();
      }
      return offset();
    }
  }

  private final BaseScalarReader elementReader;

  private ScalarArrayReader(VectorAccessor va,
      BaseScalarReader elementReader) {
    super(va);
    this.elementReader = elementReader;
  }

  public static ArrayObjectReader build(VectorAccessor va,
      BaseScalarReader elementReader) {
    elementReader.bindVector(VectorAccessors.arrayDataAccessor(va));
    elementReader.bindNullState(NullStateReader.REQUIRED_STATE_READER);
    ScalarArrayReader arrayReader = new ScalarArrayReader(va, elementReader);
    arrayReader.bindNullState(NullStateReader.REQUIRED_STATE_READER);
    return new ArrayObjectReader(arrayReader);
  }

  @Override
  public void bindIndex(ColumnReaderIndex index) {
    super.bindIndex(index);
    elementIndex = new ScalarElementIndex(baseIndex);
    elementReader.bindIndex(elementIndex);
  }

  @Override
  public ObjectType entryType() {
    return ObjectType.SCALAR;
  }

  @Override
  public ScalarReader scalar() {
    return elementReader;
  }

  @Override
  public boolean next() {
    throw new IllegalStateException("next() not supported for scalar arrays");
  }

  @Override
  public Object getObject() {
    // Simple: return elements as an object list.
    // If really needed, could return as a typed array, but that
    // is a bit of a hassle.

    setPosn(0);
    List<Object> elements = new ArrayList<>();
    while (hasNext()) {
      elements.add(elementReader.getObject());
    }
    return elements;
  }

  @Override
  public String getAsString() {
    setPosn(0);
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    int i = 0;
    while (hasNext()) {
      if (i++ > 0) {
        buf.append( ", " );
      }
      buf.append(elementReader.getAsString());
    }
    buf.append("]");
    return buf.toString();
  }
}
