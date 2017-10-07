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
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Reader for an array of either tuples or other arrays.
 */

public class ObjectArrayReader extends AbstractArrayReader {

  /**
   * Reader for each element.
   */

  private final AbstractObjectReader elementReader;

  private ObjectArrayReader(RepeatedValueVector vector, AbstractObjectReader elementReader) {
    super(vector);
    this.elementReader = elementReader;
  }

  private ObjectArrayReader(VectorAccessor vectorAccessor, AbstractObjectReader elementReader) {
    super(vectorAccessor);
    this.elementReader = elementReader;
  }

  public static ArrayObjectReader build(RepeatedValueVector vector,
                                        AbstractObjectReader elementReader) {
    return new ArrayObjectReader(
        new ObjectArrayReader(vector, elementReader));
  }

  public static ArrayObjectReader buildTable(RepeatedValueVector vector,
      AbstractObjectReader elementReader) {
    return new ArrayObjectReader(
            new ObjectArrayReader(vector, elementReader));
  }

  public static AbstractObjectReader build(VectorAccessor vectorAccessor,
                                           AbstractObjectReader elementReader) {
    return new ArrayObjectReader(
        new ObjectArrayReader(vectorAccessor, elementReader));
  }

  @Override
  public void bindIndex(ColumnReaderIndex index) {
    super.bindIndex(index);
    elementIndex = new ElementReaderIndex(baseIndex);
    elementReader.bindIndex(elementIndex);
  }

  @Override
  public ObjectType entryType() {
    return elementReader.type();
  }

  @Override
  public void setPosn(int index) {
    elementIndex.set(index);
    elementReader.reposition();
  }

  @Override
  public boolean next() {
    if (! elementIndex.next()) {
      return false;
    }
    elementReader.reposition();
    return true;
  }

  @Override
  public ObjectReader entry() {
    return elementReader;
  }

  @Override
  public ObjectReader entry(int index) {
    setPosn(index);
    return entry();
  }

  @Override
  public Object getObject() {
    List<Object> array = new ArrayList<>();
    for (int i = 0; i < elementIndex.size(); i++) {
      array.add(entry(i).getObject());
    }
    return array;
  }

  public ColumnReaderIndex elementIndex() {
    return elementIndex;
  }

  @Override
  public String getAsString() {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    for (int i = 0; i < size(); i++) {
      if (i > 0) {
        buf.append( ", " );
      }
      buf.append(entry(i).getAsString());
    }
    buf.append("]");
    return buf.toString();
  }
}
