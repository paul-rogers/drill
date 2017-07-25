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
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

public class ScalarArrayReader extends AbstractArrayReader {

  private final BaseElementReader elementReader;

  private ScalarArrayReader(ColumnReaderIndex baseIndex,
                           RepeatedValueVector vector,
                           BaseElementIndex elementIndex,
                           BaseElementReader elementReader) {
    super(baseIndex, vector, elementIndex);
    this.elementReader = elementReader;
  }

  public static ArrayObjectReader build(ColumnReaderIndex baseIndex,
                                        RepeatedValueVector vector,
                                        BaseElementReader elementReader) {
    FixedWidthElementReaderIndex elementIndex = new FixedWidthElementReaderIndex(baseIndex);
    elementReader.bind(elementIndex, vector.getDataVector());
    return new ArrayObjectReader(new ScalarArrayReader(baseIndex, vector, elementIndex, elementReader));
  }

  @Override
  public ObjectType entryType() {
    return ObjectType.SCALAR;
  }

  @Override
  public ScalarElementReader elements() {
    return elementReader;
  }

  @Override
  public Object getObject() {
    List<Object> elements = new ArrayList<>();
    for (int i = 0; i < size(); i++) {
      elements.add(elementReader.getObject(i));
    }
    return elements;
  }

  @Override
  public String getAsString() {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    for (int i = 0; i < size(); i++) {
      if (i > 0) {
        buf.append( ", " );
      }
      buf.append(elementReader.getAsString(i));
    }
    buf.append("]");
    return buf.toString();
  }
}
