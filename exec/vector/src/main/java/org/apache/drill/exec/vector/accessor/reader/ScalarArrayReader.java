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
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

public class ScalarArrayReader extends AbstractArrayReader {

  private final BaseElementReader elementReader;

  public ScalarArrayReader(ColumnReaderIndex baseIndex,
                           RepeatedValueVector vector,
                           BaseElementReader elementReader) {
    super(baseIndex, vector, new FixedWidthElementReaderIndex(baseIndex));
    this.elementReader = elementReader;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getAsString() {
    // TODO Auto-generated method stub
    return null;
  }

}
