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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Writer for an array of either a map or another array. Here, the contents
 * are a structure and need explicit saves. State transitions in addition to the
 * base class are:
 *
 * <table border=1>
 * <tr><th>Public API</th><th>Array Event</th><th>Offset Event</th><th>Element Event</th></tr>
 * <tr><td>save() (array)</td>
 *     <td>saveValue()</td>
 *     <td>saveValue()</td>
 *     <td>saveValue()</td></tr>
 * </table>
 */

public class ObjectArrayWriter extends AbstractArrayWriter {

  private ObjectArrayWriter(RepeatedValueVector vector, AbstractObjectWriter elementWriter) {
    super(vector, elementWriter);
  }

  public static ArrayObjectWriter build(RepeatedValueVector vector, AbstractObjectWriter elementWriter) {
    return new ArrayObjectWriter(
        new ObjectArrayWriter(vector, elementWriter));
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    elementIndex = new ArrayElementWriterIndex(index);
    super.bindIndex(index);
  }

  @Override
  public void save() {
    elementIndex.nextElement();
    saveValue();

    // The end value above is for the "outer" value: the array
    // as a whole. Here, we end the value for the "inner" value:
    // the array elements.

    elementObjWriter.saveValue();
  }

  @Override
  public void set(Object... values) {
    setObject(values);
  }

  @Override
  public void setObject(Object array) {
    Object values[] = (Object[]) array;
    for (int i = 0; i < values.length; i++) {
      elementObjWriter.set(values[i]);
      save();
    }
  }

  @Override
  public void startWriteAt(int index) {
    assert false;
  }

  @Override
  public int lastWriteIndex() {
    // Undefined for arrays
    return 0;
  }
}
