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

/**
 * Writer for an array of either a map or another array.
 */

public class ObjectArrayWriter extends AbstractArrayWriter {

  private ObjectArrayWriter(AbstractObjectWriter elementWriter) {
    super(elementWriter);
  }

  public static ArrayObjectWriter build(AbstractObjectWriter elementWriter) {
    return new ArrayObjectWriter(
        new ObjectArrayWriter(elementWriter));
  }


  @Override
  public void save() {
    elementIndex.nextElement();
    endValue();
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
  public void reset(int index) {
    assert false;
  }

  @Override
  public int lastWriteIndex() {
    // TODO Auto-generated method stub
    return 0;
  }
}
