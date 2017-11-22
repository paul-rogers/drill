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
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ColumnAccessors.UInt1ColumnWriter;
import org.apache.drill.exec.vector.complex.ListVector;

public class ListWriterImpl extends ObjectArrayWriter {

  /**
   * For scalar arrays, incrementing the element index and
   * committing the current value is done automatically since
   * there is exactly one value per array element.
   */

  public class ScalarListElementWriterIndex extends ArrayElementWriterIndex {

    @Override
    public void nextElement() { next(); }
  }

  private final UInt1ColumnWriter isSetWriter;

  public ListWriterImpl(ListVector vector, AbstractObjectWriter memberWriter) {
    super(vector.getOffsetVector(), memberWriter);
    isSetWriter = new UInt1ColumnWriter(vector.getBitsVector());

    // If the member is a scalar, then simulate the auto-increment
    // functionality of a scalar array.

    if (memberWriter.type() == ObjectType.SCALAR) {
      elementIndex = new ScalarListElementWriterIndex();
    }
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    super.bindIndex(index);
    isSetWriter.bindIndex(index);
  }

  @Override
  public void setNull(boolean isNull) {
    isSetWriter.setInt(isNull ? 0 : 1);
  }

  @Override
  public void restartRow() {
    super.restartRow();
    isSetWriter.restartRow();
  }

  @Override
  public void preRollover() {
    super.preRollover();
    isSetWriter.preRollover();
  }

  @Override
  public void postRollover() {
    super.postRollover();
    isSetWriter.postRollover();
  }

  @Override
  public void startWrite() {
    super.startWrite();
    isSetWriter.startWrite();
  }

  @Override
  public void endArrayValue() {

    // Do the shim save first: it requires state which is reset
    // in the super call.

    if (elementIndex.arraySize() > 0) {
      setNull(false);
    }
    super.endArrayValue();
  }

  @Override
  public void endWrite() {
    isSetWriter.endWrite();
    super.endWrite();
  }
}
