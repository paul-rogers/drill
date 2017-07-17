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
package org.apache.drill.exec.vector.accessor2.impl;

import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ElementWriterIndex;
import org.apache.drill.exec.vector.accessor2.ArrayWriter;
import org.apache.drill.exec.vector.accessor2.ObjectWriter;
import org.apache.drill.exec.vector.accessor2.ScalarWriter;
import org.apache.drill.exec.vector.accessor2.TupleWriter;
import org.apache.drill.exec.vector.accessor2.ObjectWriter.ObjectType;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

public abstract class AbstractArrayWriterImpl implements ArrayWriter, WriterEvents {

  public static class ArrayObjectWriter extends AbstractObjectWriter {

    private AbstractArrayWriterImpl arrayWriter;

    public ArrayObjectWriter(AbstractArrayWriterImpl arrayWriter) {
      this.arrayWriter = arrayWriter;
    }

    @Override
    public ObjectType type() {
      return ObjectType.ARRAY;
    }

    @Override
    public void set(Object value) throws VectorOverflowException {
      arrayWriter.setArray(value);
    }

    public void start() {
      arrayWriter.startWrite();
    }

    @Override
    public ArrayWriter array() {
      return arrayWriter;
    }

    @Override
    public void startWrite() {
      arrayWriter.startWrite();
    }

    @Override
    public void startRow() {
      arrayWriter.startRow();
    }

    @Override
    public void endRow() {
      arrayWriter.endRow();
    }

    @Override
    public void endWrite() throws VectorOverflowException {
      arrayWriter.endWrite();
    }
  }

  private final ColumnWriterIndex baseIndex;
  private final FixedWidthElementIndex elementIndex;
  private final AbstractObjectWriter elementObjWriter;
  private final UInt4Vector.Mutator mutator;
  private int lastWritePosn = 0;

  public AbstractArrayWriterImpl(ColumnWriterIndex baseIndex, RepeatedValueVector vector, AbstractObjectWriter elementObjWriter) {
    this.elementObjWriter = elementObjWriter;
    this.baseIndex = baseIndex;
    elementIndex = new FixedWidthElementIndex(baseIndex);
    mutator = vector.getOffsetVector().getMutator();
  }

  protected ElementWriterIndex elementIndex() { return elementIndex; }


  @Override
  public int size() {
    return elementIndex.arraySize();
  }

  private void setOffset(int posn, int offset) {
    mutator.setSafe(posn, offset);
  }

  @Override
  public ObjectWriter entry() {
    return elementObjWriter;
  }

  @Override
  public void startWrite() {
    elementIndex.reset();
    setOffset(0, 0);
  }

  @Override
  public void startRow() { fillEmpties(); }

  private void fillEmpties() {
    final int curPosn = elementIndex.vectorIndex();
    while (lastWritePosn < baseIndex.vectorIndex()) {
      lastWritePosn++;
      setOffset(lastWritePosn, curPosn);
    }
  }

  @Override
  public void endRow() {
    assert lastWritePosn == baseIndex.vectorIndex();
    setOffset(lastWritePosn + 1, elementIndex.vectorIndex());
  }

  @Override
  public void endWrite() {
    fillEmpties();
    mutator.setValueCount(elementIndex.vectorIndex());
  }

  @Override
  public ObjectType entryType() {
    return elementObjWriter.type();
  }

  @Override
  public ScalarWriter scalar() {
    return elementObjWriter.scalar();
  }

  @Override
  public TupleWriter tuple() {
    return elementObjWriter.tuple();
  }

  @Override
  public ArrayWriter array() {
    return elementObjWriter.array();
  }
}
