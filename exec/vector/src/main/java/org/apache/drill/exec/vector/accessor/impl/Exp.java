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
package org.apache.drill.exec.vector.accessor.impl;

import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VectorOverflowException;

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

public class Exp {

  public interface VectorFacade {

  }

  public interface ConcreteVectorFacade extends VectorFacade {

    DrillBuf buffer();

    void writeBytes(int index, byte bytes[], int start, int length) throws VectorOverflowException;

    byte[] readBytes(int index);
    void readBytes(int index, byte[] buffer);
  }

  public interface IntPayload {

    void writeInt(int index, int value);
    int readInt(int index);
  }

  public class BaseVectorFacade implements ConcreteVectorFacade {

    protected final BaseDataValueVector vector;

    public BaseVectorFacade(BaseDataValueVector vector) {
      this.vector = vector;
    }

    @Override
    public DrillBuf buffer() {
      return vector.getBuffer();
    }

    @Override
    public void writeBytes(int index, byte[] bytes, int start, int length)
        throws VectorOverflowException {
      // TODO Auto-generated method stub

    }

    @Override
    public byte[] readBytes(int index) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void readBytes(int index, byte[] buffer) {
      // TODO Auto-generated method stub

    }

  }

  public class BaseFixedWidthVectorFacade extends BaseVectorFacade {

    private final int valueWidth;

    public BaseFixedWidthVectorFacade(BaseDataValueVector vector, int valueWidth) {
      super(vector);
      this.valueWidth = valueWidth;
    }

    protected long addr(int index) {
      int offset = index * valueWidth;
      DrillBuf buf = vector.getBuffer();
      assert false; // buf.check(offset, valueWidth);
      return 0; // buf.addr(offset);
    }

  }

  public class IntVectorFacade extends BaseFixedWidthVectorFacade implements IntPayload {

    public IntVectorFacade(IntVector vector) {
      super(vector, IntVector.VALUE_WIDTH);
    }

    @Override
    public void writeInt(int index, int value) {
      PlatformDependent.putInt(addr(index), value);
    }

    @Override
    public int readInt(int index) {
      return PlatformDependent.getInt(addr(index));
    }
  }

  public class UInt4VectorFacade extends BaseFixedWidthVectorFacade implements IntPayload {

    public UInt4VectorFacade(UInt4Vector vector) {
      super(vector, UInt4Vector.VALUE_WIDTH);
    }

    @Override
    public void writeInt(int index, int value) {
      PlatformDependent.putInt(addr(index), value);
    }

    @Override
    public int readInt(int index) {
      return PlatformDependent.getInt(addr(index));
    }
  }

  public class IndirectionVectorFacade extends UInt4VectorFacade {

    public IndirectionVectorFacade(UInt4Vector vector) {
      super(vector);
    }

    public int sizeAt(int index) {
      return readInt(index + 1) - readInt(index);
    }
  }

  public class ByteVectorFacade extends BaseVectorFacade {

    public ByteVectorFacade(BaseDataValueVector vector) {
      super(vector);
    }

  }

  public interface ArrayPayload {
    int arraySize(int index);
  }

  public class VarCharVectorFacade implements VectorFacade, ArrayPayload {

    private final IndirectionVectorFacade indirectionFacade;
    private final ByteVectorFacade dataFacade;

    public VarCharVectorFacade(VarCharVector vector) {
      indirectionFacade = new IndirectionVectorFacade(vector.getOffsetVector());
      dataFacade = new ByteVectorFacade(vector);
    }

    public int writeBytes(int index, int dataIndex, byte[] bytes, int start, int length)
        throws VectorOverflowException {
      dataFacade.writeBytes(dataIndex, bytes, start, length);
      int nextOffset = dataIndex + length;
      indirectionFacade.writeInt(index + 1, nextOffset);
      return nextOffset;
    }

    public void fillEmpties(int fromIndex, int toIndex, int nextDataIndex) {
      for (int index = fromIndex; index < toIndex; index++) {
        indirectionFacade.writeInt(index + 1, nextDataIndex);
      }
    }

    @Override
    public int arraySize(int index) {
      return indirectionFacade.sizeAt(index);
    }
  }

  public class RepeatedVarCharFacade implements VectorFacade, ArrayPayload {

    private final IndirectionVectorFacade indirectionFacade;
    private final VarCharVectorFacade dataFacade;

    public RepeatedVarCharFacade(RepeatedVarCharVector vector) {
      indirectionFacade = new IndirectionVectorFacade(vector.getOffsetVector());
      dataFacade = new VarCharVectorFacade(vector.getDataVector());
    }

    @Override
    public int arraySize(int index) {
      return indirectionFacade.sizeAt(index);
    }

    public int writeBytes(int rowIndex, int arrayIndex, int dataIndex, byte[] bytes, int start, int length)
        throws VectorOverflowException {
      return dataFacade.writeBytes(arrayIndex, dataIndex, bytes, start, length);
    }

    public void setArraySize(int rowIndex, int nextArrayIndex) {
      indirectionFacade.writeInt(rowIndex + 1, nextArrayIndex);
    }

    public void fillEmpties(int fromIndex, int toIndex, int nextArrayIndex) {
      for (int index = fromIndex; index < toIndex; index++) {
        indirectionFacade.writeInt(index + 1, nextArrayIndex);
      }
    }
  }

  public static class VectorConversions {

  }

}
