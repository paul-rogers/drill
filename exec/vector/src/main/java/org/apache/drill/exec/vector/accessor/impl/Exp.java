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

import org.apache.drill.exec.vector.IntVector;

public class Exp {

  //

  public interface AccessIndex {
    int index();
    int vectorIndex();
    boolean valid();
  }

  public interface ReadIndex extends AccessIndex {
    boolean next();
    void seek(int posn);
  }

  public interface WriteIndex extends AccessIndex {
    void commit();
    void rollback();
    void close();
  }

  public static class DirectRowReadIndex implements ReadIndex {

    private final int rowCount;
    private int rowIndex = -1;

    public DirectRowReadIndex(int rowCount) {
      this.rowCount = rowCount;
    }

    @Override
    public int index() {
      return rowIndex;
    }

    @Override
    public int vectorIndex() {
      return rowIndex;
    }

    @Override
    public boolean valid() {
      return 0 <= rowIndex && rowIndex < rowCount;
    }

    @Override
    public boolean next() {
      if (rowIndex < rowCount) {
        rowIndex++;
      }
      return rowIndex < rowCount;
    }

    @Override
    public void seek(int posn) {
      posn = Math.max(-1, Math.min(rowCount, posn));
    }
  }

  public abstract static class RowWriteIndex implements WriteIndex {

    private final int rowLimit;
    private int rowIndex = 0;

    public RowWriteIndex(int rowLimit) {
      this.rowLimit = rowLimit;
    }

    @Override
    public int index() {
      return rowIndex;
    }

    @Override
    public int vectorIndex() {
      return rowIndex;
    }

    @Override
    public boolean valid() {
      return rowIndex < rowLimit;
    }

    @Override
    public void commit() {
      assert valid();
      rowIndex++;
    }

    @Override
    public void rollback() {
    }
  }

  public abstract class AbstractArrayIndex implements AccessIndex {
    protected final IntVector offsetVector;
    // Pointer into the offset vector for the current array
    protected int offsetPosn;
    // Pointer into the array values for first array element
    protected int vectorPosn;
    // Zero-based index into the current array
    protected int arrayIndex;

    public AbstractArrayIndex(IntVector offsetVector) {
      this.offsetVector = offsetVector;
    }

    @Override
    public int index() {
      return offsetPosn;
    }

    @Override
    public int vectorIndex() {
      return vectorPosn + arrayIndex;
    }
  }

  public class ArrayReadIndex extends AbstractArrayIndex implements ReadIndex {
    private final IntVector.Accessor accessor;
    private int count;

    public ArrayReadIndex(IntVector offsetVector) {
      super(offsetVector);
      accessor = offsetVector.getAccessor();
    }

    public void seek(int element) {
      offsetPosn = element;
      vectorPosn = accessor.get(element);
      count = accessor.get(element + 1) - vectorPosn;
    }

    @Override
    public boolean valid() {
      return arrayIndex < count;
    }

    @Override
    public boolean next() {
      if (valid()) {
        arrayIndex++;
      }
      return valid();
    }
  }

  public class ArrayWriteIndex extends AbstractArrayIndex implements WriteIndex {

    private final IntVector.Mutator mutator;
    private int sizeLimit;

    public ArrayWriteIndex(IntVector offsetVector) {
      super(offsetVector);
      mutator = offsetVector.getMutator();
    }

    @Override
    public boolean valid() {
      return arrayIndex < sizeLimit;
    }

    @Override
    public void commit() {
      arrayIndex++;
    }

    @Override
    public void rollback() {
    }

    @Override
    public void close() {
      mutator.set(++offsetPosn, vectorPosn + arrayIndex);
      vectorPosn += arrayIndex;
      arrayIndex = 0;
    }
  }

  public interface AccessSet {

  }
}
