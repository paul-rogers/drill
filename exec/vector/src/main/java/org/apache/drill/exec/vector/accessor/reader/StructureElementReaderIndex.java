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

/**
 * Index into the vector of elements for a repeated vector.
 * Keeps track of the current offset in terms of value positions.
 */

public class StructureElementReaderIndex implements ColumnReaderIndex {

  private final ColumnReaderIndex base;
  private int startOffset;
  private int posn;
  private int length;

  public StructureElementReaderIndex(ColumnReaderIndex base) {
    this.base = base;
  }

  @Override
  public int batchIndex() {
    return base.batchIndex();
  }

  @Override
  public int vectorIndex() {
    return startOffset + posn;
  }

  public void reset(int startOffset, int length) {
    this.startOffset = startOffset;
    this.length = length;
    posn = 0;
  }

  public void set(int index) {
    if (index < 0 ||  length <= index) {
      throw new IndexOutOfBoundsException("Index = " + index + ", length = " + length);
    }
    posn = index;
  }

  public int size() { return length; }
  public int posn() { return posn; }
}
