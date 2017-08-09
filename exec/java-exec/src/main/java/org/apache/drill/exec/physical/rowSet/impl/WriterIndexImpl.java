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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;

/**
 * Writer index that points to each row in the row set. The index starts at
 * the 0th row and advances one row on each increment. This allows writers to
 * start positioned at the first row. Writes happen in the current row.
 * Calling <tt>next()</tt> advances to the next position, effectively saving
 * the current row. The most recent row can be abandoned easily simply by not
 * calling <tt>next()</tt>. This means that the number of completed rows is
 * the same as the row index.
 */

class WriterIndexImpl implements ColumnWriterIndex {

  private final int rowCountLimit;
  private int rowIndex = 0;

  public WriterIndexImpl() {
    this(ValueVector.MAX_ROW_COUNT);
  }

  public WriterIndexImpl(int rowCountLimit) {
    this.rowCountLimit = rowCountLimit;
  }

  @Override
  public int vectorIndex() { return rowIndex; }

  public boolean next() {
    if (++rowIndex < rowCountLimit) {
      return true;
    } else {
      // Should not call next() again once batch is full.
      rowIndex = rowCountLimit;
      return false;
    }
  }

  public int size() {
    // The index always points to the next slot past the
    // end of valid rows.
    return rowIndex;
  }

  public boolean valid() { return rowIndex < rowCountLimit; }

//  @Override
//  public void overflowed() {
//    state = State.VECTOR_OVERFLOW;
//    if (listener != null) {
//      listener.overflowed();
//    } else {
//
//    }
//  }

  public void reset(int index) {
    assert index <= rowIndex;
    rowIndex = index;
  }

  public void reset() {
    rowIndex = 0;
  }

  @Override
  public void nextElement() { }
}
