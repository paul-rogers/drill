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
package org.apache.drill.test.rowSet;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.physical.rowSet.model.ReaderIndex;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.RowSetModel;
import org.apache.drill.exec.physical.rowSet.model.single.ReaderBuilderVisitor;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

/**
 * Base class for row sets backed by a single record batch.
 */

public abstract class AbstractSingleRowSet extends AbstractRowSet implements SingleRowSet {

  public static class RowSetReaderBuilder extends ReaderBuilderVisitor {

    public RowSetReader buildReader(AbstractSingleRowSet rowSet, ReaderIndex rowIndex) {
      SingleRowSetModel rowModel = rowSet.rowSetModelImpl();
      return new RowSetReaderImpl(rowModel.schema(), rowIndex, buildTuple(rowModel));
    }
  }

  protected final SingleRowSetModel model;

  public AbstractSingleRowSet(AbstractSingleRowSet rowSet) {
    super(rowSet.allocator);
    model = rowSet.model;
  }

  public AbstractSingleRowSet(BufferAllocator allocator, SingleRowSetModel storage) {
    super(allocator);
    this.model = storage;
  }

  @Override
  public RowSetModel rowSetModel() { return model; }

  public SingleRowSetModel rowSetModelImpl() { return model; }

  @Override
  public int size() {
    RecordBatchSizer sizer = new RecordBatchSizer(container());
    return sizer.actualSize();
  }

  /**
   * Internal method to build the set of column readers needed for
   * this row set. Used when building a row set reader.
   * @param rowIndex object that points to the current row
   * @return an array of column readers: in the same order as the
   * (non-map) vectors.
   */

  protected RowSetReader buildReader(ReaderIndex rowIndex) {
    return new RowSetReaderBuilder().buildReader(this, rowIndex);
  }
}
