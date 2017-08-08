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
import org.apache.drill.exec.physical.rowSet.model.TupleModel.RowSetModel;
import org.apache.drill.exec.physical.rowSet.model.hyper.HyperRowSetModel;
import org.apache.drill.exec.physical.rowSet.model.hyper.ReaderBuilderVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.test.rowSet.RowSet.HyperRowSet;

/**
 * Implements a row set wrapper around a collection of "hyper vectors."
 * A hyper-vector is a logical vector formed by a series of physical vectors
 * stacked on top of one another. To make a row set, we have a hyper-vector
 * for each column. Another way to visualize this is as a "hyper row set":
 * a stacked collection of single row sets: each column is represented by a
 * vector per row set, with each vector in a row set having the same number
 * of rows. An SV4 then provides a uniform index into the rows in the
 * hyper set. A hyper row set is read-only.
 */

public class HyperRowSetImpl extends AbstractRowSet implements HyperRowSet {

  public static class RowSetReaderBuilder extends ReaderBuilderVisitor {

    public RowSetReader buildReader(HyperRowSetModel rowSetModel, SelectionVector4 sv4) {
      HyperRowIndex rowIndex = new HyperRowIndex(sv4);
      return new RowSetReaderImpl(rowSetModel.schema(), rowIndex, buildTuple(rowSetModel));
    }
  }

  /**
   * Selection vector that indexes into the hyper vectors.
   */

  private HyperRowSetModel rowSetModel;
  private final SelectionVector4 sv4;

  public HyperRowSetImpl(BufferAllocator allocator, VectorContainer container, SelectionVector4 sv4) {
    super(allocator);
    rowSetModel = HyperRowSetModel.fromContainer(container);
    this.sv4 = sv4;
  }

  @Override
  public boolean isExtendable() { return false; }

  @Override
  public boolean isWritable() { return false; }

  @Override
  public RowSetReader reader() {
    return new RowSetReaderBuilder().buildReader(rowSetModel, sv4);
  }

  @Override
  public SelectionVectorMode indirectionType() { return SelectionVectorMode.FOUR_BYTE; }

  @Override
  public SelectionVector4 getSv4() { return sv4; }

  @Override
  public int rowCount() { return sv4.getCount(); }

  @Override
  public RowSet merge(RowSet other) {
    return new HyperRowSetImpl(allocator, container().merge(other.container()), sv4);
  }

  @Override
  public RowSetModel rowSetModel() { return rowSetModel; }
}
