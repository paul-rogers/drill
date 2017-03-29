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
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.TupleAccessor.TupleSchema;
import org.apache.drill.exec.vector.accessor.impl.AbstractTupleAccessor;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnAccessor.RowIndex;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnAccessor.VectorAccessor;
import org.apache.drill.test.rowSet.HyperRowSetImpl.HyperRowIndex;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

/**
 * Basic implementation of a row set for both the single and multiple
 * (hyper) varieties, both the fixed and extendable varieties.
 */

public abstract class AbstractRowSet implements RowSet {

  public static abstract class RowSetIndex implements RowIndex {
    protected int rowIndex = -1;

    public int position() { return rowIndex; }
    public abstract boolean next();
    public abstract int size();
    public abstract boolean valid();
    public void setRowCount() { throw new UnsupportedOperationException(); }
    public void set(int index) { rowIndex = index; }
  }

  public static abstract class BoundedRowIndex extends RowSetIndex {

    protected final int rowCount;

    public BoundedRowIndex(int rowCount) {
      this.rowCount = rowCount;
    }

    @Override
    public boolean next() {
      if (++rowIndex < rowCount ) {
        return true;
      } else {
        rowIndex--;
        return false;
      }
    }

    @Override
    public int size() { return rowCount; }

    @Override
    public boolean valid() { return rowIndex < rowCount; }
  }

  public static class HyperVectorAccessor implements VectorAccessor {

    private final HyperRowIndex rowIndex;
    private final ValueVector[] vectors;

    public HyperVectorAccessor(HyperVectorWrapper<ValueVector> hvw, HyperRowIndex rowIndex) {
      this.rowIndex = rowIndex;
      vectors = hvw.getValueVectors();
    }

    @Override
    public ValueVector vector() {
      return vectors[rowIndex.batch()];
    }
  }

  public abstract class AbstractRowSetAccessor extends AbstractTupleAccessor {

    protected final RowSetIndex index;

    protected AbstractRowSetAccessor(TupleSchema schema, RowSetIndex index) {
      super(schema);
      this.index = index;
    }
  }

  public class RowSetReaderImpl extends AbstractRowSetAccessor implements RowSetReader {

    protected final TupleReader row;

    public RowSetReaderImpl(TupleSchema schema, RowSetIndex index, TupleReader row) {
      super(schema, index);
      this.row = row;
    }

    @Override
    public boolean next() { return index.next(); }

    @Override
    public boolean valid() { return index.valid(); }

    @Override
    public int index() { return index.position(); }

    @Override
    public int size() { return index.size(); }

    @Override
    public int rowIndex() { return index.index(); }

    @Override
    public int batchIndex() { return index.batch(); }

    @Override
    public void set(int index) { this.index.set(index); }

    @Override
    public TupleReader row() { return row; }
  }

  public class RowSetWriterImpl extends AbstractRowSetAccessor implements RowSetWriter {

    protected final TupleWriter row;

    protected RowSetWriterImpl(TupleSchema schema, RowSetIndex index, TupleWriter row) {
      super(schema, index);
      this.row = row;
    }

    @Override
    public TupleWriter row() { return row; }

    @Override
    public void setRow(Object...values) {
      if (! index.valid()) {
        throw new IndexOutOfBoundsException("Write past end of row set");
      }
      for (int i = 0; i < values.length;  i++) {
        row.set(i, values[i]);
      }
      save();
    }

    @Override
    public boolean valid() { return index.valid(); }

    @Override
    public int index() { return index.position(); }

    @Override
    public void save() { index.next(); }

    @Override
    public void done() { index.setRowCount(); }
  }

  protected final BufferAllocator allocator;
  protected final RowSetSchema schema;
  protected final VectorContainer container;
  protected SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  public AbstractRowSet(BufferAllocator allocator, BatchSchema schema, VectorContainer container) {
    this.allocator = allocator;
    this.schema = new RowSetSchema(schema);
    this.container = container;
  }

  @Override
  public VectorAccessible getVectorAccessible() { return container; }

  @Override
  public VectorContainer getContainer() { return container; }

  @Override
  public int rowCount() { return container.getRecordCount(); }

  @Override
  public void clear() {
    container.zeroVectors();
    container.setRecordCount(0);
  }

  @Override
  public RowSetSchema schema() { return schema; }

  @Override
  public BufferAllocator getAllocator() { return allocator; }

  @Override
  public void print() {
    new RowSetPrinter(this).print();
  }

  @Override
  public int getSize() {
    throw new UnsupportedOperationException("getSize");
  }

  @Override
  public BatchSchema getBatchSchema() {
    return container.getSchema();
  }
}
