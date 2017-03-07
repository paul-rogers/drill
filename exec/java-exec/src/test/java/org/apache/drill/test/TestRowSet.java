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
package org.apache.drill.test;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.exec.vector.accessor.ColumnWriter;

/**
 * A row set is a collection of rows stored as value vectors. Elsewhere in
 * Drill we call this a "record batch", but that term has been overloaded to
 * mean the runtime implementation of an operator...
 * <p>
 * A row set encapsulates a set of vectors and provides access to Drill's
 * various "views" of vectors: {@link VectorContainer},
 * {@link VectorAccessible}, etc.
 * <p>
 * A row set is defined by a {@link TestSchema}. For testing purposes, a row
 * set has a fixed schema; we don't allow changing the set of vectors
 * dynamically.
 * <p>
 * The row set also provides a simple way to write and read records using the
 * {@link RowSetWriter} and {@link RowSetReader} interfaces. As per Drill
 * conventions, a row set can be written (once), read many times, and finally
 * cleared.
 * <p>
 * Drill provides a large number of vector (data) types. Each requires a
 * type-specific way to set data. The row set writer uses a {@link ColumnWriter}
 * to set each value in a way unique to the specific data type. Similarly, the
 * row set reader provides a {@link ColumnReader} interface. In both cases,
 * columns can be accessed by index number (as defined in the schema) or
 * by name.
 * <p>
 * Putting this all together, the typical life-cycle flow is:
 * <ul>
 * <li>Define the schema using {@link TestSchema#builder()}.</li>
 * <li>Create the row set from the schema.</li>
 * <li>Populate the row set using a writer from {@link #writer(int)}.</li>
 * <li>Optionally add a selection vector: {@link #makeSv2()}.</li>
 * <li>Process the vector container using the code under test.</li>
 * <li>Retrieve the results using a reader from {@link #reader()}.</li>
 * <li>Dispose of vector memory with {@link #clear()}.</li>
 * </ul>
 */

public class TestRowSet {

  public interface RowSetWriter {
    void advance();
    void done();
    ColumnWriter column(int colIndex);
    ColumnWriter column(String colName);
  }

  public interface RowSetReader {
    boolean valid();
    boolean next();
    int rowIndex();
    int rowCount();
    ColumnReader column(int colIndex);
    ColumnReader column(String colName);
  }

  private final BufferAllocator allocator;
  private SelectionVector2 sv2;
  private final TestSchema schema;
  private final ValueVector[] valueVectors;
  private final VectorContainer container = new VectorContainer();
  private SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  public TestRowSet(BufferAllocator allocator, TestSchema schema) {
    this.allocator = allocator;
    this.schema = schema;
    valueVectors = new ValueVector[schema.count()];
    create();
  }

  private void create() {
    for (int i = 0; i < schema.count(); i++) {
      final MaterializedField field = schema.get(i);
      @SuppressWarnings("resource")
      ValueVector v = TypeHelper.getNewVector(field, allocator, callBack);
      valueVectors[i] = v;
      container.add(v);
    }
    container.buildSchema(SelectionVectorMode.NONE);
  }

  public VectorAccessible getVectorAccessible() {
    return container;
  }

  public void makeSv2() {
    if (sv2 != null) {
      return;
    }
    sv2 = new SelectionVector2(allocator);
    if (!sv2.allocateNewSafe(rowCount())) {
      throw new OutOfMemoryException("Unable to allocate sv2 buffer");
    }
    for (int i = 0; i < rowCount(); i++) {
      sv2.setIndex(i, (char) i);
    }
    sv2.setRecordCount(rowCount());
  }

  public SelectionVector2 getSv2() {
    return sv2;
  }

  public void allocate(int recordCount) {
    for (final ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, recordCount, 50, 10);
    }
  }

  public void setRowCount(int rowCount) {
    container.setRecordCount(rowCount);
    for (VectorWrapper<?> w : container) {
      w.getValueVector().getMutator().setValueCount(rowCount);
    }
  }

  public int rowCount() { return container.getRecordCount(); }

  public RowSetWriter writer(int initialRowCount) {
    allocate(initialRowCount);
    return new RowSetWriterImpl(this);
  }

  public RowSetReader reader() {
    return new RowSetReaderImpl(this);
  }

  public void clear() {
    container.zeroVectors();
    if (sv2 != null) {
      sv2.clear();
    }
    container.setRecordCount(0);
    sv2 = null;
  }

  public ValueVector[] vectors() {
    return valueVectors;
  }

  public TestSchema schema() {
    return schema;
  }
}