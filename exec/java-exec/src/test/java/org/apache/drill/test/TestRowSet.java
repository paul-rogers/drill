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

public class TestRowSet {

  public interface ColumnReader {
    boolean isNull();
    int getInt();
    String getString();
  }

  public interface RecordSetWriter {
    void advance();
    void done();
    ColumnWriter column(int colIndex);
    ColumnWriter column(String colName);
  }

  public interface ColumnWriter {
    void setNull();
    void setInt(int value);
    void setString(String value);
  }

  public interface RecordSetReader {
    boolean valid();
    boolean advance();
    int rowIndex();
    int rowCount();
    ColumnReader column(int colIndex);
    ColumnReader column(String colName);
  }

  private BufferAllocator allocator;
  private SelectionVector2 sv2;
  private TestSchema schema;
  ValueVector[] valueVectors;
  private final VectorContainer container = new VectorContainer();
  private SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  public TestRowSet(BufferAllocator allocator, TestSchema schema) {
    this.allocator = allocator;
    this.schema = schema;
    create();
  }

  private void create() {
    valueVectors = new ValueVector[schema.count()];
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

  public RecordSetWriter writer() {
    return new RecordSetWriterImpl(this);
  }

  public RecordSetReader reader() {
    return new RecordSetReaderImpl(this);
  }

  public void clear() {
    container.zeroVectors();
    if (sv2 != null) {
      sv2.clear();
    }
    container.setRecordCount(0);
    sv2 = null;
  }
}