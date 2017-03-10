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

import java.io.PrintStream;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;

/**
 * A row set is a collection of rows stored as value vectors. Elsewhere in
 * Drill we call this a "record batch", but that term has been overloaded to
 * mean the runtime implementation of an operator...
 * <p>
 * A row set encapsulates a set of vectors and provides access to Drill's
 * various "views" of vectors: {@link VectorContainer},
 * {@link VectorAccessible}, etc.
 * <p>
 * A row set is defined by a {@link RowSetSchema}. For testing purposes, a row
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
 * <li>Define the schema using {@link RowSetSchema#builder()}.</li>
 * <li>Create the row set from the schema.</li>
 * <li>Populate the row set using a writer from {@link #writer(int)}.</li>
 * <li>Optionally add a selection vector: {@link #makeSv2()}.</li>
 * <li>Process the vector container using the code under test.</li>
 * <li>Retrieve the results using a reader from {@link #reader()}.</li>
 * <li>Dispose of vector memory with {@link #clear()}.</li>
 * </ul>
 */

public class RowSet {

  public interface RowSetWriter {
    void advance();
    void done();
    ColumnWriter column(int colIndex);
    ColumnWriter column(String colName);
    void set(int colIndex, Object value);
    void setRow(Object...values);
  }

  public interface RowSetReader {
    boolean valid();
    boolean next();

    /**
     * Return the index of the row within the effective result set.
     * (If an SV2 is in use, uses that to obtain data at the current
     * index.)
     * @return the current reader index
     */
    int index();
    /**
     * Total number of rows in the row set.
     * @return total number of rows
     */
    int size();
    /**
     * Number of columns in the row set.
     * @return number of columns
     */
    int width();
    /**
     * Actual row offset. If no Sv2 is in use, then same as the index.
     * If an SV2 is in use, then the offset of the row to which the
     * SV2 points.
     * @return offset in the vectors of the data for this row
     */
    int offset();
    ColumnReader column(int colIndex);
    ColumnReader column(String colName);
    Object get(int colIndex);
    String getAsString(int colIndex);
  }

  public static class RowSetPrinter {
    private RowSet rowSet;

    public RowSetPrinter(RowSet rowSet) {
      this.rowSet = rowSet;
    }

    public void print() {
      print(System.out);
    }

    public void print(PrintStream out) {
      boolean hasSv2 = rowSet.hasSv2();
      RowSetReader reader = rowSet.reader();
      int colCount = reader.width();
      printSchema(out, hasSv2);
      while (reader.next()) {
        printHeader(out, reader, hasSv2);
        for (int i = 0; i < colCount; i++) {
          if (i > 0) {
            out.print(", ");
          }
          out.print(reader.getAsString(i));
        }
        out.println();
      }
    }

    private void printSchema(PrintStream out, boolean hasSv2) {
      out.print("#, ");
      if (hasSv2) {
        out.print("row #, ");
      }
      RowSetSchema schema = rowSet.schema;
      for (int i = 0; i < schema.count(); i++) {
        if (i > 0) {
          out.print(", ");
        }
        out.print(schema.get(i).getLastName());
      }
      out.println();
    }

    private void printHeader(PrintStream out, RowSetReader reader, boolean hasSv2) {
      out.print(reader.index());
      if (hasSv2) {
        out.print("(");
        out.print(reader.offset());
        out.print(")");
      }
      out.print(": ");
    }
  }

  private final BufferAllocator allocator;
  private SelectionVector2 sv2;
  private final RowSetSchema schema;
  private final ValueVector[] valueVectors;
  private final VectorContainer container;
  private SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  public RowSet(BufferAllocator allocator, RowSetSchema schema) {
    this.allocator = allocator;
    this.schema = schema;
    valueVectors = new ValueVector[schema.count()];
    container = new VectorContainer();
    create();
  }

  public RowSet(BufferAllocator allocator, VectorContainer container) {
    this.allocator = allocator;
    schema = new RowSetSchema(container.getSchema());
    this.container = container;
    valueVectors = new ValueVector[container.getNumberOfColumns()];
    int i = 0;
    for (VectorWrapper<?> w : container) {
      valueVectors[i++] = w.getValueVector();
    }
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

  public VectorAccessible getVectorAccessible() { return container; }

  public VectorContainer getContainer() { return container; }

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
    container.buildSchema(SelectionVectorMode.TWO_BYTE);
  }

  public SelectionVector2 getSv2() { return sv2; }

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

  public ValueVector[] vectors() { return valueVectors; }

  public RowSetSchema schema() { return schema; }

  public BufferAllocator getAllocator() { return allocator; }

  public boolean hasSv2() { return sv2 != null; }

  public void print() {
    new RowSetPrinter(this).print();
  }

  public int getSize() {
    RecordBatchSizer sizer = new RecordBatchSizer(container, sv2);
    return sizer.actualSize();
  }

  public BatchSchema getBatchSchema() {
    return container.getSchema();
  }
}
