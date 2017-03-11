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
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
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

public interface RowSet {

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

  VectorAccessible getVectorAccessible();

  VectorContainer getContainer();

  void makeSv2();

  SelectionVector2 getSv2();

  void allocate(int recordCount);

  void setRowCount(int rowCount);

  int rowCount();

  RowSetWriter writer(int initialRowCount);

  RowSetReader reader();

  void clear();

  ValueVector[] vectors();

  RowSetSchema schema();

  BufferAllocator getAllocator();

  boolean hasSv2();

  void print();

  int getSize();

  BatchSchema getBatchSchema();
}
