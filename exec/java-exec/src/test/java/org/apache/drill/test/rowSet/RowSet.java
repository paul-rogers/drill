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
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.TupleAccessor;
import org.apache.drill.exec.vector.accessor.TupleAccessor.AccessSchema;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.TupleWriter;

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

//  public interface RowSetAccessor extends TupleAccessor {
//    boolean next();
//    boolean valid();
//
//    /**
//     * Return the index of the row within the effective result set.
//     * (If an SV2 is in use, uses that to obtain data at the current
//     * index.)
//     * @return the current reader index
//     */
//
//    int index();
//
//    /**
//     * The index of the underlying row which may be indexed by an
//     * Sv2 or Sv4.
//     *
//     * @return
//     */
//
//    int rowIndex();
//
//    /**
//     * Total number of rows in the row set.
//     * @return total number of rows
//     */
//    int size();
//
//    /**
//     * Number of columns in the row set.
//     * @return number of columns
//     */
//    int width();
//
//    /**
//     * Batch index: 0 for a single batch, batch for the current
//     * row is a hyper-batch.
//     * @return index of the batch for the current row
//     */
//    int batchIndex();
//  }

  public interface RowSetWriter {
    AccessSchema schema();
    TupleWriter row();
    void setRow(Object...values);
    boolean valid();
    int index();
    void save();
    void done();
  }

  public interface RowSetReader {
    AccessSchema schema();

    /**
     * Total number of rows in the row set.
     * @return total number of rows
     */
    int size();

    boolean next();
    int index();
    void set(int index);

    /**
     * Batch index: 0 for a single batch, batch for the current
     * row is a hyper-batch.
     * @return index of the batch for the current row
     */
    int batchIndex();

    /**
     * The index of the underlying row which may be indexed by an
     * Sv2 or Sv4.
     *
     * @return
     */

    int rowIndex();
    boolean valid();
    TupleReader row();
  }

  boolean isExtendable();

  boolean isWritable();

  VectorAccessible getVectorAccessible();

  VectorContainer getContainer();

  int rowCount();

  RowSetWriter writer();

  RowSetReader reader();

  void clear();

  RowSetSchema schema();

  BufferAllocator getAllocator();

  SelectionVectorMode getIndirectionType();

  void print();

  int getSize();

  BatchSchema getBatchSchema();

  public interface SingleRowSet extends RowSet {
    ValueVector[] vectors();
    SingleRowSet toIndirect();
    SelectionVector2 getSv2();
  }

  public interface ExtendableRowSet extends SingleRowSet {
    void allocate(int recordCount);
    void setRowCount(int rowCount);
    RowSetWriter writer(int initialRowCount);
  }

  public interface HyperRowSet extends RowSet {
    SelectionVector4 getSv4();
    HyperVectorWrapper<ValueVector> getHyperVector(int i);
  }

}
