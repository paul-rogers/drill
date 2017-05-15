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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;

/**
 * Implementation of a column when creating a row batch.
 * Every column resides at an index, is defined by a schema,
 * is backed by a value vector, and and is written to by a writer.
 * Each column also tracks the schema version in which it was added
 * to detect schema evolution. Each column has an optional overflow
 * vector that holds overflow record values when a batch becomes
 * full.
 * <p>
 * Overflow vectors require special consideration. The vector class itself
 * must remain constant as it is bound to the writer. To handle overflow,
 * the implementation must replace the buffer in the vector with a new
 * one, saving the full vector to return as part of the final row batch.
 * This puts the column in one of three states:
 * <ul>
 * <li>Normal: only one vector is of concern - the vector for the active
 * row batch.</li>
 * <li>Overflow: a write to a vector caused overflow. For all columns,
 * the data buffer is shifted to a harvested vector, and a new, empty
 * buffer is put into the active vector.</li>
 * <li>Excess: a (small) column received values for the row that will
 * overflow due to a later column. When overflow occurs, the excess
 * column value, from the overflow record, resides in the active
 * vector. It must be shifted from the active vector into the new
 * overflow buffer.
 */

public class TupleSetImpl implements TupleSchema {

  public static class TupleLoaderImpl implements TupleLoader {

    public TupleSetImpl tupleSet;

    public TupleLoaderImpl(TupleSetImpl tupleSet) {
      this.tupleSet = tupleSet;
    }

    @Override
    public TupleSchema schema() { return tupleSet; }

    @Override
    public ColumnLoader column(int colIndex) {
      // TODO: Cache loaders here
      return tupleSet.columnImpl(colIndex).writer;
    }

    @Override
    public ColumnLoader column(String colName) {
      ColumnImpl col = tupleSet.columnImpl(colName);
      if (col == null) {
        throw new UndefinedColumnException(colName);
      }
      return col.writer;
    }
  }

  public static class ColumnImpl {

    private enum State { START, ACTIVE, HARVESTED, OVERFLOW, LOOK_AHEAD }

    final TupleSetImpl tupleSet;
    final int index;
    final MaterializedField schema;
    private ColumnImpl.State state = State.START;
    final ValueVector vector;
    final int addVersion;
    final AbstractColumnWriter columnWriter;
    final ColumnLoaderImpl writer;
    private ValueVector backupVector;

    /**
     * Build a column implementation, including vector and writers, based on the
     * schema provided.
     * @param tupleSet the tuple set that owns this column
     * @param schema the schema of the column
     * @param index the index of the column within the tuple set
     */

    public ColumnImpl(TupleSetImpl tupleSet, MaterializedField schema, int index) {
      this.tupleSet = tupleSet;
      this.schema = schema;
      this.index = index;
      RowSetMutatorImpl rowSetMutator = tupleSet.rowSetMutator();
      this.addVersion = rowSetMutator.schemaVersion();
      vector = TypeHelper.getNewVector(schema, rowSetMutator.allocator(), null);
      columnWriter = ColumnAccessorFactory.newWriter(schema.getType());
      WriterIndexImpl writerIndex = rowSetMutator.writerIndex();
      columnWriter.bind(writerIndex, vector);
      if (schema.getDataMode() == DataMode.REPEATED) {
        writer = new ArrayColumnLoader(writerIndex, columnWriter);
      } else {
        writer = new ScalarColumnLoader(writerIndex, columnWriter);
      }
    }

    /**
     * A column within the row batch overflowed. Prepare to absorb the rest of
     * the in-flight row by rolling values over to a new vector, saving the
     * complete vector for later. This column could have a value for the overflow
     * row, or for some previous row, depending on exactly when and where the
     * overflow occurs.
     *
     * @param overflowIndex the index of the row that caused the overflow, the
     * values of which should be copied to a new "look-ahead" vector
     */

    public void rollOver(int overflowIndex) {
      assert state == State.ACTIVE;

      // Close out the active vector, setting the record count.
      // This will be replaced later when the batch is done, with the
      // final row count. Here we set the count to fill in missing values and
      // set offsets in preparation for carving off the overflow value, if any.

      int writeIndex = writer.writeIndex();
      vector.getMutator().setValueCount(writeIndex);

      // Switch buffers between the backup vector and the writer's output
      // vector. Done this way because writers are bound to vectors and
      // we wish to keep the binding.

      if (backupVector == null) {
        backupVector = TypeHelper.getNewVector(schema, tupleSet.rowSetMutator().allocator(), null);
      }
      allocateVector(backupVector);
      vector.exchange(backupVector);
      state = State.OVERFLOW;

      // Any overflow value(s) to copy?

      if (writeIndex < overflowIndex) {
        return;
      }

      // Copy overflow values from the full vector to the new
      // look-ahead vector.

      int dest = 0;
      for (int src = overflowIndex; src < writeIndex; src++, dest++) {
        vector.copyEntry(dest, backupVector, src);
      }

      // Tell the writer the new write index. At this point, vectors will have
      // distinct write indexes depending on whether data was copied or not.

      writer.resetTo(dest);
    }

    /**
     * Writing of a row batch is complete. Prepare the vector for harvesting
     * to send downstream. If this batch encountered overflow, set aside the
     * look-ahead vector and put the full vector buffer back into the active
     * vector.
     */

    public void harvest() {
      assert state == State.ACTIVE || state == State.OVERFLOW;
      if (state == State.OVERFLOW) {
        vector.exchange(backupVector);
        state = State.LOOK_AHEAD;
      } else {
        state = State.HARVESTED;
      }
    }

    /**
     * Prepare the column for a new row batch. Clear the previous values.
     * If the previous batch created a look-ahead buffer, restore that to the
     * active vector so we start writing where we left off. Else, reset the
     * write position to the start.
     */

    public void resetBatch() {
      assert state == State.START || state == State.HARVESTED || state == State.LOOK_AHEAD;
      if (state == State.LOOK_AHEAD) {
        vector.exchange(backupVector);
        backupVector.clear();

        // Note: do not reset the writer: it is already positioned in the backup
        // vector from when we wrote the overflow row.

      } else {
        if (state != State.START) {
          vector.clear();
        }
        allocateVector(vector);
        writer.reset();
      }
      state = State.ACTIVE;
    }

    public void allocateVector(ValueVector toAlloc) {
      AllocationHelper.allocate(toAlloc, tupleSet.rowSetMutator().targetRowCount(), 50, 10);
    }

    public void reset() {
      vector.clear();
      if (backupVector != null) {
        backupVector.clear();
        backupVector = null;
      }
    }
  }

  private final RowSetMutatorImpl rowSetMutator;
  private final TupleSetImpl parent;
  private final TupleLoaderImpl loader;
  private final List<TupleSetImpl.ColumnImpl> columns = new ArrayList<>();
  private final Map<String,TupleSetImpl.ColumnImpl> nameIndex = new HashMap<>();
  private final List<AbstractColumnWriter> startableWriters = new ArrayList<>();

  public TupleSetImpl(RowSetMutatorImpl rowSetMutator) {
    this.rowSetMutator = rowSetMutator;
    parent = null;
    loader = new TupleLoaderImpl(this);
  }

  public TupleSetImpl(TupleSetImpl parent) {
    this.parent = parent;
    rowSetMutator = parent.rowSetMutator;
    loader = new TupleLoaderImpl(this);
  }

  public void start() {
    for (TupleSetImpl.ColumnImpl col : columns) {
      col.resetBatch();
    }
  }

  @Override
  public int addColumn(MaterializedField columnSchema) {

    // Verify name is not a (possibly case insensitive) duplicate.

    String lastName = columnSchema.getLastName();
    String key = rowSetMutator.toKey(lastName);
    if (column(key) != null) {
      // TODO: Include full path as context
      throw new IllegalArgumentException("Duplicate column: " + lastName);
    }

    // Verify that the cardinality (mode) is acceptable. Can't add a
    // non-nullable (required) field once one or more rows are saved;
    // we don't know what value to write to the unsaved rows.
    // TODO: If the first batch, we could fill the values with 0 (the
    // default, for types for which that is a valid value.

    if (columnSchema.getDataMode() == DataMode.REQUIRED &&
        rowSetMutator.rowCount() > 0) {
      throw new IllegalArgumentException("Cannot add a required field once data exists: " + lastName);
    }
    // TODO: If necessary, verify path

    // Add the column, increasing the schema version to indicate the change.

    rowSetMutator.bumpVersion();
    TupleSetImpl.ColumnImpl colImpl = new ColumnImpl(this, columnSchema, columnCount());
    columns.add(colImpl);
    nameIndex.put(key, colImpl);

    // Array writers must be told about the start of each row.

    if (columnSchema.getDataMode() == DataMode.REPEATED) {
      startableWriters.add(colImpl.columnWriter);
    }

    // If a batch is active, prepare the column for writing.

    if (rowSetMutator.writeable()) {
      colImpl.resetBatch();
    }
    return colImpl.index;
  }

  public RowSetMutatorImpl rowSetMutator() { return rowSetMutator; }

  @Override
  public int columnIndex(String colName) {
    ColumnImpl col = columnImpl(colName);
    return col == null ? -1 : col.index;
  }

  @Override
  public MaterializedField column(int colIndex) {
    return columnImpl(colIndex).schema;
  }

  protected ColumnImpl columnImpl(String colName) {
    return nameIndex.get(rowSetMutator.toKey(colName));
  }

  @Override
  public MaterializedField column(String colName) {
    TupleSetImpl.ColumnImpl col = columnImpl(colName);
    return col == null ? null : col.schema;
  }

  @Override
  public int columnCount() { return columns.size(); }

  public ColumnImpl columnImpl(int colIndex) {
    // Let the list catch out-of-bounds indexes
    return columns.get(colIndex);
  }

  public void startRow() {
    for (AbstractColumnWriter writer : startableWriters) {
      writer.start();
    }
  }

  protected void rollOver(int overflowIndex) {
    for (TupleSetImpl.ColumnImpl col : columns) {
      col.rollOver(overflowIndex);
    }
  }

  protected void harvest() {
    for (TupleSetImpl.ColumnImpl col : columns) {
      col.harvest();
    }
  }

  public TupleLoader loader() { return loader; }

  public void reset() {
    for (TupleSetImpl.ColumnImpl col : columns) {
      col.reset();
    }
  }

  public void close() {
    reset();
  }

  @Override
  public BatchSchema schema() {
    List<MaterializedField> fields = new ArrayList<>();
    for (TupleSetImpl.ColumnImpl col : columns) {
      fields.add(col.schema);
    }
    return new BatchSchema(SelectionVectorMode.NONE, fields);
  }
}
