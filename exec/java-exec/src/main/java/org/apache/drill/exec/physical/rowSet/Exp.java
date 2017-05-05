package org.apache.drill.exec.physical.rowSet;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.Exp.TupleSetImpl.ColumnImpl;
import org.apache.drill.exec.physical.rowSet.Exp.WriterIndexImpl.OverflowListener;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.joda.time.Period;

public class Exp {

  /**
   * Builds a row set (record batch) based on a defined schema which may
   * evolve (expand) over time. Automatically rolls "overflow" rows over
   * when a batch fills.
   * <p>
   * Presents columns using a physical schema. That is, map columns appear
   * as columns that provide a nested map schema. Presumes that column
   * access is primarily structural: first get a map, then process all
   * columns for the map.
   * <p>
   * If the input is a flat structure, then the physical schema has a
   * flattened schema as the degenerate case.
   * <p>
   * In both cases, access to columns is by index or by name. If new columns
   * are added while loading, their index is always at the end of the existing
   * columns.
   *
   * @see {@link VectorContainerWriter}, the class which this class
   * replaces
   */

  public interface RowSetMutator {

    /**
     * Current schema version. The version increments by one each time
     * a column is added.
     * @return the current schema version
     */

    int schemaVersion();
    int targetRowCount();
    int targetVectorSize();

    /**
     * Total number of batches created. Includes the current batch if
     * the row count in this batch is non-zero.
     * @return the number of batches produced including the current
     * one
     */

    int batchCount();

    /**
     * Total number of rows loaded for all previous batches and the
     * current batch.
     * @return total row count
     */

    int totalRowCount();

    /**
     * Start a new row batch. Valid only when first started, or after the
     * previous batch has been harvested.
     */

    void start();

    /**
     * Writer for the top-level tuple (the entire row). Valid only when
     * the mutator is actively writing a batch (after <tt>start()</tt>
     * but before </tt>harvest()</tt>.)
     *
     * @return writer for the top-level columns
     */

    TupleLoader writer();

    /**
     * Called after writing each row to move to the next row.
     */

    void save();

    /**
     * Indicates that no more rows fit into the current row batch
     * and that the row batch should be harvested and sent downstream.
     * Any overflow row is automatically saved for the next cycle.
     * @return whether the current row set has reached capacity
     */

    boolean isFull();

    /**
     * The number of rows in the current row set. Does not count any
     * overflow row saved for the next batch.
     * @return number of rows to be sent downstream
     */

    int rowCount();

    /**
     * Harvest the current row batch, and resetting the mutator
     * to the start of the next row batch (which may already contain
     * an overflow row.
     * @return the row batch to send downstream
     */

    VectorContainer harvest(); // ?

    /**
     * Called after all rows are returned, whether because no more data is
     * available, or the caller wishes to cancel the current row batch
     * and complete.
     */

    void close();
  }

  /**
   * Defines the schema of a tuple: either the top-level row or a nested
   * "map" (really structure). A schema is a collection of columns (backed
   * by vectors in the loader itself.) Columns are accessible by name or
   * index. New columns may be added at any time; the take the next available
   * index.
   */

  public interface TupleSchema {
    int columnCount();
    MaterializedField column(int colIndex);
    MaterializedField column(String colName);

    /**
     * Add a new column to the schema.
     *
     * @param columnSchema
     * @return the index of the new column
     */

    int addColumn(MaterializedField columnSchema);
  }

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

  public static class TupleSetImpl implements TupleSchema {

    public static class ColumnImpl {

      private enum State { START, ACTIVE, HARVESTED, OVERFLOW, LOOK_AHEAD }

      final TupleSetImpl tupleSet;
      final int index;
      final MaterializedField schema;
      private State state = State.START;
      final ValueVector vector;
      final int addVersion;
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
        writer = new ColumnLoaderImpl(rowSetMutator.writerIndex(), vector);
        resetBatch();
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
        state = State.OVERFLOW;
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
        }
        state = State.HARVESTED;
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

      public void close() {
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
    private final List<ColumnImpl> columns = new ArrayList<>();
    private final Map<String,ColumnImpl> nameIndex = new HashMap<>();

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
      for (ColumnImpl col : columns) {
        col.resetBatch();
      }
    }

    @Override
    public int addColumn(MaterializedField columnSchema) {
      String lastName = columnSchema.getLastName();
      String key = rowSetMutator.toKey(lastName);
      if (column(key) != null) {
        // TODO: Include full path as context
        throw new IllegalArgumentException("Duplicate column: " + lastName);
      }
      // TODO: If necessary, verify path
      rowSetMutator.bumpVersion();
      ColumnImpl colImpl = new ColumnImpl(this, columnSchema, columnCount());
      columns.add(colImpl);
      nameIndex.put(key, colImpl);
      return colImpl.index;
    }

    public RowSetMutatorImpl rowSetMutator() { return rowSetMutator; }

    @Override
    public MaterializedField column(int colIndex) {
      return columnImpl(colIndex).schema;
    }

    protected ColumnImpl columnImpl(String colName) {
      return nameIndex.get(rowSetMutator.toKey(colName));
    }

    @Override
    public MaterializedField column(String colName) {
      ColumnImpl col = columnImpl(colName);
      return col == null ? null : col.schema;
    }

    @Override
    public int columnCount() { return columns.size(); }

    public ColumnImpl columnImpl(int colIndex) {
      // Let the list catch out-of-bounds indexes
      return columns.get(colIndex);
    }

    protected void rollOver(int overflowIndex) {
      for (ColumnImpl col : columns) {
        col.rollOver(overflowIndex);
      }
    }

    protected void harvest() {
      for (ColumnImpl col : columns) {
        col.harvest();
      }
    }

    public TupleLoader loader() { return loader; }

    public void close() {
      for (ColumnImpl col : columns) {
        col.close();
      }
    }
  }

  /**
   * Writes values into the current row or map by column index or name.
   * Column indexes and names are as defined by the schema.
   *
   * @see {@link SingleMapWriter}, the class which this class
   * replaces
   */

  public interface TupleLoader {
    TupleSchema schema();
    ColumnLoader column(int colIndex);
    ColumnLoader column(String colName);
  }

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
      return tupleSet.columnImpl(colName).writer;
    }

  }

  /**
   * Writer for a single scalar or array column.
   *
   * @see {@link ScalarWriter}, the base class which this class
   * replaces. The prior version used a collection of vector-specific
   * subclasses of which the application must be aware. This version
   * uses a single interface for the application,.
   *
   */
  public interface ScalarLoader {
    void setInt(int value);
    void setLong(long value);
    void setDouble(double value);
    void setString(String value);
    void setBytes(byte[] value);
    void setDecimal(BigDecimal value);
    void setPeriod(Period value);
  }

  public interface ColumnLoader extends ScalarLoader {
    TupleLoader map();
    ArrayLoader array();
  }

  public interface ArrayLoader extends ColumnLoader {
    int size();
  }

  /**
   * Writer index that points to each row in the row set. The index starts at
   * the 0th row and advances one row on each increment. This allows writers to
   * start positioned at the first row. Writes happen in the current row.
   * Calling <tt>next()</tt> advances to the next position, effectively saving
   * the current row. The most recent row can be abandoned easily simply by not
   * calling <tt>next()</tt>. This means that the number of completed rows is
   * the same as the row index.
   */

  static class WriterIndexImpl implements ColumnWriterIndex {

    public enum State { OK, VECTOR_OVERFLOW, END_OF_BATCH }

    public interface OverflowListener {
      void overflowed();
    }

    private OverflowListener listener;
    private int rowIndex = 0;
    private State state = State.OK;

    public WriterIndexImpl(OverflowListener listener) {
      this.listener = listener;
    }

    @Override
    public int vectorIndex() { return rowIndex; }

    public boolean next() {
      if (++rowIndex < ValueVector.MAX_ROW_COUNT) {
        return true;
      } else {
        // Should not call next() again once batch is full.
        assert rowIndex == ValueVector.MAX_ROW_COUNT;
        rowIndex = ValueVector.MAX_ROW_COUNT;
        state = state == State.OK ? State.END_OF_BATCH : state;
        return false;
      }
    }

    public int size() {
      // The index always points to the next slot past the
      // end of valid rows.
      return rowIndex;
    }

    public boolean valid() { return state == State.OK; }

    public boolean hasOverflow() { return state == State.VECTOR_OVERFLOW; }

    @Override
    public void overflowed() {
      state = State.VECTOR_OVERFLOW;
      if (listener != null) {
        listener.overflowed();
      }
    }

    public void reset(int index) {
      assert index <= rowIndex;
      state = State.OK;
      rowIndex = index;
    }

    public void reset() {
      state = State.OK;
      rowIndex = 0;
    }
  }

  public static class MutatorOptions {
    public final int vectorSizeLimit;
    public final int rowCountLimit;
    public final boolean caseSensitive;

    public MutatorOptions() {
      vectorSizeLimit = ValueVector.MAX_BUFFER_SIZE;
      rowCountLimit = ValueVector.MAX_ROW_COUNT;
      caseSensitive = false;
    }
  }

  public static class VectorContainerBuilder {
    private final RowSetMutatorImpl rowSetMutator;
    private int lastUpdateVersion = -1;
    private VectorContainer container;

    public VectorContainerBuilder(RowSetMutatorImpl rowSetMutator) {
      this.rowSetMutator = rowSetMutator;
      container = new VectorContainer(rowSetMutator.allocator);
    }

    public void update() {
      if (lastUpdateVersion < rowSetMutator.schemaVersion()) {
        scanTuple(rowSetMutator.rootTuple);
        container.buildSchema(SelectionVectorMode.NONE);
        lastUpdateVersion = rowSetMutator.schemaVersion();
      }
      container.setRecordCount(rowSetMutator.rowCount());
    }

    private void scanTuple(TupleSetImpl tupleSet) {
      for (int i = 0; i < tupleSet.columnCount(); i++) {
        ColumnImpl colImpl = tupleSet.columnImpl(i);
        if (colImpl.addVersion <= lastUpdateVersion) {
          continue;
        }
        container.add(colImpl.vector);
      }
    }

    public VectorContainer container() { return container; }
  }

  public static class RowSetMutatorImpl implements RowSetMutator, OverflowListener {

    private enum State { START, ACTIVE, OVERFLOW, HARVESTED, LOOK_AHEAD, CLOSED }

    private final MutatorOptions options;
    private State state = State.START;
    private final BufferAllocator allocator;
    private int schemaVersion = 0;
    private TupleSetImpl rootTuple;
    private final WriterIndexImpl writerIndex;
    private VectorContainerBuilder containerBuilder;
    private int previousBatchCount;
    private int previousRowCount;
    private int pendingRowCount;

    public RowSetMutatorImpl(BufferAllocator allocator, MutatorOptions options) {
      this(allocator);
    }

    public RowSetMutatorImpl(BufferAllocator allocator) {
      this.allocator = allocator;
      options = new MutatorOptions();
      writerIndex = new WriterIndexImpl(this);
      rootTuple = new TupleSetImpl(this);
    }

    public String toKey(String colName) {
      return options.caseSensitive ? colName : colName.toLowerCase();
    }

    public BufferAllocator allocator() { return allocator; }

    protected int bumpVersion() { return ++schemaVersion; }

    @Override
    public int schemaVersion() { return schemaVersion; }

    @Override
    public void start() {
      if (state != State.START && state != State.HARVESTED && state != State.LOOK_AHEAD) {
        throw new IllegalStateException("Unexpected state: " + state);
      }
      rootTuple.start();
      if (state != State.LOOK_AHEAD) {
        writerIndex.reset();
      }
      pendingRowCount = 0;
      state = State.ACTIVE;
    }

    @Override
    public TupleLoader writer() {
      if (state != State.ACTIVE) {
        throw new IllegalStateException("Unexpected state: " + state);
      }
      return rootTuple.loader();
    }

    @Override
    public void save() {
      if (state != State.ACTIVE) {
        throw new IllegalStateException("Unexpected state: " + state);
      }
      writerIndex.next();
    }

    @Override
    public boolean isFull() { return ! writerIndex.valid(); }

    @Override
    public int rowCount() {
      return state == State.ACTIVE ? writerIndex.size() + pendingRowCount : 0;
    }

    protected ColumnWriterIndex writerIndex() { return writerIndex; }

    @Override
    public int targetRowCount() { return options.rowCountLimit; }

    @Override
    public int targetVectorSize() { return options.vectorSizeLimit; }

    @Override
    public void overflowed() {
      if (state != State.ACTIVE) {
        throw new IllegalStateException("Unexpected state: " + state);
      }
      pendingRowCount = rowCount();
      rootTuple.rollOver(writerIndex.vectorIndex());
      writerIndex.reset();
      state = State.OVERFLOW;
    }

    @Override
    public VectorContainer harvest() {
      if (state != State.ACTIVE && state != State.OVERFLOW) {
        throw new IllegalStateException("Unexpected state: " + state);
      }
      rootTuple.harvest();
      if (containerBuilder == null) {
        containerBuilder = new VectorContainerBuilder(this);
      }
      containerBuilder.update();
      VectorContainer container = containerBuilder.container();
      previousBatchCount++;
      previousRowCount += container.getRecordCount();
      state = state == State.OVERFLOW ? State.LOOK_AHEAD : State.HARVESTED;
      return container;
    }

    @Override
    public void close() {
      if (state == State.CLOSED) {
        return;
      }
      rootTuple.close();
      // TODO Auto-generated method stub
      state = State.CLOSED;
    }

    @Override
    public int batchCount() {
      return previousBatchCount + (rowCount() == 0 ? 0 : 1);
    }

    @Override
    public int totalRowCount() {
      return previousRowCount + rowCount();
    }
  }

  public static class ColumnLoaderImpl implements ColumnLoader {

    private static final String ROLLOVER_FAILED = "Row batch rollover failed.";

    private final AbstractColumnWriter writer;

    protected ColumnLoaderImpl(ColumnWriterIndex index, ValueVector vector) {
      writer = ColumnAccessorFactory.newWriter(vector.getField().getType());
      writer.bind(index, vector);
    }

    public int writeIndex() { return writer.lastWriteIndex(); }
    public void reset() { writer.reset(); }
    public void resetTo(int dest) { writer.reset(dest); }

    @Override
    public void setInt(int value) {
      try {
        writer.setInt(value);
      } catch (VectorOverflowException e) {
        writer.vectorIndex().overflowed();
        try {
          writer.setInt(value);
        } catch (VectorOverflowException e1) {
          throw new IllegalStateException(ROLLOVER_FAILED);
        }
      }
    }

    @Override
    public void setLong(long value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void setDouble(double value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void setString(String value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void setBytes(byte[] value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void setDecimal(BigDecimal value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void setPeriod(Period value) {
      // TODO Auto-generated method stub

    }

    @Override
    public TupleLoader map() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ArrayLoader array() {
      // TODO Auto-generated method stub
      return null;
    }

  }
}
