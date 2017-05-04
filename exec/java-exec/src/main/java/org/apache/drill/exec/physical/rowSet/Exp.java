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
     * Writer for the top-level tuple (the entire row).
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

  public static class TupleSetImpl implements TupleSchema {

    public static class ColumnImpl {
      final int index;
      final MaterializedField schema;
      final ValueVector vector;
      final int addVersion;
      final ColumnLoader writer;

      public ColumnImpl(MaterializedField schema, int index,
          int schemaVersion, ValueVector vector, ColumnLoader writer) {
        this.schema = schema;
        this.index = index;
        this.addVersion = schemaVersion;
        this.vector = vector;
        this.writer = writer;
      }
    }

    private final RowSetMutatorImpl rowSetMutator;
    private final TupleSetImpl parent;
    private final List<ColumnImpl> columns = new ArrayList<>();
    private final Map<String,ColumnImpl> nameIndex = new HashMap<>();

    public TupleSetImpl(RowSetMutatorImpl rowSetMutator) {
      this.rowSetMutator = rowSetMutator;
      parent = null;
    }

    public TupleSetImpl(TupleSetImpl parent) {
      this.parent = parent;
      rowSetMutator = parent.rowSetMutator;
    }

    @SuppressWarnings("resource")
    @Override
    public int addColumn(MaterializedField columnSchema) {
      String lastName = columnSchema.getLastName();
      String key = rowSetMutator.toKey(lastName);
      if (column(key) != null) {
        // TODO: Include full path as context
        throw new IllegalArgumentException("Duplicate column: " + lastName);
      }
      // TODO: If necessary, verify path
      ValueVector vector = TypeHelper.getNewVector(columnSchema, rowSetMutator.allocator(), null);
      ColumnLoader writer = new ColumnLoaderImpl(rowSetMutator.writerIndex(), vector);
      ColumnImpl colImpl = new ColumnImpl(columnSchema, columnCount(), rowSetMutator.bumpVersion(), vector, writer);
      columns.add(colImpl);
      nameIndex.put(key, colImpl);
      return colImpl.index;
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
      ColumnImpl col = columnImpl(colName);
      return col == null ? null : col.schema;
    }

    @Override
    public int columnCount() { return columns.size(); }

    public ColumnImpl columnImpl(int colIndex) {
      // Let the list catch out-of-bounds indexes
      return columns.get(colIndex);
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

    public TupleLoaderImpl(RowSetMutatorImpl rowSetMutator) {
      tupleSet = new TupleSetImpl(rowSetMutator);
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
      if (lastUpdateVersion == rowSetMutator.schemaVersion()) {
        return;
      }
      scanTuple(rowSetMutator.rootLoader.tupleSet);
      container.buildSchema(SelectionVectorMode.NONE);
      container.setRecordCount(rowSetMutator.rowCount());
      lastUpdateVersion = rowSetMutator.schemaVersion();
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

    public VectorContainer container() {
      return container;
    }
  }

  public static class RowSetMutatorImpl implements RowSetMutator, OverflowListener {

    private final MutatorOptions options;
    private int schemaVersion = -1;
    private TupleLoaderImpl rootLoader;
    private final WriterIndexImpl writerIndex;
    private final BufferAllocator allocator;
    private VectorContainerBuilder containerBuilder;

    public RowSetMutatorImpl(BufferAllocator allocator, MutatorOptions options) {
      this(allocator);
    }

    public RowSetMutatorImpl(BufferAllocator allocator) {
      this.allocator = allocator;
      options = new MutatorOptions();
      writerIndex = new WriterIndexImpl(this);
      rootLoader = new TupleLoaderImpl(this);
    }

    public String toKey(String colName) {
      return options.caseSensitive ? colName : colName.toLowerCase();
    }

    public BufferAllocator allocator() { return allocator; }

    protected int bumpVersion() { return ++schemaVersion; }

    @Override
    public int schemaVersion() { return schemaVersion; }

    @Override
    public TupleLoader writer() { return rootLoader; }

    @Override
    public void save() { writerIndex.next(); }

    @Override
    public boolean isFull() { return ! writerIndex.valid(); }

    @Override
    public int rowCount() { return writerIndex.size(); }

     protected ColumnWriterIndex writerIndex() { return writerIndex; }

    @Override
    public int targetRowCount() { return options.rowCountLimit; }

    @Override
    public int targetVectorSize() { return options.vectorSizeLimit; }

    @Override
    public void overflowed() {
      // TODO Auto-generated method stub

    }

    @Override
    public VectorContainer harvest() {
      if (containerBuilder == null) {
        containerBuilder = new VectorContainerBuilder(this);
      }
      containerBuilder.update();
      return containerBuilder.container();
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

  }


  public static class ColumnLoaderImpl implements ColumnLoader {

    private static final String ROLLOVER_FAILED = "Row batch rollover failed.";

    private final AbstractColumnWriter writer;

    protected ColumnLoaderImpl(ColumnWriterIndex index, ValueVector vector) {
      writer = ColumnAccessorFactory.newWriter(vector.getField().getType());
      writer.bind(index, vector);
    }

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
