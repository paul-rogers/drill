package org.apache.drill.exec.physical.rowSet.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.BaseColumnLoader.PrimitiveColumnLoader.State;
import org.apache.drill.exec.physical.rowSet.impl.BaseTupleLoader.MapLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.VectorContainerBuilder;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.accessor.writer.ObjectArrayWriter;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

public abstract class LoaderStructure {

  public abstract static class TupleStructure extends LoaderStructure {
    protected final ResultSetLoaderImpl resultSetLoader;
    private final List<LoaderStructure> children = new ArrayList<>();
    
    public TupleStructure() {
      
    }
    
    public void add() ...
  }
  
  public static class RowStructure extends TupleStructure {
    
  }
  
  public static class MapStructure extends TupleStructure {
    
  }
  
  public abstract static class ColumnStructure extends LoaderStructure {
    protected final AbstractTupleLoader parentTuple;
    protected final ColumnMetadata schema;

    public ColumnStructure(AbstractTupleLoader parentTuple, ColumnMetadata schema) {
      this.parentTuple = parentTuple;
      this.schema = schema;
    }

//    @Override
//    public boolean isProjected() { return true; }
//
//    @Override
//    public int vectorIndex() { return schema.index(); }

    public ColumnMetadata metadata() { return schema; }

    public TupleLoader tupleLoader() { return parentTuple; }
  }
  
  /**
   * Implementation for primitive columns: those with actual vector
   * backing. (Maps are vectors in name only; they have no actual
   * vector backing the map itself.
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
  public static class PrimitiveColumnStructure extends ColumnStructure {
    private enum State {

      /**
       * Column is newly added. No data yet provided.
       */
      START,

      /**
       * Actively writing to the column. May have data.
       */
      ACTIVE,

      /**
       * After sending the current batch downstream, before starting
       * the next one.
       */
      HARVESTED,

      /**
       * Like ACTIVE, but means that the data has overflowed and the
       * column's data for the current row appears in the new,
       * overflow batch. For a reader that omits some columns, written
       * columns will be in OVERFLOW state, unwritten columns in
       * ACTIVE state.
       */
      OVERFLOW,

      /**
       * Like HARVESTED, but indicates that the column has data saved
       * in the overflow batch.
       */
      LOOK_AHEAD,

      /**
       * Like LOOK_AHEAD, but indicates the special case that the column
       * was added after overflow, so there is no vector for the column
       * in the harvested batch.
       */
      NEW_LOOK_AHEAD
    }

    private State state = State.START;
    protected AbstractScalarWriter columnWriter;
    private ValueVector backupVector;

    /**
     * Build a column implementation, including vector and writers, based on the
     * schema provided.
     * @param tupleSet the tuple set that owns this column
     * @param schema the schema of the column
     * @param index the index of the column within the tuple set
     */

    public PrimitiveColumnLoader(BaseTupleLoader tupleSet, ColumnMetadata schema, ValueVector vector) {
      super(tupleSet, schema, vector);
      allocateVector(vector);
      writer = ColumnAccessorFactory.buildColumnWriter(vector);
      tupleSet.addColumnWriter(writer);
      columnWriter = (AbstractScalarWriter) writer.scalar();
    }

    /**
     * Prepare the column for a new row batch. Clear the previous values.
     * If the previous batch created a look-ahead buffer, restore that to the
     * active vector so we start writing where we left off. Else, reset the
     * write position to the start.
     */

    @Override
    public void startBatch() {
      switch (state) {
      case NEW_LOOK_AHEAD:

        // Column is new, was not exchanged with backup vector

        break;
      case LOOK_AHEAD:
        vector.exchange(backupVector);
        backupVector.clear();
        break;
      case HARVESTED:

        // Note: do not reset the writer: it is already positioned in the backup
        // vector from when we wrote the overflow row.

        vector.clear();
        allocateVector(vector);
        writer.reset(-1);
        break;
      case START:
        break;
      default:
        throw new IllegalStateException("Unexpected state: " + state);
      }
      state = State.ACTIVE;
    }

    public void allocateVector(ValueVector toAlloc) {
      // TODO: Revise with better predictive metadata
      AllocationHelper.allocate(toAlloc, parentTuple.resultSetLoader().initialRowCount(), schema.allocationSize(), 10);
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

    @Override
    public void rollOver(int overflowIndex) {
      assert state == State.ACTIVE;

      // Remember the last write index for the original vector.

      int writeIndex = writer.lastWriteIndex();

      // Close out the active vector, setting the record count.
      // This will be replaced later when the batch is done, with the
      // final row count. Here we set the count to fill in missing values and
      // set offsets in preparation for carving off the overflow value, if any.

      columnWriter.endWrite();

      // Switch buffers between the backup vector and the writer's output
      // vector. Done this way because writers are bound to vectors and
      // we wish to keep the binding.

      if (backupVector == null) {
        backupVector = TypeHelper.getNewVector(schema.schema(), parentTuple.resultSetLoader().allocator(), null);
      }
      allocateVector(backupVector);
      vector.exchange(backupVector);
      state = State.OVERFLOW;

      // Any overflow value(s) to copy?

      int newIndex = -1;
      if (writeIndex >= overflowIndex) {

        // Copy overflow values from the full vector to the new
        // look-ahead vector.

        newIndex = 0;
        for (int src = overflowIndex; src <= writeIndex; src++, newIndex++) {
          vector.copyEntry(newIndex, backupVector, src);
        }
      }

      // Tell the writer that it has a new buffer and that it should reset
      // its last write index depending on whether data was copied or not.

      columnWriter.reset(newIndex);
    }

    /**
     * Writing of a row batch is complete. Prepare the vector for harvesting
     * to send downstream. If this batch encountered overflow, set aside the
     * look-ahead vector and put the full vector buffer back into the active
     * vector.
     */

    @Override
    public void harvest() {
      switch (state) {
      case OVERFLOW:
        vector.exchange(backupVector);
        state = State.LOOK_AHEAD;
        break;

      case ACTIVE:

        // If added after overflow, no data to save from the complete
        // batch: the vector does not appear in the completed batch.

        if (addVersion > parentTuple.resultSetLoader().schemaVersion()) {
          state = State.NEW_LOOK_AHEAD;
          return;
        }
        columnWriter.endWrite();
        state = State.HARVESTED;
        break;
      default:
        throw new IllegalStateException("Unexpected state: " + state);
      }
    }
    @Override
    public void reset() {
      vector.clear();
      if (backupVector != null) {
        backupVector.clear();
        backupVector = null;
      }
    }

    @Override
    public void buildContainer(VectorContainerBuilder containerBuilder) {

      // Don't add the vector if it is new in an overflow row.
      // Don't add it if it is already in the container.

      if (state != State.NEW_LOOK_AHEAD &&
          addVersion > containerBuilder.lastUpdateVersion()) {
        containerBuilder.add(vector);
      }
    }
  }

  public static class MapColumnLoader extends BaseColumnLoader {

    protected final MapLoader mapLoader;
    protected final AbstractMapVector mapVector;
    protected final AbstractTupleWriter mapWriter;

    public MapColumnLoader(BaseTupleLoader tupleSet, ColumnMetadata schema, ValueVector vector) {
      super(tupleSet, schema, vector);
      if (schema.mode() == DataMode.REPEATED) {
        RepeatedMapVector repeatedMapVector = (RepeatedMapVector) vector;
        AbstractObjectWriter mapWriter = MapWriter.buildMapArray(schema, repeatedMapVector);
        writer = ObjectArrayWriter.build(mapWriter);
      } else {
        MapVector mapVector = (MapVector) vector;
        writer = MapWriter.buildSingleMap(schema, new ArrayList<>());
      }
      writer.bindVector(vector);
      tupleSet.addColumnWriter(writer);
      mapLoader = new MapLoader(tupleSet, writer);
    }

    @Override
    public void startBatch() {
      mapLoader.resetBatch();
    }

    @Override
    public void rollOver(int overflowIndex) {
      mapLoader.rollOver(overflowIndex);
    }

    @Override
    public void harvest() {
      mapLoader.harvest();
    }

    @Override
    public void reset() {
      mapLoader.reset();
    }

    @Override
    public void buildContainer(VectorContainerBuilder containerBuilder) {
      mapLoader.buildContainer(containerBuilder);
    }
  }

  public abstract void startBatch();

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

  public abstract void rollOver(int overflowIndex);

  /**
   * Writing of a row batch is complete. Prepare the vector for harvesting
   * to send downstream. If this batch encountered overflow, set aside the
   * look-ahead vector and put the full vector buffer back into the active
   * vector.
   */

  public abstract void harvest();

  public abstract void reset();

  public abstract void buildContainer(VectorContainerBuilder containerBuilder);

  @Override
  public abstract ObjectWriter writer();
}
