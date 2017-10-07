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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapState;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter.ColumnWriterListener;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;

/**
 * Represents the write-time state for a column including the writer and the (optional)
 * backing vector. Implements per-column operations such as vector overflow. If a column
 * is a (possibly repeated) map, then the column state will hold a tuple state.
 * <p>
 * If a column is not projected, then the writer exists (to make life easier for the
 * reader), but there will be no vector backing the writer.
 * <p>
 * Different columns need different kinds of vectors: a data vector, possibly an offset
 * vector, or even a non-existent vector. The {@link VectorState} class abstracts out
 * these differences.
 */

public abstract class ColumnState {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnState.class);

  /**
   * Primitive (non-map) column state. Handles all three cardinalities.
   * Column metadata is hosted on the writer.
   */

  public static class PrimitiveColumnState extends ColumnState implements ColumnWriterListener {

    public PrimitiveColumnState(LoaderInternals loader,
        AbstractObjectWriter colWriter,
        VectorState vectorState) {
      super(loader, colWriter, vectorState);
      ScalarWriter scalarWriter;
      if (colWriter.type() == ObjectType.ARRAY) {
        scalarWriter = writer.array().scalar();
      } else {
        scalarWriter = writer.scalar();
      }
      ((AbstractScalarWriter) scalarWriter).bindListener(this);
    }

    @Override
    public boolean canExpand(ScalarWriter writer, int delta) {
      return loader.canExpand(delta);
    }

    @Override
    public void overflowed(ScalarWriter writer) {
      loader.overflowed();
    }

    /**
     * Get the output schema. For a primitive (non-structured) column,
     * the output schema is the same as the internal schema.
     */

    @Override
    public ColumnMetadata outputSchema() { return schema(); }

    @Override
    public void dump(HierarchicalFormatter format) {
      // TODO Auto-generated method stub
    }
  }

  public static abstract class BaseContainerColumnState extends ColumnState {

    public BaseContainerColumnState(LoaderInternals loader,
        AbstractObjectWriter writer, VectorState vectorState) {
      super(loader, writer, vectorState);
    }

    public abstract ContainerState container();

    @Override
    public void updateCardinality(int cardinality) {
      super.updateCardinality(cardinality);
      container().updateCardinality();
    }

    @Override
    public void startBatch(boolean schemaOnly) {
      super.startBatch(schemaOnly);
      container().startBatch(schemaOnly);
    }

    @Override
    public void rollover() {
      super.rollover();
      container().rollover();
    }

    @Override
    public void harvestWithLookAhead() {
      super.harvestWithLookAhead();
      container().harvestWithLookAhead();
    }

    @Override
    public void close() {
      super.close();
      container().close();
    }
  }

  /**
   * Represents a map column (either single or repeated). Includes maps that
   * are top-level, nested within other maps, or nested inside a union.
   * Schema management is a bit complex:
   * <table border=1>
   * <tr><th rowspan=2>Condition</th><th colspan=2>Action</th></tr>
   * <tr><th>Outside of Union</th><th>Inside of Union<th></tr>
   * <tr><td>Unprojected</td><td>N/A</td><td>Omitted from output</td></tr>
   * <tr><td>Added in prior batch</td><td colspan=2>Included in output</td></tr>
   * <tr><td>Added in present batch, before overflow</td>
   *     <td colspan=2>Included in output</td></tr>
   * <tr><td>Added in present batch, after overflow</td>
   *     <td>Omitted from output this batch (added next batch)</td>
   *     <td>Included in output</td></tr>
   * </table>
   * <p>
   * The above rules say that, for maps in a union, the output schema
   * is identical to the internal writer schema. But, for maps outside
   * of union, the output schema is a subset of the internal schema with
   * two types of omissions:
   * <ul>
   * <li>Unprojected columns</li>
   * <li>Columns added after overflow</li>
   * </ul
   * <p>
   * New columns can be added at any time for data readers that discover
   * their schema as data is read (such as JSON). In this case, new columns
   * always appear at the end of the map (remember, in Drill, a "map" is actually
   * a structured: an ordered, named list of columns.) When looking for newly
   * added columns, they will always be at the end.
   */

  public static class MapColumnState extends BaseContainerColumnState {
    protected final MapState mapState;
    protected boolean isVersioned;
    protected final ColumnMetadata outputSchema;

    public MapColumnState(MapState mapState,
        AbstractObjectWriter writer,
        VectorState vectorState,
        boolean isVersioned) {
      super(mapState.loader(), writer, vectorState);
      this.mapState = mapState;
      mapState.bindColumnState(this);
      this.isVersioned = isVersioned;
      if (isVersioned) {
        outputSchema = schema().cloneEmpty();
      } else {
        outputSchema = schema();
      }
      mapState.bindOutputSchema(outputSchema.mapSchema());
    }

    public MapState mapState() { return mapState; }

    @Override
    public ContainerState container() { return mapState; }

    @Override
    public boolean isProjected() {
      return mapState.hasProjections();
    }

    /**
     * Indicate if this map is versioned. A versionable map has three attributes:
     * <ol>
     * <li>Columns can be unprojected. (Columns appear as writers for the client
     * of the result set loader, but are not materialized and do not appear in
     * the projected output container.</li>
     * <li>Columns appear in the output only if added before the overflow row.</li>
     * <li>As a result, the output schema is a subset of the internal input
     * schema.</li>
     * </ul>
     * @return <tt>true</tt> if this map is versioned as described above
     */

    public boolean isVersioned() { return isVersioned; }

    @Override
    public ColumnMetadata outputSchema() { return outputSchema; }
  }

  /**
   * Union or list (repeated union) column state.
   */

  public static class UnionColumnState extends BaseContainerColumnState {

    private final ContainerState unionState;

    public UnionColumnState(LoaderInternals loader,
        AbstractObjectWriter writer,
        VectorState vectorState,
        ContainerState unionState) {
      super(loader, writer, vectorState);
      this.unionState = unionState;
      unionState.bindColumnState(this);
    }

    @Override
    public boolean isProjected() {
      // Unions and lists are always projected
      return true;
    }

    /**
     * Get the output schema. For a primitive (non-structured) column,
     * the output schema is the same as the internal schema.
     */
    @Override
    public ColumnMetadata outputSchema() { return schema(); }

    @Override
    public ContainerState container() { return unionState; }
  }

  /**
   * Columns move through various lifecycle states as identified by this
   * enum. (Yes, sorry that the term "state" is used in two different ways
   * here: the variables for a column and the point within the column
   * lifecycle.
   */

  protected enum State {

    /**
     * Column is in the normal state of writing with no overflow
     * in effect.
     */

    NORMAL,

    /**
     * Like NORMAL, but means that the data has overflowed and the
     * column's data for the current row appears in the new,
     * overflow batch. For a client that omits some columns, written
     * columns will be in OVERFLOW state, unwritten columns in
     * NORMAL state.
     */

    OVERFLOW,

    /**
     * Indicates that the column has data saved
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

  protected final LoaderInternals loader;
  protected final int addVersion;
  protected final VectorState vectorState;
  protected State state;
  protected AbstractObjectWriter writer;

  /**
   * Cardinality of the value itself. If this is an array,
   * then this is the number of arrays. A separate number,
   * the inner cardinality, is computed as the outer cardinality
   * times the expected array count (from metadata.) The inner
   * cardinality is the total number of array items in the
   * vector.
   */

  protected int cardinality;
  protected int outputIndex = -1;

  public ColumnState(LoaderInternals loader,
      AbstractObjectWriter writer, VectorState vectorState) {
    this.loader = loader;
    this.vectorState = vectorState;
    this.addVersion = loader.bumpVersion();
    state = loader.hasOverflow() ?
        State.NEW_LOOK_AHEAD : State.NORMAL;
    this.writer = writer;
  }

  public AbstractObjectWriter writer() { return writer; }
  public ColumnMetadata schema() { return writer.schema(); }
  public VectorState vectorState() { return vectorState; }

  public <T extends ValueVector> T vector() { return vectorState.vector(); }

  public void allocateVectors() {
    assert cardinality != 0;
    loader.tallyAllocations(vectorState.allocate(cardinality));
  }

  /**
   * Prepare the column for a new row batch after overflow on the previous
   * batch. Restore the look-ahead buffer to the
   * active vector so we start writing where we left off.
   */

  public void startBatch(boolean schemaOnly) {
    switch (state) {
    case NORMAL:
      if (! schemaOnly) {
        allocateVectors();
      }
      break;

    case NEW_LOOK_AHEAD:

      // Column is new, was not exchanged with backup vector

      break;

    case LOOK_AHEAD:

      // Restore the look-ahead values to the main vector.

      vectorState.startBatchWithLookAhead();
      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // In all cases, we are back to normal writing.

    state = State.NORMAL;
  }

  /**
   * A column within the row batch overflowed. Prepare to absorb the rest of the
   * in-flight row by rolling values over to a new vector, saving the complete
   * vector for later. This column could have a value for the overflow row, or
   * for some previous row, depending on exactly when and where the overflow
   * occurs.
   */

  public void rollover() {
    assert state == State.NORMAL;

    // If the source index is 0, then we could not fit this one
    // value in the original vector. Nothing will be accomplished by
    // trying again with an an overflow vector. Just fail.
    //
    // Note that this is a judgment call. It is possible to allow the
    // vector to double beyond the limit, but that will require a bit
    // of thought to get right -- and, of course, completely defeats
    // the purpose of limiting vector size to avoid memory fragmentation...

    if (loader.rowIndex() == 0) {
      throw UserException
        .memoryError("A single column value is larger than the maximum allowed size of 16 MB")
        .build(logger);
    }

    // Otherwise, do the roll-over to a look-ahead vector.

    vectorState.rollover(cardinality);

    // Remember that we did this overflow processing.

    state = State.OVERFLOW;
  }

  /**
   * Writing of a row batch is complete. Prepare the vector for harvesting
   * to send downstream. If this batch encountered overflow, set aside the
   * look-ahead vector and put the full vector buffer back into the active
   * vector.
   */

  public void harvestWithLookAhead() {
    switch (state) {
    case NEW_LOOK_AHEAD:

      // If added after overflow, no data to save from the complete
      // batch: the vector does not appear in the completed batch.

      break;

    case OVERFLOW:

      // Otherwise, restore the original, full buffer and
      // last write position.

      vectorState.harvestWithLookAhead();

      // Remember that we have look-ahead values stashed away in the
      // backup vector.

      state = State.LOOK_AHEAD;
      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  public boolean isProjected() {
    return vectorState.isProjected();
  }

  public void updateCardinality(int cardinality) {
    this.cardinality = cardinality;
  }

  public int outerCardinality() { return cardinality; }

  public int innerCardinality() {
    ColumnMetadata schema = schema();
    return schema.isArray()
        ? cardinality * schema.expectedElementCount()
        : cardinality;
  }

  public void buildOutput(TupleState tupleState) {
    outputIndex = tupleState.addOutputColumn(vector(), outputSchema());
  }

  public abstract ColumnMetadata outputSchema();

  public void close() {
    vectorState.close();
  }

  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attribute("addVersion", addVersion)
      .attribute("state", state)
      .attributeIdentity("writer", writer)
      .attribute("vectorState")
      ;
    vectorState.dump(format);
    format.endObject();
  }
}
