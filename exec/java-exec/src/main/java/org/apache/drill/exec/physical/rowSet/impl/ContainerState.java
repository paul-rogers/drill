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
import java.util.Collection;

import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.*;
import org.apache.drill.exec.physical.rowSet.impl.NullVectorState.UnmanagedVectorState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.OffsetVectorState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.SimpleVectorState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapArrayState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.SingleMapState;
import org.apache.drill.exec.physical.rowSet.impl.UnionState.UnionVectorState;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.PrimitiveColumnMetadata;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter.TupleObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.ColumnWriterFactory;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.accessor.writer.UnionWriterImpl;
import org.apache.drill.exec.vector.accessor.writer.UnionWriterImpl.VariantObjectWriter;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.exec.vector.complex.UnionVector;

/**
 * Abstract representation of a container of vectors: a row, a map, a
 * repeated map, a list or a union.
 * <p>
 * The container is responsible for creating new columns in response
 * from a writer listener event. Column creation requires a set of
 * four items:
 * <ul>
 * <li>The value vector (which may be null if the column is not
 * projected.</li>
 * <li>The writer for the column.</li>
 * <li>A vector state that manages allocation, overflow, cleanup
 * and other vector-specific tasks.</li>
 * <li>A column state which orchestrates the above three items.</li>
 * <ul>
 */

public abstract class ContainerState {

  protected final LoaderInternals loader;
  protected final ProjectionSet projectionSet;

  /**
   * Vector cache for this loader.
   * @see {@link OptionBuilder#setVectorCache()}.
   */

  protected final ResultVectorCache vectorCache;

  public ContainerState(LoaderInternals loader, ResultVectorCache vectorCache, ProjectionSet projectionSet) {
    this.loader = loader;
    this.vectorCache = vectorCache;
    this.projectionSet = projectionSet;
  }

  public abstract int innerCardinality();
  protected abstract void addColumn(ColumnState colState);
  protected LoaderInternals loader() { return loader; }

  public boolean isProjected(String columnName) {
    return projectionSet.isProjected(columnName);
  }

  protected abstract boolean isWithinUnion();

  public ResultVectorCache vectorCache() { return vectorCache; }

  /**
   * Implementation of the work to add a new column to this tuple given a
   * schema description of the column.
   *
   * @param columnSchema schema of the column
   * @return writer for the new column
   */

  protected ColumnState addColumn(ColumnMetadata columnSchema) {

    // Indicate projection in the metadata.

    ((AbstractColumnMetadata) columnSchema).setProjected(
        isProjected(columnSchema.name()));

    // Build the column

    ColumnState colState;
    if (columnSchema.isMap()) {
      colState = buildMap(columnSchema);
    } else if (columnSchema.isVariant()) {
      if (columnSchema.isArray()) {
        colState = buildList(columnSchema);
      } else {
        colState = buildUnion(columnSchema);
      }
    } else {
      colState = buildPrimitive(columnSchema);
    }
    addColumn(colState);
    colState.updateCardinality(innerCardinality());
    if (loader.writeable()) {
      colState.allocateVectors();
    }
    return colState;
  }

  /**
   * Build a primitive column. Check if the column is projected. If not,
   * allocate a dummy writer for the column. If projected, then allocate
   * a vector, a writer, and the column state which binds the two together
   * and manages the column.
   *
   * @param columnSchema schema of the new primitive column
   * @return column state for the new column
   */

  @SuppressWarnings("resource")
  private ColumnState buildPrimitive(ColumnMetadata columnSchema) {
    ValueVector vector;
    if (columnSchema.isProjected()) {

      // Create the vector for the column.

      vector = vectorCache.addOrGet(columnSchema.schema());

      // In permissive mode, the mode or precision of the vector may differ
      // from that requested. Update the schema to match.

      if (vectorCache.isPermissive() && ! vector.getField().isEquivalent(columnSchema.schema())) {
        columnSchema = ((PrimitiveColumnMetadata) columnSchema).mergeWith(vector.getField());
      }
    } else {

      // Column is not projected. No materialized backing for the column.

      vector = null;
    }

    // Create the writer.

    AbstractObjectWriter colWriter = ColumnWriterFactory.buildColumnWriter(columnSchema, vector);

    // Build the vector state which manages the vector.

    VectorState vectorState;
    if (vector == null) {
      vectorState = new NullVectorState();
    } else if (columnSchema.isArray()) {
      vectorState = new RepeatedVectorState(colWriter, (RepeatedValueVector) vector);
    } else if (columnSchema.isNullable()) {
      vectorState = new NullableVectorState(
          colWriter,
          (NullableVector) vector);
    } else {
      vectorState = SimpleVectorState.vectorState(
            colWriter.scalar(), vector);
    }

    // Create the column state which binds the vector and writer together.

    return new PrimitiveColumnState(loader, colWriter,
        vectorState);
  }

  /**
   * Build a new map (single or repeated) column. Except for maps nested inside
   * of unions, no map vector is created
   * here, instead we create a tuple state to hold the columns, and defer the
   * map vector (or vector container) until harvest time.
   *
   * @param columnSchema description of the map column
   * @return column state for the map column
   */

  private ColumnState buildMap(ColumnMetadata columnSchema) {

    // When dynamically adding columns, must add the (empty)
    // map by itself, then add columns to the map via separate
    // calls.

    assert columnSchema.isMap();
    assert columnSchema.mapSchema().size() == 0;

    // Create the vector, vector state and writer.

    if (columnSchema.isArray()) {
      return buildMapArray(columnSchema);
    } else {
      return buildSingleMap(columnSchema);
    }
  }

  @SuppressWarnings("resource")
  private ColumnState buildSingleMap(ColumnMetadata columnSchema) {
    MapVector vector;
    if (isWithinUnion()) {
      vector = new MapVector(columnSchema.schema(), loader.allocator(), null);
    } else {
      vector = null;
    }
    TupleObjectWriter mapWriter = MapWriter.buildMap(columnSchema,
        vector, new ArrayList<AbstractObjectWriter>());
    SingleMapState mapState = new SingleMapState(loader,
        vectorCache.childCache(columnSchema.name()),
        projectionSet.mapProjection(columnSchema.name()));
    return new MapColumnState(mapState,
        mapWriter, new UnmanagedVectorState(vector));
  }

  @SuppressWarnings("resource")
  private ColumnState buildMapArray(ColumnMetadata columnSchema) {

    // Create the map's offset vector.

    UInt4Vector offsetVector;
    if (columnSchema.isProjected()) {
      offsetVector = new UInt4Vector(
        BaseRepeatedValueVector.OFFSETS_FIELD,
        loader.allocator());
    } else {
      offsetVector = null;
    }

    // Create the writer using the offset vector

    AbstractObjectWriter writer = MapWriter.buildMapArray(
        columnSchema, offsetVector,
        new ArrayList<AbstractObjectWriter>());

    // Wrap the offset vector in a vector state

    VectorState vectorState;
    if (columnSchema.isProjected()) {
      vectorState = new OffsetVectorState(
          (((AbstractArrayWriter) writer.array()).offsetWriter()),
          offsetVector,
          writer.array().entry());
    } else {
      vectorState = new NullVectorState();
    }

    // Assemble it all into the column state.

    MapArrayState mapState = new MapArrayState(loader,
        vectorCache.childCache(columnSchema.name()),
        projectionSet.mapProjection(columnSchema.name()));
    return new MapColumnState(mapState, writer, vectorState);
  }

  @SuppressWarnings("resource")
  private ColumnState buildUnion(ColumnMetadata columnSchema) {
    assert columnSchema.isVariant() && ! columnSchema.isArray();

    if (! columnSchema.isProjected()) {
      throw new UnsupportedOperationException("Drill does not currently support unprojected union columns: " +
          columnSchema.name());
    }

    // Create the union vector.

    UnionVector vector = new UnionVector(columnSchema.schema(), loader.allocator(), null);

    // Then the union writer.

    UnionWriterImpl unionWriter = new UnionWriterImpl(columnSchema, vector);
    VariantObjectWriter writer = new VariantObjectWriter(unionWriter);

    // The union vector state which manages the types vector.

    UnionVectorState vectorState = new UnionVectorState(vector, unionWriter);

    // Create the manager for the columns within the union.

    UnionState unionState = new UnionState(loader,
        vectorCache.childCache(columnSchema.name()), new NullProjectionSet(true));

    // Assemble it all into a union column state.

    return new UnionColumnState(loader,
        writer, vectorState, unionState);
  }

  private ColumnState buildList(ColumnMetadata columnSchema) {
    // TODO Auto-generated method stub
    assert false;
    return null;
  }

  protected abstract Collection<ColumnState> columnStates();

  /**
   * A column within the row batch overflowed. Prepare to absorb the rest of the
   * in-flight row by rolling values over to a new vector, saving the complete
   * vector for later. This column could have a value for the overflow row, or
   * for some previous row, depending on exactly when and where the overflow
   * occurs.
   */

  public void rollover() {
    for (ColumnState colState : columnStates()) {
      colState.rollover();
    }
  }

  /**
   * Writing of a row batch is complete, and an overflow occurred. Prepare the
   * vector for harvesting to send downstream. Set aside the look-ahead vector
   * and put the full vector buffer back into the active vector.
   */

  public void harvestWithLookAhead() {
    for (ColumnState colState : columnStates()) {
      colState.harvestWithLookAhead();
    }
  }

  /**
   * Start a new batch by shifting the overflow buffers back into the main
   * write vectors and updating the writers.
   */

  public void startBatch(boolean schemaOnly) {
    for (ColumnState colState : columnStates()) {
      colState.startBatch(schemaOnly);
    }
  }

  /**
   * Clean up state (such as backup vectors) associated with the state
   * for each vector.
   */

  public void close() {
    for (ColumnState colState : columnStates()) {
      colState.close();
    }
  }
}
