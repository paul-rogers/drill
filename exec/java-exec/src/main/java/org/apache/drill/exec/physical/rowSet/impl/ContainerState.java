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

import java.util.Collection;

import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.*;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.PrimitiveColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.ColumnWriterFactory;

public abstract class ContainerState {

  protected final ResultSetLoaderImpl resultSetLoader;
  protected final ProjectionSet projectionSet;

  /**
   * Vector cache for this loader.
   * @see {@link OptionBuilder#setVectorCache()}.
   */

  protected final ResultVectorCache vectorCache;

  public ContainerState(ResultSetLoaderImpl rsLoader, ResultVectorCache vectorCache, ProjectionSet projectionSet) {
    this.resultSetLoader = rsLoader;
    this.vectorCache = vectorCache;
    this.projectionSet = projectionSet;
  }

  public abstract int innerCardinality();
  protected abstract void addColumn(ColumnState colState);

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
    if (resultSetLoader.writeable()) {
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

    if (columnSchema.isArray()) {
      return PrimitiveColumnState.newPrimitiveArray(resultSetLoader, vector, colWriter);
    } else {
      return PrimitiveColumnState.newPrimitive(resultSetLoader, vector, colWriter);
    }
  }

  /**
   * Build a new map (single or repeated) column. No map vector is created
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

    // Create the writer. Will be returned to the tuple writer.

    String colName = columnSchema.name();
    ProjectionSet childProjection = projectionSet.mapProjection(colName);
    if (columnSchema.isArray()) {
      return MapArrayColumnState.build(resultSetLoader,
          vectorCache.childCache(colName),
          columnSchema,
          childProjection);
    } else {
      return MapColumnState.build(resultSetLoader,
          vectorCache.childCache(colName),
          columnSchema,
          childProjection,
          isWithinUnion());
    }
  }

  private ColumnState buildUnion(ColumnMetadata columnSchema) {
    assert columnSchema.isVariant() && ! columnSchema.isArray();

    if (! columnSchema.isProjected()) {
      throw new UnsupportedOperationException("Drill does not currently support unprojected union columns: " +
          columnSchema.name());
    }

    return UnionColumnState.build(resultSetLoader,
        vectorCache.childCache(columnSchema.name()),
        columnSchema,
        new NullProjectionSet(true));
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
