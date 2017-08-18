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

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.ColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

/**
 * Builds the harvest vector container that includes only the columns that
 * are included in the harvest schema version. That is, it excludes columns
 * added while writing the overflow row.
 * <p>
 * Because a Drill row is actually a hierarchy, walks the internal hierarchy
 * and builds a corresponding output hierarchy.
 * <ul>
 * <li>The root node is the row itself (vector container),</li>
 * <li>Internal nodes are maps (structures),</li.
 * <li>Leaf notes are primitive vectors (which may be arrays.</li>
 * </ul>
 * The basic algorithm is to identify the version of the output schema,
 * then add any new columns added up to that version. This object maintains
 * the output container across batches, meaning that updates are incremental:
 * we need only add columns that are new since the last update.
 * <p>
 * As special case occurs when columns are added in the overflow row. These
 * columns <b>do not</i> appear in the output container for the main part
 * of the batch; instead they appear in the <i>next</i> output container
 * that includes the overflow row.
 */

public class VectorContainerBuilder {

  /**
   * Drill vector containers and maps are both tuples, but they irritatingly
   * have completely different APIs for working with their child vectors.
   * This class acts as a proxy to wrap the two APIs to provide a common
   * view for the use of the container builder.
   */

  public static abstract class TupleProxy {
    protected TupleMetadata schema;

    public TupleProxy(TupleMetadata schema) {
      this.schema = schema;
    }

    protected abstract int size();
    protected abstract ValueVector vector(int index);
    protected abstract void add(ValueVector vector);

    protected TupleProxy mapProxy(int index) {
      return new MapProxy(
          schema.metadata(index).mapSchema(),
          (AbstractMapVector) vector(index));
    }
  }

  /**
   * Proxy wrapper class for a vector container.
   */

  protected static class ContainerProxy extends TupleProxy {

    private VectorContainer container;

    protected ContainerProxy(TupleMetadata schema, VectorContainer container) {
      super(schema);
      this.container = container;
    }

    @Override
    protected int size() {
      return container.getNumberOfColumns();
    }

    @Override
    protected ValueVector vector(int index) {
      return container.getValueVector(index).getValueVector();
    }

    @Override
    protected void add(ValueVector vector) {
      container.add(vector);
    }
  }

  /**
   * Proxy wrapper for a map container.
   */

  protected static class MapProxy extends TupleProxy {

    private AbstractMapVector mapVector;

    protected MapProxy(TupleMetadata schema, AbstractMapVector mapVector) {
      super(schema);
      this.mapVector = mapVector;
    }

    @Override
    protected int size() {
      return mapVector.size();
    }

    @Override
    protected ValueVector vector(int index) {
      return mapVector.getChildByOrdinal(index);
    }

    @Override
    protected void add(ValueVector vector) {
      mapVector.putChild(vector.getField().getName(), vector);
    }
  }

  private final ResultSetLoaderImpl resultSetLoader;
  private int lastUpdateVersion = -1;
  private TupleMetadata schema;
  private VectorContainer container;

  public VectorContainerBuilder(ResultSetLoaderImpl rsLoader) {
    this.resultSetLoader = rsLoader;
    container = new VectorContainer(rsLoader.allocator);
    schema = new TupleSchema();
  }

  public void update() {
    if (lastUpdateVersion >= resultSetLoader.schemaVersion()) {
      return;
    }
    lastUpdateVersion = resultSetLoader.schemaVersion();
    updateTuple(resultSetLoader.rootModel(), new ContainerProxy(schema, container));
    container.buildSchema(SelectionVectorMode.NONE);
  }

  public VectorContainer container() { return container; }

  public int lastUpdateVersion() { return lastUpdateVersion; }

  public BufferAllocator allocator() {
     return resultSetLoader.allocator();
  }

  private void updateTuple(AbstractSingleTupleModel sourceModel, TupleProxy destProxy) {
    int prevCount = destProxy.size();
    int currentCount = sourceModel.size();

    // Scan any existing maps for column additions

    for (int i = 0; i < prevCount; i++) {
      ColumnModel colModel = sourceModel.column(i);
      if (colModel.schema().isMap()) {
        updateTuple((AbstractSingleTupleModel) colModel.mapModel(), destProxy.mapProxy(i));
      }
    }

    // Add new columns, which map be maps

    for (int i = prevCount; i < currentCount; i++) {
      AbstractSingleColumnModel colModel = (AbstractSingleColumnModel) sourceModel.column(i);
      ColumnState state = colModel.coordinator();
      if (state.addVersion > lastUpdateVersion) {
        break;
      }
      if (colModel.schema().isMap()) {
        buildMap(destProxy, (MapColumnModel) colModel);
      } else {
        destProxy.add(colModel.vector());
        destProxy.schema.addColumn(colModel.schema());
        assert destProxy.size() == destProxy.schema.size();
      }
    }
  }

  private void buildMap(TupleProxy parentTuple, MapColumnModel colModel) {

    // Creating the map vector will create its contained vectors if we
    // give it a materialized field with children. So, instead pass a clone
    // without children so we can add them.

    ColumnMetadata mapColSchema = colModel.schema().cloneEmpty();

    // Don't get the map vector from the vector cache. Map vectors may
    // have content that varies from batch to batch. Only the leaf
    // vectors can be cached.

    @SuppressWarnings("resource")
    AbstractMapVector mapVector = (AbstractMapVector) TypeHelper.getNewVector(mapColSchema.schema(), allocator(), null);

    // Add the map vector and schema to the parent tuple

    parentTuple.add(mapVector);
    int index = parentTuple.schema.addColumn(mapColSchema);
    assert index == parentTuple.size();

    // Update the tuple, which will add the new columns in the map

    updateTuple(colModel.mapModelImpl(), parentTuple.mapProxy(index));
  }

  public TupleMetadata schema() { return schema; }
}