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
package org.apache.drill.exec.physical.rowSet.model.hyper;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.TupleModel;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.RowSetModel;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.MapColumnMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

/**
 * Implements a row set wrapper around a collection of "hyper vectors."
 * A hyper-vector is a logical vector formed by a series of physical vectors
 * stacked on top of one another. To make a row set, we have a hyper-vector
 * for each column. Another way to visualize this is as a "hyper row set":
 * a stacked collection of single row sets: each column is represented by a
 * vector per row set, with each vector in a row set having the same number
 * of rows. An SV4 then provides a uniform index into the rows in the
 * hyper set. A hyper row set is read-only.
 * <p>
 * Hyper-batches are read-only. The constituent vectors have already been
 * allocated and written; a hyper batch can only be read.
 */

public class HyperRowSetModel extends AbstractHyperTupleModel implements RowSetModel {

  /**
   * Primitive (non-map) column definition. Handles all three cardinalities.
   */

  public static class PrimitiveColumnModel extends AbstractHyperColumnModel {
    protected final HyperVectorWrapper<? extends ValueVector> vectors;

    /**
     * Create the column model using the column metadata provided, and
     * using a vector wrapper which matches the metadata. In particular the
     * same {@link MaterializedField} must back both the vector and
     * column metadata.
     *
     * @param schema metadata about the vector
     * @param vector vector to add
     */

    public PrimitiveColumnModel(ColumnMetadata schema, HyperVectorWrapper<? extends ValueVector> vectors) {
      super(schema);
      this.vectors = vectors;
      assert schema.schema() == vectors.getField();
    }

    /**
     * Create the column model, creating the column metadata from
     * the vector's schema.
     *
     * @param vector vector to add
     */

    public PrimitiveColumnModel(HyperVectorWrapper<? extends ValueVector> vectors) {
      this(TupleSchema.fromField(vectors.getField()), vectors);
    }

    @Override
    public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
      if (schema.isArray()) {
        return visitor.visitPrimitiveArrayColumn(this, arg);
      } else {
        return visitor.visitPrimitiveColumn(this, arg);
      }
    }

    @Override
    public HyperVectorWrapper<? extends ValueVector> vectors() { return vectors; }
  }

  /**
   * Map column definition. Represents both single and array maps. Maps
   * contain a nested tuple structure. (Not that, in this model, map
   * columns, themselves, are not maps. Rather, map columns contain a
   * tuple. This makes the class structure much simpler.)
   */

  public static class MapColumnModel extends AbstractHyperColumnModel {
    private final MapModel mapModel;
    protected final HyperVectorWrapper<? extends AbstractMapVector> vectors;

    /**
     * Construct a map column model from a schema, vector and map which are
     * all already populated.
     *
     * @param schema metadata description of the map column
     * @param vector vector that backs the column
     * @param mapModel tuple model for the columns within the map
     */
    public MapColumnModel(MapColumnMetadata schema,
                          HyperVectorWrapper<? extends AbstractMapVector> vectors,
                          MapModel mapModel) {
      super(schema);
      this.vectors = vectors;
      this.mapModel = mapModel;
      assert schema.mapSchema() == mapModel.schema();
      assert schema.schema() == vectors.getField();
    }

    @Override
    public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
      if (schema.isArray()) {
        return visitor.visitMapArrayColumn(this, arg);
      } else {
        return visitor.visitMapColumn(this, arg);
      }
    }

    @Override
    public TupleModel mapModel() { return mapModel; }

    public MapModel mapModelImpl() { return mapModel; }

    @Override
    public HyperVectorWrapper<? extends ValueVector> vectors() { return vectors; }
  }

  /**
   * Represents the tuple structure of a map column.
   */

  public static class MapModel extends AbstractHyperTupleModel {
    protected final HyperVectorWrapper<? extends AbstractMapVector> vectors;

    /**
     * Construct a new, empty map model using the tuple schema provided
     * (which is generally provided by the map column schema.)
     *
     * @param schema empty tuple schema
     * @param vector empty map vector
     */

    public MapModel(TupleSchema schema, HyperVectorWrapper<? extends AbstractMapVector> vectors) {
      super(schema, new ArrayList<ColumnModel>());
      this.vectors = vectors;
      assert schema.size() == 0;
    }

    /**
     * Build the map model given the map vector itself and the
     * list of column models that represent the map contents, and the tuple
     * metadata that describes the columns. The same {@link MaterializedField}
     * must back both the vector and tuple metadata.
     *
     * @param schema metadata for the map
     * @param vector vector which holds the map
     * @param columns column models for each column in the map
     */
    public MapModel(TupleSchema schema, HyperVectorWrapper<? extends AbstractMapVector> vectors, List<ColumnModel> columns) {
      super(schema, columns);
      this.vectors = vectors;
      assert schema.size() == vectors.getField().getChildren().size();
     }

    @Override
    public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
      if (schema.parent().isArray()) {
        return visitor.visitMapArray(this, arg);
      } else {
        return visitor.visitMap(this, arg);
      }
    }

    public HyperVectorWrapper<? extends ValueVector> vectors() { return vectors; }

    @Override
    public BufferAllocator allocator() {
      if (vectors.getValueVectors().length == 0) {
        return null;
      } else {
        return vectors.getValueVectors()[0].getAllocator();
      }
    }
  }

  private final VectorContainer container;

  public HyperRowSetModel(VectorContainer container, TupleSchema schema,
      List<ColumnModel> columns) {
    super(schema, columns);
    this.container = container;
    assert container.getNumberOfColumns() == schema.size();
  }

  public static HyperRowSetModel fromContainer(VectorContainer container) {
    return new VectorContainerParser().buildModel(container);
  }

  @Override
  public BufferAllocator allocator() { return container.getAllocator(); }

  @Override
  public VectorContainer container() { return container; }

  @Override
  public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
    return visitor.visitRow(this, arg);
  }
}
