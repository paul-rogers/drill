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
package org.apache.drill.exec.physical.rowSet.model.single;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.TupleModel;
import org.apache.drill.exec.physical.rowSet.model.TupleModel.RowSetModel;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.MapColumnMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

/**
 * Concrete implementation of the row set model for a "single" row set.
 * Allows dynamic creation of new columns. (The mechanics of populating
 * those columns is delegated to writers.)
 * <p>
 * Provides the means to allocate vectors according to the allocation metadata
 * provided by the external schema (mostly the expected column width for
 * variable-width columns, and the expected cardinality for array columns.)
 * <p>
 * Single row sets allow write-once
 * behavior to populate vectors. After population, vectors can be read
 * any number of times.
 */

// TODO: Add list support. (Arrays of maps or primitives are already covered.)

public class SingleRowSetModel extends AbstractSingleTupleModel implements RowSetModel {

  /**
   * Primitive (non-map) column definition. Handles all three cardinalities.
   */

  public static class PrimitiveColumnModel extends AbstractSingleColumnModel {
    protected final ValueVector vector;

    /**
     * Create the column model using the column metadata provided, and
     * using a vector which matches the metadata. In particular the
     * same {@link MaterializedField} must back both the vector and
     * column metadata.
     *
     * @param schema metadata about the vector
     * @param vector vector to add
     */

    public PrimitiveColumnModel(ColumnMetadata schema, ValueVector vector) {
      super(schema);
      this.vector = vector;
      assert schema.schema() == vector.getField();
    }

    /**
     * Create the column model, creating the column metadata from
     * the vector's schema.
     *
     * @param vector vector to add
     */

    public PrimitiveColumnModel(ValueVector vector) {
      this(TupleSchema.fromField(vector.getField()), vector);
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
    public ValueVector vector() { return vector; }

    public void allocate(int valueCount) {
      AllocationHelper.allocatePrecomputedChildCount(vector, valueCount, schema.expectedWidth(), schema.expectedElementCount());
    }
  }

  /**
   * Map column definition. Represents both single and array maps. Maps
   * contain a nested tuple structure. (Not that, in this model, map
   * columns, themselves, are not maps. Rather, map columns contain a
   * tuple. This makes the class structure much simpler.)
   */

  public static class MapColumnModel extends AbstractSingleColumnModel {
    private final MapModel mapModel;
    private final AbstractMapVector vector;

    /**
     * Construct a map column model from a schema, vector and map which are
     * all already populated.
     *
     * @param schema metadata description of the map column
     * @param vector vector that backs the column
     * @param mapModel tuple model for the columns within the map
     */
    public MapColumnModel(MapColumnMetadata schema, AbstractMapVector vector, MapModel mapModel) {
      super(schema);
      this.vector = vector;
      this.mapModel = mapModel;
      assert schema.mapSchema() == mapModel.schema();
      assert schema.schema() == vector.getField();
      assert vector.size() == schema.mapSchema().size();
    }

    /**
     * Create a new, empty map column using the vector and metadata provided.
     *
     * @param schema metadata description of the map column
     * @param vector vector that backs the column
     */

    public MapColumnModel(ColumnMetadata schema, AbstractMapVector vector) {
      super(schema);
      this.vector = vector;
      mapModel = new MapModel((TupleSchema) schema.mapSchema(), vector);
      assert schema.mapSchema() == mapModel.schema();
      assert schema.schema() == vector.getField();
      assert vector.size() == 0;
      assert schema.mapSchema().size() == 0;
      assert vector.getField().getChildren().size() == 0;
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
    public AbstractMapVector vector() { return vector; }

    public void allocateArray(int valueCount) {
      ((RepeatedMapVector) vector).getOffsetVector().allocateNew(valueCount);
    }
  }

  /**
   * Represents the tuple structure of a map column.
   */

  public static class MapModel extends AbstractSingleTupleModel {
    private final AbstractMapVector vector;
    private ObjectWriter writer;

    /**
     * Construct a new, empty map model using the tuple schema provided
     * (which is generally provided by the map column schema.)
     *
     * @param schema empty tuple schema
     * @param vector empty map vector
     */

    public MapModel(TupleSchema schema, AbstractMapVector vector) {
      super(schema, new ArrayList<ColumnModel>());
      this.vector = vector;
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
    public MapModel(TupleSchema schema, AbstractMapVector vector, List<ColumnModel> columns) {
      super(schema, columns);
      this.vector = vector;
      assert vector.size() == schema.size();
    }

    @Override
    public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
      if (schema.parent().isArray()) {
        return visitor.visitMapArray(this, arg);
      } else {
        return visitor.visitMap(this, arg);
      }
    }

    public AbstractMapVector vector() { return vector; }

    @Override
    public BufferAllocator allocator() {
      return vector.getAllocator();
    }

    @Override
    public void addColumnImpl(AbstractSingleColumnModel colModel) {
      addBaseColumn(colModel);
      vector.putChild(colModel.schema().name(), colModel.vector());
      assert vector.size() == columns.size();
    }

    public void bindWriter(ObjectWriter writer) {
      this.writer = writer;
      writer.bindListener(this);
    }

    public ObjectWriter writer() { return writer; }

    @Override
    public void dump(HierarchicalFormatter format) {
      format.extend();
      super.dump(format);
      format
        .attributeIdentity("vector", vector)
        .attribute("schema", vector.getField())
        .attributeIdentity("writer", writer)
        .endObject();
    }
  }

  private final VectorContainer container;
  private AbstractTupleWriter writer;

  public SingleRowSetModel(VectorContainer container) {
    this.container = container;
  }

  public SingleRowSetModel(BufferAllocator allocator) {
    container = new VectorContainer(allocator);
  }

  public SingleRowSetModel(TupleMetadata schema, VectorContainer container,
      List<ColumnModel> columns) {
    super((TupleSchema) schema, columns);
    this.container = container;
  }

  public static SingleRowSetModel fromContainer(VectorContainer container) {
    return new VectorContainerParser().buildModel(container);
  }

  public static SingleRowSetModel fromSchema(BufferAllocator allocator,
      TupleMetadata schema) {
    return new ModelBuilder(allocator).buildModel(schema);
  }

  @Override
  public BufferAllocator allocator() { return container.getAllocator(); }

  @Override
  public VectorContainer container() { return container; }

  @Override
  public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
    return visitor.visitRow(this, arg);
  }

  @Override
  public void addColumnImpl(AbstractSingleColumnModel colModel) {
    addBaseColumn(colModel);

    // Add the column if not already part of the container. If
    // part of the container, ensure that the vectors match.

    if (container.getNumberOfColumns() < columns.size()) {
      container.add(colModel.vector());
      assert container.getNumberOfColumns() == columns.size();
    } else {
      assert container.getValueVector(columns.size() - 1).getValueVector() == colModel.vector();
    }
  }

  public void allocate(int rowCount) {
    new AllocationVisitor().allocate(this, rowCount);
  }

  public void bindWriter(AbstractTupleWriter writer) {
    this.writer = writer;
    writer.bindListener(this);
  }

  public AbstractTupleWriter writer() { return writer; }

  public void close() {
    container.clear();
  }

  @Override
  public void dump(HierarchicalFormatter format) {
    format.extend();
    super.dump(format);
    format.attributeArray("container");
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      format.elementIdentity(i, container.getValueVector(i).getValueVector());
    }
    format.endArray().attribute("writer");
    writer.dump(format);
    format.endObject();
  }
}
