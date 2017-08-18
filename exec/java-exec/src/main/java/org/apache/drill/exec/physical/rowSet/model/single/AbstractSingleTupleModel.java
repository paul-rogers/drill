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

import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.BaseTupleModel;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter.ColumnWriterListener;
import org.apache.drill.exec.vector.accessor.TupleWriter.TupleWriterListener;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

/**
 * Base class common to columns and tuples in the single-batch implementation.
 * Placed in a separate file to provide services common to both the row set
 * (main class file) and map vectors (nested class.) Defines the single-batch
 * visitor structure.
 */

public abstract class AbstractSingleTupleModel extends BaseTupleModel implements TupleWriterListener {

  public interface TupleCoordinator {
    void columnAdded(AbstractSingleTupleModel tuple, AbstractSingleColumnModel column);

    ObjectWriter columnAdded(AbstractSingleTupleModel abstractSingleTupleModel,
        TupleWriter tupleWriter, ColumnMetadata column);

    void dump(HierarchicalFormatter format);
  }

  public interface ColumnCoordinator {
    void overflowed(AbstractSingleColumnModel model);
  }

  /**
   * Generic visitor-aware single-vector column model.
   */

  public static abstract class AbstractSingleColumnModel extends BaseColumnModel implements ColumnWriterListener {

    private ColumnCoordinator coordinator;
    private ObjectWriter writer;

    public AbstractSingleColumnModel(ColumnMetadata schema) {
      super(schema);
    }

    /**
     * Defines the single-batch visitor interface for columns.
     *
     * @param visitor the visitor object
     * @param arg value passed into the visitor method
     * @return value returned from the visitor method
     */

    public abstract <R, A> R visit(ModelVisitor<R, A> visitor, A arg);
    public abstract ValueVector vector();

    public void bindCoordinator(ColumnCoordinator coordinator) {
      this.coordinator = coordinator;
    }

    @SuppressWarnings("unchecked")
    public <T extends ColumnCoordinator> T coordinator() {
      return (T) coordinator;
    }

    public void bindWriter(ObjectWriter writer) {
      this.writer = writer;
      writer.bindListener(this);
    }

    public ObjectWriter writer() { return writer; }

    @Override
    public void overflowed(ScalarWriter writer) {
      if (coordinator != null) {
        coordinator.overflowed(this);
      } else {
        throw new UnsupportedOperationException("Vector overflow");
      }
    }
  }

  private TupleCoordinator coordinator;

  public AbstractSingleTupleModel() { }

  public AbstractSingleTupleModel(TupleSchema schema, List<ColumnModel> columns) {
    super(schema, columns);
  }

  public ColumnModel add(MaterializedField field) {
    return add(TupleSchema.fromField(field));
  }

  public ColumnModel add(ColumnMetadata colMetadata) {
    ModelBuilder builder = new ModelBuilder(allocator());
    AbstractSingleColumnModel colModel = builder.buildColumn(colMetadata);
    addColumnImpl(colModel);
    return colModel;
  }

  public abstract void addColumnImpl(AbstractSingleColumnModel colModel);

  public abstract BufferAllocator allocator();

  /**
   * Defines the single-batch visitor interface for columns.
   *
   * @param visitor the visitor object
   * @param arg value passed into the visitor method
   * @return value returned from the visitor method
   */

  public abstract <R, A> R visit(ModelVisitor<R, A> visitor, A arg);

  public <R, A> R visitChildren(ModelVisitor<R, A> visitor, A arg) {
    for (ColumnModel colModel : columns) {
      ((AbstractSingleColumnModel) colModel).visit(visitor, arg);
    }
    return null;
  }

  public void bindCoordinator(TupleCoordinator coordinator) {
    this.coordinator = coordinator;
  }

  @SuppressWarnings("unchecked")
  public <T extends TupleCoordinator> T coordinator() {
    return (T) coordinator;
  }

  @Override
  public ObjectWriter addColumn(TupleWriter tupleWriter, MaterializedField column) {
    return addColumn(tupleWriter, TupleSchema.fromField(column));
  }

  @Override
  public ObjectWriter addColumn(TupleWriter tupleWriter, ColumnMetadata column) {
    if (coordinator == null) {
      throw new UnsupportedOperationException("Column add");
    } else {
      return coordinator.columnAdded(this, tupleWriter, column);
    }
  }

  public void dump(HierarchicalFormatter format) {
    format.startObject(this);
    if (coordinator == null) {
      format.attribute("coordinator", null);
    } else {
      format.attribute("coordinator");
      coordinator.dump(format);
    }

    format.endObject();
  }
}
