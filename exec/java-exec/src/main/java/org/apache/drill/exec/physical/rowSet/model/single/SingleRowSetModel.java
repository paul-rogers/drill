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
import org.apache.drill.exec.physical.rowSet.model.TupleModel.RowSetModel;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;

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
