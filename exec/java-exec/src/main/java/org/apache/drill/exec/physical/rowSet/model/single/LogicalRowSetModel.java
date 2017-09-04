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
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;

/**
 * Row set model not backed by a vector container. This is a logical
 * model that may require additional mapping to produce a physical model
 * (that is, a vector container.) A logical model allows non-materialized
 * columns used, for example, when doing projection in a reader.
 */

public class LogicalRowSetModel extends AbstractSingleTupleModel {

  private final BufferAllocator allocator;
  private AbstractTupleWriter writer;

  public LogicalRowSetModel(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public LogicalRowSetModel(TupleMetadata schema, BufferAllocator allocator,
      List<ColumnModel> columns) {
    super((TupleSchema) schema, columns);
    this.allocator = allocator;
  }

  public static LogicalRowSetModel fromSchema(BufferAllocator allocator,
      TupleMetadata schema) {
    return new ModelBuilder(allocator).buildLogicalModel(schema);
  }

  @Override
  public BufferAllocator allocator() { return allocator; }

  @Override
  public void addColumnImpl(AbstractSingleColumnModel colModel) {
    addBaseColumn(colModel);
  }

  public void bindWriter(AbstractTupleWriter writer) {
    this.writer = writer;
  }

  public AbstractTupleWriter writer() { return writer; }

  public void close() {
    for (ColumnModel col : columns) {
      ((AbstractSingleColumnModel) col).clear();
    }
  }

  @Override
  public <R, A> R visit(ModelVisitor<R, A> visitor, A arg) {
    return visitor.visitLogicalRow(this, arg);
  }

  @Override
  public void dump(HierarchicalFormatter format) {
    format.extend();
    super.dump(format);
    format.endArray().attribute("writer");
    writer.dump(format);
    format.endObject();
  }
}
