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

import org.apache.drill.exec.physical.rowSet.model.BaseTupleModel.BaseColumnModel;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

/**
 * Generic visitor-aware single-vector column model.
 */

public abstract class AbstractSingleColumnModel extends BaseColumnModel {

  public interface ColumnCoordinator {
    void dump(HierarchicalFormatter format);
  }

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
  }

  public ObjectWriter writer() { return writer; }

  public void clear() { }
}
