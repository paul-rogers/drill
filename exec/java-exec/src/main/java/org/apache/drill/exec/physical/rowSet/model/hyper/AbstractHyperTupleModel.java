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

import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.BaseTupleModel;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.TupleSchema;

/**
 * Base class common to columns and tuples in the single-batch implementation.
 * Placed in a separate file to provide services common to both the row set
 * (main class file) and map vectors (nested class.) Defines the single-batch
 * visitor structure.
 */

public abstract class AbstractHyperTupleModel extends BaseTupleModel {

  /**
   * Generic visitor-aware single-vector column model.
   */

  public static abstract class AbstractHyperColumnModel extends BaseColumnModel {

    public AbstractHyperColumnModel(ColumnMetadata schema) {
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
    public abstract HyperVectorWrapper<? extends ValueVector> vectors();
  }

  public AbstractHyperTupleModel() { }

  public AbstractHyperTupleModel(TupleSchema schema, List<ColumnModel> columns) {
    super(schema, columns);
  }

  public abstract BufferAllocator allocator();

  /**
   * Defines the single-batch visitor interface for columns.
   *
   * @param visitor the visitor object
   * @param arg value passed into the visitor method
   * @return value returned from the visitor method
   */

  public abstract <R, A> R  visit(ModelVisitor<R, A> visitor, A arg);

  public <R, A> R visitChildren(ModelVisitor<R, A> visitor, A arg) {
    for (ColumnModel colModel : columns) {
      ((AbstractHyperColumnModel) colModel).visit(visitor, arg);
    }
    return null;
  }
}