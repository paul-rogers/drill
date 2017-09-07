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

import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Primitive (non-map) column definition. Handles all three cardinalities.
 */

public class PrimitiveColumnModel extends AbstractSingleColumnModel {
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
    if (vector != null) {
      AllocationHelper.allocatePrecomputedChildCount(vector,
          valueCount,
          schema.expectedWidth(),
          schema.expectedElementCount());
    }
  }

  @Override
  public void clear() {
    if (vector != null) {
      vector.clear();
    }
  }
}
