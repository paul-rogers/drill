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
package org.apache.drill.test.rowSet;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

/**
 * Fluent builder to quickly build up an row set (record batch)
 * programmatically.
 */

public final class RowSetBuilder {

  private DirectRowSet rowSet;
  private RowSetWriter writer;
  private boolean withSv2;

  public RowSetBuilder(BufferAllocator allocator, BatchSchema schema) {
    this(allocator, schema, 10);
  }

  public RowSetBuilder(BufferAllocator allocator, BatchSchema schema, int capacity) {
    rowSet = new DirectRowSet(allocator, schema);
    writer = rowSet.writer(capacity);
  }

  public RowSetBuilder add(Object...values) {
    if (! writer.valid()) {
      throw new IllegalStateException( "Write past end of row set" );
    }
    TupleWriter row = writer.row();
    for (int i = 0; i < values.length;  i++) {
      row.set(i, values[i]);
    }
    return this;
  }

  public RowSetBuilder withSv2() {
    withSv2 = true;
    return this;
  }

  public SingleRowSet build() {
    writer.done();
    if (withSv2) {
      return rowSet.toIndirect();
    }
    return rowSet;
  }
}
