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
package org.apache.drill.test;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.test.TestRowSet.RowSetWriter;

public final class RowSetBuilder {

  private TestRowSet rowSet;
  private RowSetWriter writer;
  private boolean withSv2;

  public RowSetBuilder(BufferAllocator allocator, TestSchema schema) {
    this(allocator, schema, 10);
  }

  public RowSetBuilder(BufferAllocator allocator, TestSchema schema, int capacity) {
    rowSet = new TestRowSet(allocator, schema);
    writer = rowSet.writer(capacity);
  }

  public RowSetBuilder add(Object...values) {
    for (int i = 0; i < values.length;  i++) {
      Object value = values[i];
      if (value == null) {
        writer.column(i).setNull();
      } else if (value instanceof Integer) {
        writer.column(i).setInt((Integer) value);
      } else if (value instanceof Long) {
        writer.column(i).setLong((Long) value);
      } else if (value instanceof String) {
        writer.column(i).setString((String) value);
      } else if (value instanceof byte[]) {
        writer.column(i).setBytes((byte[]) value);
      } else {
        throw new IllegalArgumentException("Unsupported type " +
                  value.getClass().getSimpleName() + " for column " + i);
      }
    }
    writer.advance();
    return this;
  }

  public RowSetBuilder withSv2() {
    withSv2 = true;
    return this;
  }

  public TestRowSet build() {
    writer.done();
    if (withSv2) {
      rowSet.makeSv2();
    }
    return rowSet;
  }
}
