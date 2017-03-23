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
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.test.rowSet.TupleSchema.RowSetSchema;

/**
 * Basic implementation of a row set for both the single and multiple
 * (hyper) varieties, both the fixed and extendable varieties.
 */

public abstract class AbstractRowSet implements RowSet {

  protected final BufferAllocator allocator;
  protected final RowSetSchema schema;
  protected final VectorContainer container;
  protected SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  public AbstractRowSet(BufferAllocator allocator, BatchSchema schema, VectorContainer container) {
    this.allocator = allocator;
    this.schema = new RowSetSchema(schema);
    this.container = container;
  }

  @Override
  public VectorAccessible getVectorAccessible() { return container; }

  @Override
  public VectorContainer getContainer() { return container; }

  @Override
  public int rowCount() { return container.getRecordCount(); }

  @Override
  public void clear() {
    container.zeroVectors();
    container.setRecordCount(0);
  }

  @Override
  public RowSetSchema schema() { return schema; }

  @Override
  public BufferAllocator getAllocator() { return allocator; }

  @Override
  public void print() {
    new RowSetPrinter(this).print();
  }

  @Override
  public int getSize() {
    throw new UnsupportedOperationException("getSize");
  }

  @Override
  public BatchSchema getBatchSchema() {
    return container.getSchema();
  }
}
