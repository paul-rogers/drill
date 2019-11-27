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
package org.apache.drill.exec.physical.impl.protocol;

import java.util.Collections;
import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Wraps a vector container and optional selection vector in an interface
 * simpler than the entire {@link RecordBatch}. This implementation hosts
 * a container only.
 */

public abstract class AbstractContainerAccessor implements BatchAccessor {

  protected int batchCount;

  public int batchCount() { return batchCount; }

  @Override
  public boolean isValid() {
    VectorContainer container = container();
    return container != null && container.hasRecordCount();
  }

  @Override
  public BatchSchema schema() {
    return container().getSchema();
  }

  @Override
  public int rowCount() {
    Preconditions.checkState(isValid());
    switch(schema().getSelectionVectorMode()) {
    case TWO_BYTE:
      return selectionVector2().getCount();
    case FOUR_BYTE:
      return selectionVector4().getCount();
    default:
      return container().getRecordCount();
    }
  }

  @Override
  public SelectionVector2 selectionVector2() {
    // Throws an exception by default because containers
    // do not support selection vectors.
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 selectionVector4() {
    // Throws an exception by default because containers
    // do not support selection vectors.
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container().getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return container().getValueAccessorById(clazz, ids);
  }

  @Override
  public WritableBatch writableBatch() {
    Preconditions.checkState(isValid());
    return WritableBatch.get(container());
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    if (isValid()) {
      return container().iterator();
    } else {
      return Collections.emptyIterator();
    }
  }
}
