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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectWriter;

public abstract class BaseTupleLoader extends AbstractTupleLoader {

  public static class MapLoader extends BaseTupleLoader {
    @SuppressWarnings("unused")
    private final BaseTupleLoader parent;

    public MapLoader(BaseTupleLoader parent, ObjectWriter writer) {
      super(parent.resultSetLoader());
      this.parent = parent;
    }

    @Override
    public void bindVector(ValueVector vector) {
      // TODO Auto-generated method stub

    }
  }

  public static class RootLoader extends BaseTupleLoader {

    public RootLoader(ResultSetLoaderImpl resultSetLoader, WriterIndexImpl writerIndex) {
      super(resultSetLoader);
      bindIndex(writerIndex);
    }

    @Override
    public void bindVector(ValueVector vector) {
      // TODO Auto-generated method stub

    }
  }

  public BaseTupleLoader(ResultSetLoaderImpl resultSetLoader) {
    super(resultSetLoader);
  }

  @Override
  public void setSchema(BatchSchema schema) {
    if (! columns.isEmpty()) {
      throw new IllegalStateException("Can only set schema when the tuple schema is empty");
    }
    for (MaterializedField field : schema) {
      addColumn(field);
    }
  }

  @Override
  public ColumnLoader addColumn(MaterializedField columnSchema) {

    // Verify name is not a (possibly case insensitive) duplicate.

    String lastName = columnSchema.getName();
    String key = resultSetLoader.toKey(lastName);
    if (column(key) != null) {
      // TODO: Include full path as context
      throw new IllegalArgumentException("Duplicate column: " + lastName);
    }

    // Add the column.

    ColumnMetadata colMetadata = schema.add(columnSchema);
    assert colMetadata.index() == columns.size();
    BaseColumnLoader colLoader = BaseColumnLoader.build(this, colMetadata);
    columns.add(colLoader);

    // If a batch is active, prepare the column for writing.

    if (resultSetLoader.writeable()) {
      colLoader.startBatch();
    }
    return colLoader;
  }

  @Override
  protected void rollOver(int overflowIndex) {
    for (AbstractColumnLoader col : columns) {
      col.rollOver(overflowIndex);
    }
  }

  @Override
  protected void resetBatch() {
    for (AbstractColumnLoader col : columns) {
      col.startBatch();
    }
  }

  @Override
  protected void harvest() {
    endWrite();
    for (AbstractColumnLoader col : columns) {
      col.harvest();
    }
  }

  @Override
  public void buildContainer(VectorContainerBuilder containerBuilder) {
    for (AbstractColumnLoader col : columns) {
      col.buildContainer(containerBuilder);
    }
  }

  @Override
  public void reset() {
    for (AbstractColumnLoader col : columns) {
      col.reset();
    }
  }

  @Override
  public void reset(int index) {
    throw new IllegalStateException();
  }

  @Override
  public void close() {
    reset();
  }
}
