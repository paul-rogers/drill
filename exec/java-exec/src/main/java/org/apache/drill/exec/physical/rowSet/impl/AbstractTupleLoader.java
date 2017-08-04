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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.VectorContainerBuilder;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;

public abstract class AbstractTupleLoader extends AbstractTupleWriter implements TupleLoader {

  public static abstract class AbstractColumnLoader implements ColumnLoader {
    protected final AbstractTupleLoader parentTuple;
    protected final ColumnMetadata schema;

    public AbstractColumnLoader(AbstractTupleLoader parentTuple, ColumnMetadata schema) {
      this.parentTuple = parentTuple;
      this.schema = schema;
    }

    @Override
    public boolean isProjected() { return true; }

    @Override
    public int vectorIndex() { return schema.index(); }

    @Override
    public ColumnMetadata metadata() { return schema; }

    @Override
    public TupleLoader tupleLoader() { return parentTuple; }

    public abstract void startBatch();

    /**
     * A column within the row batch overflowed. Prepare to absorb the rest of
     * the in-flight row by rolling values over to a new vector, saving the
     * complete vector for later. This column could have a value for the overflow
     * row, or for some previous row, depending on exactly when and where the
     * overflow occurs.
     *
     * @param overflowIndex the index of the row that caused the overflow, the
     * values of which should be copied to a new "look-ahead" vector
     */

    public abstract void rollOver(int overflowIndex);

    /**
     * Writing of a row batch is complete. Prepare the vector for harvesting
     * to send downstream. If this batch encountered overflow, set aside the
     * look-ahead vector and put the full vector buffer back into the active
     * vector.
     */

    public abstract void harvest();

    public abstract void reset();

    public abstract void buildContainer(VectorContainerBuilder containerBuilder);

    @Override
    public abstract ObjectWriter writer();
  }

  protected final ResultSetLoaderImpl resultSetLoader;
  protected final List<AbstractColumnLoader> columns = new ArrayList<>();

  public AbstractTupleLoader(ResultSetLoaderImpl resultSetLoader) {
    super(new TupleSchema());
    this.resultSetLoader = resultSetLoader;
  }

  public ResultSetLoaderImpl resultSetLoader() { return resultSetLoader; }

  @Override
  public BatchSchema batchSchema() {
    return new BatchSchema(SelectionVectorMode.NONE, schema.toFieldList());
  }

  @Override
  public ColumnLoader columnLoader(String name) {
    return columnLoader(schema.index(name));
  }

  @Override
  public ColumnLoader columnLoader(int index) {
    return columns.get(index);
  }

  protected abstract void resetBatch();

  protected abstract void rollOver(int overflowIndex);

  protected abstract void harvest();

  public abstract void buildContainer(VectorContainerBuilder containerBuilder);

  public abstract void reset();

  public abstract void close();
}
