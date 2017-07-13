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
package org.apache.drill.exec.vector.accessor2.impl;

import org.apache.drill.exec.vector.accessor.TupleAccessor.TupleSchema;
import org.apache.drill.exec.vector.accessor2.ObjectWriter;
import org.apache.drill.exec.vector.accessor2.ScalarWriter;
import org.apache.drill.exec.vector.accessor2.TupleWriter;
import org.apache.drill.exec.vector.accessor2.ObjectWriter.ObjectType;

/**
 * Implementation for a writer for a tuple (a row or a map.) Provides access to each
 * column using either a name or a numeric index.
 */

public abstract class AbstractTupleWriter implements TupleWriter {

  public static class TupleObjectWriter extends AbstractObjectWriter {

    private AbstractTupleWriter tupleWriter;

    public TupleObjectWriter(AbstractTupleWriter tupleWriter) {
      this.tupleWriter = tupleWriter;
    }

    @Override
    public ObjectType type() {
      return ObjectType.SCALAR;
    }

    @Override
    public void set(Object value) {
      tupleWriter.setTuple(value);
    }

    public void start() {
      tupleWriter.start();
    }

    @Override
    public TupleWriter tuple() {
      return tupleWriter;
    }
  }

  protected final TupleSchema schema;
  private final AbstractObjectWriter writers[];

  public AbstractTupleWriter(TupleSchema schema, AbstractObjectWriter writers[]) {
    this.schema = schema;
    this.writers = writers;
  }

  @Override
  public TupleSchema schema() {
    return schema;
  }

  public void start() {
    for (int i = 0; i < writers.length;  i++) {
      writers[i].start();
    }
  }

  @Override
  public ObjectWriter column(int colIndex) {
    return writers[colIndex];
  }

  @Override
  public ObjectWriter column(String colName) {
    int index = schema.columnIndex(colName);
    if (index == -1) {
      return null; }
    return writers[index];
  }

  @Override
  public void set(int colIndex, Object value) {
    ObjectWriter colWriter = column(colIndex);
    switch (colWriter.type()) {
    case ARRAY:
      colWriter.array().setArray(value);
      break;
    case SCALAR:
      colWriter.scalar().setObject(value);
      break;
    case TUPLE:
      colWriter.tuple().setTuple(value);
      break;
    default:
      throw new IllegalStateException("Unexpected object type: " + colWriter.type());
    }
  }

  public void setTuple(Object ...values) {
    int count = Math.min(values.length, schema().count());
    for (int i = 0; i < count; i++) {
      set(i, values[i]);
    }
  }
}
