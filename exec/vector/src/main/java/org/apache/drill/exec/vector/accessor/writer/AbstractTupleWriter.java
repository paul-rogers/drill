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
package org.apache.drill.exec.vector.accessor.writer;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Implementation for a writer for a tuple (a row or a map.) Provides access to each
 * column using either a name or a numeric index.
 */

public abstract class AbstractTupleWriter implements TupleWriter, WriterEvents {

  /**
   * Generic object wrapper for the tuple writer.
   */

  public static class TupleObjectWriter extends AbstractObjectWriter {

    private AbstractTupleWriter tupleWriter;

    public TupleObjectWriter(AbstractTupleWriter tupleWriter) {
      this.tupleWriter = tupleWriter;
    }

    @Override
    public ObjectType type() { return ObjectType.TUPLE; }

    @Override
    public void set(Object value) { tupleWriter.setObject(value); }

    @Override
    public TupleWriter tuple() { return tupleWriter; }

    @Override
    protected WriterEvents baseEvents() { return tupleWriter; }

  }

  public enum State { IDLE, IN_WRITE, IN_VALUE }

  protected ColumnWriterIndex vectorIndex;
  protected final TupleMetadata schema;
  protected final List<AbstractObjectWriter> writers;
  private State state = State.IDLE;

  protected AbstractTupleWriter(TupleMetadata schema, List<AbstractObjectWriter> writers) {
    this.schema = schema;
    this.writers = writers;
  }

  protected AbstractTupleWriter(TupleMetadata schema) {
    this(schema, new ArrayList<>());
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    vectorIndex = index;
    for (int i = 0; i < writers.size(); i++) {
      writers.get(i).bindIndex(index);
    }
  }

  /**
   * Add a column writer to an existing tuple writer. Used for implementations
   * that support "live" schema evolution: column discovery while writing.
   * The corresponding metadata must already have been added to the schema.
   *
   * @param colWriter the column writer to add
   */

  public void addColumnWriter(AbstractObjectWriter colWriter) {
    assert writers.size() + 1 == schema.size();
    writers.add(colWriter);
    colWriter.bindIndex(vectorIndex);
    if (state != State.IDLE) {
      colWriter.startWrite();
      if (state == State.IN_VALUE) {
        colWriter.startValue();
      }
    }
  }

  @Override
  public TupleMetadata schema() { return schema; }

  @Override
  public int size() { return schema().size(); }

  @Override
  public void startWrite() {
    assert state == State.IDLE;
    state = State.IN_WRITE;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).startWrite();
    }
  }

  @Override
  public void startValue() {
    assert state == State.IN_WRITE;
    state = State.IN_VALUE;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).startValue();
    }
  }

  @Override
  public void endValue() {
    assert state == State.IN_VALUE;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).endValue();
    }
    state = State.IN_WRITE;
  }

  @Override
  public void endWrite() {
    assert state == State.IN_WRITE;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).endWrite();
    }
    state = State.IDLE;
  }

  @Override
  public ObjectWriter column(int colIndex) {
    return writers.get(colIndex);
  }

  @Override
  public ObjectWriter column(String colName) {
    int index = schema.index(colName);
    if (index == -1) {
      return null; }
    return writers.get(index);
  }

  @Override
  public void set(int colIndex, Object value) {
    ObjectWriter colWriter = column(colIndex);
    switch (colWriter.type()) {
    case ARRAY:
      colWriter.array().setObject(value);
      break;
    case SCALAR:
      colWriter.scalar().setObject(value);
      break;
    case TUPLE:
      colWriter.tuple().setObject(value);
      break;
    default:
      throw new IllegalStateException("Unexpected object type: " + colWriter.type());
    }
  }

  @Override
  public void setTuple(Object ...values) {
    setObject(values);
  }

  @Override
  public void setObject(Object value) {
    Object values[] = (Object[]) value;
    int count = Math.min(values.length, schema().size());
    for (int i = 0; i < count; i++) {
      set(i, values[i]);
    }
  }

  @Override
  public ScalarWriter scalar(int colIndex) {
    return column(colIndex).scalar();
  }

  @Override
  public ScalarWriter scalar(String colName) {
    return column(colName).scalar();
  }

  @Override
  public TupleWriter tuple(int colIndex) {
    return column(colIndex).tuple();
  }

  @Override
  public TupleWriter tuple(String colName) {
    return column(colName).tuple();
  }

  @Override
  public ArrayWriter array(int colIndex) {
    return column(colIndex).array();
  }

  @Override
  public ArrayWriter array(String colName) {
    return column(colName).array();
  }

  @Override
  public ObjectType type(int colIndex) {
    return column(colIndex).type();
  }

  @Override
  public ObjectType type(String colName) {
    return column(colName).type();
  }

  @Override
  public int lastWriteIndex() {
    return vectorIndex.vectorIndex();
  }
}
