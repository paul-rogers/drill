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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.VariantMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnAccessorUtils;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.complex.UnionVector;

/**
 * Writer to a union vector.
 */

public class UnionWriterImpl implements VariantWriter, WriterEvents {

  public static class VariantObjectWriter extends AbstractObjectWriter {

    private final UnionWriterImpl writer;

    public VariantObjectWriter(UnionWriterImpl writer, ColumnMetadata schema) {
      super(schema);
      this.writer = writer;
    }

    @Override
    public ObjectType type() { return ObjectType.VARIANT; }

    @Override
    public void set(Object value) {
      writer.setObject(value);
    }

    @Override
    public VariantWriter variant() { return writer; }

    @Override
    public WriterEvents events() { return writer; }

    @Override
    public void dump(HierarchicalFormatter format) {
      writer.dump(format);
    }
  }

  private class DefaultListener implements VariantWriterListener {

    @Override
    public ObjectWriter addType(MinorType type) {
      ValueVector memberVector = ColumnAccessorUtils.getUnionMember(vector, type);
      schema.addType(type);
      schema.replaceSchema(vector.getField());
      ColumnMetadata memberSchema = schema.member(type);
      return ColumnWriterFactory.buildColumnWriter(memberSchema, memberVector);
    }
  }

  private final UnionVector vector;
  private final VariantMetadata schema;
  private final BaseScalarWriter typeWriter;
  private final AbstractObjectWriter variants[];
  private ColumnWriterIndex index;
  protected State state = State.IDLE;
  private VariantWriterListener listener;

  public UnionWriterImpl(VariantMetadata schema, UnionVector vector,
      AbstractObjectWriter variants[]) {
    this.vector = vector;
    this.schema = schema;
    if (variants == null) {
      this.variants = new AbstractObjectWriter[MinorType.values().length];
    } else {
      this.variants = variants;
    }
    typeWriter = ColumnWriterFactory.newWriter(vector.getTypeVector());
  }

  public UnionWriterImpl(VariantMetadata schema, UnionVector vector) {
    this(schema, vector, null);
  }

  @Override
  public VariantMetadata schema() { return schema; }

  @Override
  public int size() { return schema.size(); }

  @Override
  public boolean hasType(MinorType type) {
    return variants[type.ordinal()] != null;
  }

  @Override
  public void setNull() {

    // Not really necessary: the default value is 0.
    // This lets a caller change its mind after setting a
    // value.

    typeWriter.setInt(UnionVector.NULL_MARKER);
  }

  @Override
  public ObjectWriter member(MinorType type) {
    setType(type);
    return writerFor(type);
  }

  @Override
  public void setType(MinorType type) {
    typeWriter.setInt(type.getNumber());
  }

  private ObjectWriter writerFor(MinorType type) {
    AbstractObjectWriter writer = variants[type.ordinal()];
    if (writer != null) {
      return writer;
    }

    if (listener == null) {
      listener = new DefaultListener();
    }
    writer = (AbstractObjectWriter) listener.addType(type);
    writer.events().bindIndex(index);
    variants[type.ordinal()] = writer;
    if (state != State.IDLE) {
      writer.events().startWrite();
      if (state == State.IN_ROW) {
        writer.events().startRow();
      }
    }
    return writer;
  }

  @Override
  public ScalarWriter scalar(MinorType type) {
    return member(type).scalar();
  }

  @Override
  public TupleWriter tuple() {
    return member(MinorType.MAP).tuple();
  }

  @Override
  public ArrayWriter array() {
    return member(MinorType.LIST).array();
  }

  @Override
  public void setObject(Object value) {
    // TODO Auto-generated method stub
    assert false;
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    this.index = index;
    typeWriter.bindIndex(index);
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().bindIndex(index);
      }
    }
  }

  @Override
  public ColumnWriterIndex writerIndex() {
    return index;
  }

  @Override
  public void startWrite() {
    assert state == State.IDLE;
    state = State.IN_WRITE;
    typeWriter.startWrite();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().startWrite();
      }
    }
  }

  @Override
  public void startRow() {
    assert state == State.IN_WRITE;
    state = State.IN_ROW;
    typeWriter.startRow();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().startRow();
      }
    }
  }

  @Override
  public void endArrayValue() {
    typeWriter.endArrayValue();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().endArrayValue();
      }
    }
  }

  @Override
  public void restartRow() {
    assert state == State.IN_ROW;
    typeWriter.restartRow();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().restartRow();
      }
    }
  }

  @Override
  public void saveRow() {
    assert state == State.IN_ROW;
    typeWriter.saveRow();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().saveRow();
      }
    }
    state = State.IN_WRITE;
  }

  @Override
  public void preRollover() {
    assert state == State.IN_ROW;
    typeWriter.preRollover();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().preRollover();
      }
    }
  }

  @Override
  public void postRollover() {
    assert state == State.IN_ROW;
    typeWriter.postRollover();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().postRollover();
      }
    }
  }

  @Override
  public void endWrite() {
    assert state != State.IDLE;
    typeWriter.endWrite();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().endWrite();
      }
    }
    state = State.IDLE;
  }

  @Override
  public int lastWriteIndex() { return 0; }

  public void dump(HierarchicalFormatter format) {
    // TODO Auto-generated method stub

  }

  @Override
  public void bindListener(VariantWriterListener listener) {
    this.listener = listener;
  }
}