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

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.VariantMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.accessor.WriterPosition;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.complex.UnionVector;
import org.joda.time.Period;

/**
 * Writer to a union vector.
 */

public class UnionWriterImpl implements VariantWriter, WriterEvents {

  public interface UnionShim extends WriterEvents {
    void bindWriter(UnionWriterImpl writer);
    void setNull();
    boolean hasType(MinorType type);
    ObjectWriter member(MinorType type);
    void setType(MinorType type);
    int lastWriteIndex();
    int rowStartIndex();
    ObjectWriter addMember(ColumnMetadata colSchema);
    ObjectWriter addMember(MinorType type);
    void addMember(AbstractObjectWriter colWriter);
  }

  public static class VariantObjectWriter extends AbstractObjectWriter {

    private final UnionWriterImpl writer;

    public VariantObjectWriter(UnionWriterImpl writer) {
      this.writer = writer;
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

  /**
   * The result set loader requires information about the child positions
   * of the array that this list represents. Since the children are mutable,
   * we cannot simply ask for the child writer as with most arrays. Instead,
   * we use a proxy that will route the request to the current shim. This
   * way the proxy persists even as the shims change. Just another nasty
   * side-effect of the overly-complex list structure...
   * <p>
   * This class is needed because both the child and this array writer
   * need to implement the same methods, so we can't just implement these
   * methods on the union writer itself.
   */

  private class ElementPositions implements WriterPosition {

    @Override
    public int rowStartIndex() { return shim.rowStartIndex(); }

    @Override
    public int lastWriteIndex() { return shim.lastWriteIndex(); }
  }

  private final ColumnMetadata schema;
  private UnionShim shim;
  private ColumnWriterIndex index;
  private State state = State.IDLE;
  private VariantWriterListener listener;
  private final WriterPosition elementPosition = new ElementPositions();

  public UnionWriterImpl(ColumnMetadata schema) {
    this.schema = schema;
  }

  public UnionWriterImpl(ColumnMetadata schema, UnionVector vector,
      AbstractObjectWriter variants[]) {
    this(schema);
    bindShim(new UnionVectorShim(vector, variants));
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    this.index = index;
    shim.bindIndex(index);
  }

  public void bindListener(VariantWriterListener listener) {
    this.listener = listener;
  }

  // The following are for coordinating with the shim.

  public State state() { return state; }
  public ColumnWriterIndex index() { return index; }
  public VariantWriterListener listener() { return listener; }
  public UnionShim shim() { return shim; }
  public WriterPosition elementPosition() { return elementPosition; }

  public void bindShim(UnionShim shim) {
    this.shim = shim;
    shim.bindWriter(this);
  }

  @Override
  public ObjectType type() { return ObjectType.VARIANT; }

  @Override
  public ColumnMetadata schema() { return schema; }

  @Override
  public VariantMetadata variantSchema() { return schema.variantSchema(); }

  @Override
  public int size() { return variantSchema().size(); }

  @Override
  public boolean hasType(MinorType type) {
    return shim.hasType(type);
  }

  @Override
  public void setNull() {
    shim.setNull();
  }

  @Override
  public ObjectWriter member(MinorType type) {
    return shim.member(type);
  }

  @Override
  public void setType(MinorType type) {
    shim.setType(type);
  }

  @Override
  public ObjectWriter addMember(ColumnMetadata colSchema) {
    return shim.addMember(colSchema);
  }

  @Override
  public ObjectWriter addMember(MinorType type) {
    return shim.addMember(type);
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
  public void startWrite() {
    assert state == State.IDLE;
    state = State.IN_WRITE;
    shim.startWrite();
  }

  @Override
  public void startRow() {
    assert state == State.IN_WRITE;
    state = State.IN_ROW;
    shim.startRow();
  }

  @Override
  public void endArrayValue() {
    shim.endArrayValue();
  }

  @Override
  public void restartRow() {
    assert state == State.IN_ROW;
    shim.restartRow();
  }

  @Override
  public void saveRow() {
    assert state == State.IN_ROW;
    shim.saveRow();
    state = State.IN_WRITE;
  }

  @Override
  public void preRollover() {
    assert state == State.IN_ROW;
    shim.preRollover();
  }

  @Override
  public void postRollover() {
    assert state == State.IN_ROW;
    shim.postRollover();
  }

  @Override
  public void endWrite() {
    assert state != State.IDLE;
    shim.endWrite();
    state = State.IDLE;
  }

  @Override
  public int lastWriteIndex() { return shim.lastWriteIndex(); }

  @Override
  public int rowStartIndex() { return shim.rowStartIndex(); }

  @Override
  public void setObject(Object value) {
    if (value == null) {
      setNull();
    } else if (value instanceof Integer) {
      scalar(MinorType.INT).setInt((Integer) value);
    } else if (value instanceof Long) {
      scalar(MinorType.BIGINT).setLong((Long) value);
    } else if (value instanceof String) {
      scalar(MinorType.VARCHAR).setString((String) value);
    } else if (value instanceof BigDecimal) {
      // Can look for exactly one decimal type as is done for Object[] below
      throw new IllegalArgumentException("Decimal is ambiguous, please use scalar(type)");
    } else if (value instanceof Period) {
      // Can look for exactly one period type as is done for Object[] below
      throw new IllegalArgumentException("Period is ambiguous, please use scalar(type)");
    } else if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      scalar(MinorType.VARBINARY).setBytes(bytes, bytes.length);
    } else if (value instanceof Byte) {
      scalar(MinorType.TINYINT).setInt((Byte) value);
    } else if (value instanceof Short) {
      scalar(MinorType.SMALLINT).setInt((Short) value);
    } else if (value instanceof Double) {
      scalar(MinorType.FLOAT8).setDouble((Double) value);
    } else if (value instanceof Float) {
      scalar(MinorType.FLOAT4).setDouble((Float) value);
    } else if (value instanceof Object[]) {
      if (hasType(MinorType.MAP) && hasType(MinorType.LIST)) {
        throw new UnsupportedOperationException("Union has both a map and a list, so Object[] is ambiguous");
      } else if (hasType(MinorType.MAP)) {
        tuple().setObject(value);
      } else if (hasType(MinorType.LIST)) {
        array().setObject(value);
      } else {
        throw new IllegalArgumentException("Unsupported type " +
            value.getClass().getSimpleName());
      }
    } else {
      throw new IllegalArgumentException("Unsupported type " +
                value.getClass().getSimpleName());
    }
  }

  public void dump(HierarchicalFormatter format) {
    // TODO Auto-generated method stub

  }
}