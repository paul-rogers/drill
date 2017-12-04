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
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.VariantMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
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
  }

  public static class UnionVectorShim implements UnionShim {

    private final UnionVector vector;
    private final AbstractObjectWriter variants[];
    private UnionWriterImpl writer;
    private final BaseScalarWriter typeWriter;
    private VariantWriterListener listener;

    public UnionVectorShim(UnionVector vector,
        AbstractObjectWriter variants[]) {
      this.vector = vector;
      typeWriter = ColumnWriterFactory.newWriter(vector.getTypeVector());
      if (variants == null) {
        this.variants = new AbstractObjectWriter[MinorType.values().length];
      } else {
        this.variants = variants;
      }
    }

    @Override
    public void bindWriter(UnionWriterImpl writer) {
      this.writer = writer;
    }

    @Override
    public void bindIndex(ColumnWriterIndex index) {
      typeWriter.bindIndex(index);
      for (int i = 0; i < variants.length; i++) {
        if (variants[i] != null) {
          variants[i].events().bindIndex(index);
        }
      }
    }

    @Override
    public void setNull() {

      // Not really necessary: the default value is 0.
      // This lets a caller change its mind after setting a
      // value.

      typeWriter.setInt(UnionVector.NULL_MARKER);
    }

    @Override
    public boolean hasType(MinorType type) {
      return variants[type.ordinal()] != null;
    }

    private ObjectWriter writerFor(MinorType type) {
      AbstractObjectWriter writer = variants[type.ordinal()];
      if (writer != null) {
        return writer;
      }
      if (listener == null) {
        listener = new DefaultListener(this);
      }
      return addMember(type);
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

    @Override
    public ObjectWriter addMember(ColumnMetadata schema) {
      AbstractObjectWriter writer = (AbstractObjectWriter) listener.addMember(schema);
      addMember(schema.type(), writer);
      return writer;
    }

    @Override
    public ObjectWriter addMember(MinorType type) {
      AbstractObjectWriter writer = (AbstractObjectWriter) listener.addType(type);
      addMember(type, writer);
      return writer;
    }

    private void addMember(MinorType type, AbstractObjectWriter colWriter) {
      colWriter.events().bindIndex(writer.index());
      variants[type.ordinal()] = colWriter;
      if (writer.state() != State.IDLE) {
        colWriter.events().startWrite();
        if (writer.state() == State.IN_ROW) {
          colWriter.events().startRow();
        }
      }
    }

    /**
     * Add a column writer to an existing union writer. Used for implementations
     * that support "live" schema evolution: column discovery while writing.
     * The corresponding metadata must already have been added to the schema.
     *
     * @param colWriter the column writer to add
     */

    public void addColumnWriter(AbstractObjectWriter colWriter) {
      MinorType type = colWriter.schema().type();
      assert variants[type.ordinal()] == null;
      assert writer.variantSchema().hasType(type);
      addMember(type, colWriter);
    }

    @Override
    public void startWrite() {
      typeWriter.startWrite();
      for (int i = 0; i < variants.length; i++) {
        if (variants[i] != null) {
          variants[i].events().startWrite();
        }
      }
    }

    @Override
    public void startRow() {
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
      typeWriter.restartRow();
      for (int i = 0; i < variants.length; i++) {
        if (variants[i] != null) {
          variants[i].events().restartRow();
        }
      }
    }

    @Override
    public void saveRow() {
      typeWriter.saveRow();
      for (int i = 0; i < variants.length; i++) {
        if (variants[i] != null) {
          variants[i].events().saveRow();
        }
      }
     }

    @Override
    public void preRollover() {
      typeWriter.preRollover();
      for (int i = 0; i < variants.length; i++) {
        if (variants[i] != null) {
          variants[i].events().preRollover();
        }
      }
    }

    @Override
    public void postRollover() {
      typeWriter.postRollover();
      for (int i = 0; i < variants.length; i++) {
        if (variants[i] != null) {
          variants[i].events().postRollover();
        }
      }
    }

    @Override
    public void endWrite() {
      typeWriter.endWrite();
      for (int i = 0; i < variants.length; i++) {
        if (variants[i] != null) {
          variants[i].events().endWrite();
        }
      }
    }


    /**
     * Return the writer for the types vector. To be used only by the row set
     * loader overflow logic; never by the application (which is why the method
     * is not defined in the interface.)
     *
     * @return the writer for the types vector
     */

    public ColumnWriter typeWriter() { return typeWriter; }

    @Override
    public int lastWriteIndex() { return typeWriter.lastWriteIndex(); }

    @Override
    public int rowStartIndex() { return typeWriter.rowStartIndex(); }

    public void bindListener(VariantWriterListener listener) {
      this.listener = listener;
    }
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

  private static class DefaultListener implements VariantWriterListener {

    private UnionVectorShim shim;

    private DefaultListener(UnionVectorShim shim) {
      this.shim = shim;
    }

    @Override
    public ObjectWriter addType(MinorType type) {
      ValueVector memberVector = shim.vector.getMember(type);
      ColumnMetadata memberSchema = shim.writer.variantSchema().addType(type);
      return ColumnWriterFactory.buildColumnWriter(memberSchema, memberVector);
    }

    @Override
    public ObjectWriter addMember(ColumnMetadata schema) {
      throw new UnsupportedOperationException();
    }
  }

  private final ColumnMetadata schema;
  private UnionShim shim;
  private ColumnWriterIndex index;
  private State state = State.IDLE;

  public UnionWriterImpl(ColumnMetadata schema, UnionShim shim) {
    this.schema = schema;
    this.shim = shim;
    shim.bindWriter(this);
  }

  public UnionWriterImpl(ColumnMetadata schema, UnionVector vector,
      AbstractObjectWriter variants[]) {
    this(schema, new UnionVectorShim(vector, variants));
  }

  public State state() { return state; }
  public ColumnWriterIndex index() { return index; }

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
  public void bindIndex(ColumnWriterIndex index) {
    this.index = index;
    shim.bindIndex(index);
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