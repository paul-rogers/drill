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
package org.apache.drill.exec.vector.accessor.reader;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ColumnAccessorUtils;
import org.apache.drill.exec.vector.accessor.ColumnAccessors.UInt1ColumnReader;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.VariantReader;
import org.apache.drill.exec.vector.complex.UnionVector;

public class VariantReaderImpl implements VariantReader {

  public static class VariantObjectReader extends AbstractObjectReader {

    private VariantReaderImpl reader;

    public VariantObjectReader(VariantReaderImpl reader) {
      this.reader = reader;
    }

    @Override
    public ObjectType type() { return ObjectType.VARIANT; }

    @Override
    public VariantReader variant() { return reader; }

    @Override
    public void bindIndex(ColumnReaderIndex index) {
      reader.bindIndex(index);
    }

    @Override
    public Object getObject() {
      return reader.getObject();
    }

    @Override
    public String getAsString() {
      return reader.getAsString();
    }
  }

  private final UnionVector vector;
  private final UInt1ColumnReader typeReader;
  private final AbstractObjectReader variants[] = new AbstractObjectReader[MinorType.values().length];
  private ObjectType objectType;

  private ColumnReaderIndex index;

  public VariantReaderImpl(UnionVector vector) {
    this.vector = vector;
    typeReader = new UInt1ColumnReader();
    typeReader.bindVector(vector.getTypeVector());
  }

  public static AbstractObjectReader build(UnionVector vector) {
    return new VariantObjectReader(new VariantReaderImpl(vector));
  }

  public void bindIndex(ColumnReaderIndex index) {
    this.index = index;
    typeReader.bindIndex(index);
  }

  @Override
  public ObjectType valueType() {
    return objectType;
  }

  @Override
  public boolean hasType(MinorType type) {

    // Probe the reader because we can't tell if the union has a type
    // without probing for the underlying storage vector.
    // Might be able to probe the MajorType.

    return reader(type) != null;
  }

  @Override
  public boolean isNull() {
    return typeReader.getInt() == UnionVector.NULL_MARKER;
  }

  @Override
  public MinorType dataType() {
    int typeCode = typeReader.getInt();
    if (typeCode == UnionVector.NULL_MARKER) {
      return null;
    }
    return MinorType.valueOf(typeCode);
  }

  @Override
  public ObjectReader reader(MinorType type) {
    AbstractObjectReader reader = variants[type.ordinal()];
    if (reader != null) {
      return reader;
    }
    MajorType majorType = vector.getField().getType();
    if (! majorType.getSubTypeList().contains(type)) {
      return null;
    }
    ObjectType targetType;
    switch (type) {
    case MAP:
      targetType = ObjectType.TUPLE;
      break;
    case LIST:
      targetType = ObjectType.ARRAY;
      break;
    case UNION:
      throw new UnsupportedOperationException();
    default:
      targetType = ObjectType.SCALAR;
    }
    if (objectType == null) {
      objectType = targetType;
    } else if (objectType != targetType) {
      throw new UnsupportedOperationException();
    }

    // This call will create the vector if it does not yet exist.
    // Will throw an exception for unspported types.
    // so call this only if the MajorType reports that the type
    // already exists.

    ValueVector memberVector = ColumnAccessorUtils.getUnionMember(vector, type);
    reader = ColumnReaderFactory.buildColumnReader(memberVector);
    reader.bindIndex(index);
    variants[type.ordinal()] = reader;
    return reader;
  }

  private ObjectReader requireReader(MinorType type) {
    ObjectReader reader = reader(type);
    if (reader == null) {
      throw new IllegalArgumentException("Union does not include type " + type.toString());
    }
    return reader;
  }

  @Override
  public ScalarReader scalar(MinorType type) {
    return requireReader(type).scalar();
  }

  @Override
  public ObjectReader reader() {
    MinorType type = dataType();
    if (type == null) {
      return null;
    }
    return reader(type);
  }

  @Override
  public ScalarReader scalar() {
    ObjectReader reader = reader();
    if (reader == null) {
      return null;
    }
    return reader.scalar();
  }

  @Override
  public TupleReader tuple() {
    return requireReader(MinorType.MAP).tuple();
  }

  @Override
  public ArrayReader array() {
    return requireReader(dataType()).array();
  }

  @Override
  public Object getObject() {
    MinorType type = dataType();
    if (type == null) {
      return null;
    }
    return requireReader(type).getObject();
  }

  @Override
  public String getAsString() {
    MinorType type = dataType();
    if (type == null) {
      return "null";
    }
    return requireReader(type).getAsString();
  }
}
