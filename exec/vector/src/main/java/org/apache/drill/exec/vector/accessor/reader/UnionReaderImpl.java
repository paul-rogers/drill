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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.VariantMetadata;
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
import org.apache.drill.exec.vector.accessor.reader.VectorAccessors.BaseHyperVectorAccessor;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessors.SingleVectorAccessor;
import org.apache.drill.exec.vector.complex.UnionVector;

public class UnionReaderImpl implements VariantReader, ReaderEvents {

  public static class UnionObjectReader extends AbstractObjectReader {

    private UnionReaderImpl reader;

    public UnionObjectReader(UnionReaderImpl reader) {
      this.reader = reader;
    }

    @Override
    public ObjectType type() { return ObjectType.VARIANT; }

    @Override
    public VariantReader variant() { return reader; }

    @Override
    public Object getObject() {
      return reader.getObject();
    }

    @Override
    public String getAsString() {
      return reader.getAsString();
    }

    @Override
    protected ReaderEvents events() { return reader; }
  }

  private static class HyperTypeVectorAccessor extends BaseHyperVectorAccessor {

    private VectorAccessor unionVectorAccessor;

    private HyperTypeVectorAccessor(VectorAccessor va) {
      super(Types.required(MinorType.UINT1));
      unionVectorAccessor = va;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      UnionVector vector = unionVectorAccessor.vector();
      return (T) vector.getTypeVector();
    }
  }

  private static class HyperMemberVectorAccessor extends BaseHyperVectorAccessor {

    private final VectorAccessor unionVectorAccessor;

    private HyperMemberVectorAccessor(VectorAccessor va, MinorType memberType) {
      super(Types.optional(memberType)); // TODO: Handle map and list
      unionVectorAccessor = va;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      UnionVector vector = unionVectorAccessor.vector();
      return (T) ColumnAccessorUtils.getUnionMember(vector, type.getMinorType());
    }
  }

  private final VariantMetadata schema;
  private final UInt1ColumnReader typeReader;
  private final AbstractObjectReader variants[];
  protected NullStateReader nullStateReader;

  public UnionReaderImpl(VariantMetadata schema, VectorAccessor va, AbstractObjectReader variants[]) {
    this.schema = schema;
    typeReader = new UInt1ColumnReader();
    typeReader.bindNullState(NullStateReaders.REQUIRED_STATE_READER);
    VectorAccessor typeAccessor;
    if (va.isHyper()) {
      typeAccessor = new HyperTypeVectorAccessor(va);
    } else {
      UnionVector unionVector = va.vector();
      typeAccessor = new SingleVectorAccessor(unionVector.getTypeVector());
    }
    typeReader.bindVector(typeAccessor);
    nullStateReader = new NullStateReaders.TypeVectorStateReader(typeReader);
    assert variants != null  &&  variants.length == MinorType.values().length;
    this.variants = variants;
    rebindMemberNullState();
  }

  /**
   * Rebind the null state reader to include the union's own state.
   */

  private void rebindMemberNullState() {
    for (int i = 0; i < variants.length; i++) {
      AbstractObjectReader objReader = variants[i];
      if (objReader == null) {
        continue;
      }
      NullStateReader nullReader;
      MinorType type = MinorType.valueOf(i);
      switch(type) {
      case MAP:
      case LIST:
        nullReader = new NullStateReaders.ComplexMemberStateReader(typeReader, type);
        break;
      default:
        nullReader =
            new NullStateReaders.MemberNullStateReader(nullStateReader,
                objReader.events().nullStateReader());
      }
      objReader.events().bindNullState(nullReader);
    }
  }

  public static VectorAccessor memberHyperAccessor(VectorAccessor unionAccessor, MinorType memberType) {
    return new HyperMemberVectorAccessor(unionAccessor, memberType);
  }

  public static AbstractObjectReader build(VariantMetadata schema, VectorAccessor va, AbstractObjectReader variants[]) {
    return new UnionObjectReader(new UnionReaderImpl(schema, va, variants));
  }

  @Override
  public void bindNullState(NullStateReader nullStateReader) { }

  @Override
  public NullStateReader nullStateReader() { return nullStateReader; }
  @Override
  public void bindIndex(ColumnReaderIndex index) {
    typeReader.bindIndex(index);
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().bindIndex(index);
      }
    }
  }

  @Override
  public VariantMetadata schema() { return schema; }

  @Override
  public int size() { return schema.size(); }

  @Override
  public boolean hasType(MinorType type) {

    // Probe the reader because we can't tell if the union has a type
    // without probing for the underlying storage vector.
    // Might be able to probe the MajorType.

    return variants[type.ordinal()] != null;
  }

  @Override
  public void reposition() {
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().reposition();
      }
    }
  }

  @Override
  public boolean isNull() {
    return nullStateReader.isNull();
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
  public ObjectReader member(MinorType type) {
    return variants[type.ordinal()];
  }

  private ObjectReader requireReader(MinorType type) {
    ObjectReader reader = member(type);
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
  public ObjectReader member() {
    MinorType type = dataType();
    if (type == null) {
      return null;
    }
    return member(type);
  }

  @Override
  public ScalarReader scalar() {
    ObjectReader reader = member();
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
