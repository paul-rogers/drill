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
import org.apache.drill.exec.vector.accessor.ColumnAccessors.UInt1ColumnReader;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.VariantReader;
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

    @Override
    protected ReaderEvents events() { return reader; }
  }

  /**
   * Extract null state from the union vector's type vector. The union reader
   * manages the type reader, so no binding is done here.
   */

  private static class TypeVectorStateReader implements NullStateReader {

    public final UInt1ColumnReader typeReader;

    public TypeVectorStateReader(UInt1ColumnReader typeReader) {
      this.typeReader = typeReader;
    }

    @Override
    public void bindVector(ValueVector vector) { }

    @Override
    public void bindIndex(ColumnReaderIndex rowIndex) { }

    @Override
    public void bindVectorAccessor(VectorAccessor va) { }

    @Override
    public boolean isNull() {
      return typeReader.getInt() == UnionVector.NULL_MARKER;
    }
  }

  /**
   * Null state that handles the strange union semantics that both
   * the union and the values can be null. A value is null if either
   * the union or the value is null. (Though, presumably, in the normal
   * case either the union is null or one of the associated values is
   * null.)
   */

  private static class MemberNullStateReader implements NullStateReader {

    private final NullStateReader unionNullState;
    private final NullStateReader memberNullState;

    public MemberNullStateReader(NullStateReader unionNullState, NullStateReader memberNullState) {
      this.unionNullState = unionNullState;
      this.memberNullState = memberNullState;
    }

    @Override
    public void bindIndex(ColumnReaderIndex rowIndex) {
      memberNullState.bindIndex(rowIndex);
    }

    @Override
    public void bindVector(ValueVector vector) { }

    @Override
    public void bindVectorAccessor(VectorAccessor va) { }

    @Override
    public boolean isNull() {
      return unionNullState.isNull() || memberNullState.isNull();
    }
  }

  /**
   * Handle the awkward situation with complex types. They don't carry their own
   * bits (null state) vector. Instead, we define them as null if the type of
   * the union is other than the type of the map or list. (Since the same vector
   * that holds state also holds the is-null value, this check includes the
   * check if the entire union is null.)
   */

  private static class ComplexMemberStateReader implements NullStateReader {

    private UInt1ColumnReader typeReader;
    private MinorType type;

    public ComplexMemberStateReader(UInt1ColumnReader typeReader, MinorType type) {
      this.typeReader = typeReader;
      this.type = type;
    }

    @Override
    public void bindIndex(ColumnReaderIndex rowIndex) { }

    @Override
    public void bindVector(ValueVector vector) { }

    @Override
    public void bindVectorAccessor(VectorAccessor va) { }

    @Override
    public boolean isNull() {
      return typeReader.getInt() != type.getNumber();
    }
  }

  private final UInt1ColumnReader typeReader;
  private final AbstractObjectReader variants[];
  protected NullStateReader nullStateReader;

  public UnionReaderImpl(AbstractObjectReader variants[]) {
    typeReader = new UInt1ColumnReader();
    typeReader.bindNullState(NullStateReader.REQUIRED_STATE_READER);
    nullStateReader = new TypeVectorStateReader(typeReader);
    assert variants != null  &&  variants.length == MinorType.values().length;
    this.variants = variants;
  }

  public static AbstractObjectReader buildSingle(UnionVector vector, AbstractObjectReader variants[]) {
    UnionReaderImpl reader = new UnionReaderImpl(variants);
    reader.bindVector(vector);
    return new UnionObjectReader(reader);
  }

  @Override
  public void bindVector(ValueVector vector) {
    UnionVector unionVector = (UnionVector) vector;
    typeReader.bindVector(unionVector.getTypeVector());
    rebindMemberNullState();
  }

  @Override
  public void bindNullState(NullStateReader nullStateReader) { }

  @Override
  public NullStateReader nullStateReader() { return nullStateReader; }

  @Override
  public void bindVectorAccessor(MajorType majorType, VectorAccessor va) {
    nullStateReader.bindVectorAccessor(va);
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
        nullReader = new ComplexMemberStateReader(typeReader, type);
        break;
      default:
        nullReader =
            new MemberNullStateReader(nullStateReader,
                objReader.events().nullStateReader());
      }
      objReader.events().bindNullState(nullReader);
    }
  }

  @Override
  public void bindIndex(ColumnReaderIndex index) {
    typeReader.bindIndex(index);
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].bindIndex(index);
      }
    }
  }

  @Override
  public boolean hasType(MinorType type) {

    // Probe the reader because we can't tell if the union has a type
    // without probing for the underlying storage vector.
    // Might be able to probe the MajorType.

    return variants[type.ordinal()] != null;
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
