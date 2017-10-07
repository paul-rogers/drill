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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.ValueVector;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/UnionVector.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex;

<#include "/@includes/vv_imports.ftl" />
import java.util.Iterator;
import java.util.Set;

import org.apache.drill.exec.vector.complex.impl.ComplexCopier;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.record.MaterializedField;

import com.google.common.annotations.VisibleForTesting;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

/**
 * A vector which can hold values of different types. It does so by using a
 * MapVector which contains a vector for each primitive type that is stored.
 * MapVector is used in order to take advantage of its
 * serialization/deserialization methods, as well as the addOrGet method.
 *
 * For performance reasons, UnionVector stores a cached reference to each
 * subtype vector, to avoid having to do the map lookup each time the vector is
 * accessed.
 */
public class UnionVector implements ValueVector {
  
  public static final int NULL_MARKER = 0;
  public static final String MAP_NAME = "$map$";
  public static final String TYPES_NAME = "$types$";
  private static final MajorType MAJOR_TYPES[] = new MajorType[MinorType.values().length];
  
  static {
    MAJOR_TYPES[MinorType.MAP.ordinal()] = Types.optional(MinorType.MAP);
    MAJOR_TYPES[MinorType.LIST.ordinal()] = Types.optional(MinorType.LIST);
    <#list vv.types as type>
      <#list type.minor as minor>
        <#assign name = minor.class?cap_first />
        <#assign fields = minor.fields!type.fields />
        <#assign uncappedName = name?uncap_first/>
        <#if !minor.class?starts_with("Decimal")>
    MAJOR_TYPES[MinorType.${name?upper_case}.ordinal()] = Types.optional(MinorType.${name?upper_case});
        </#if>
      </#list>
    </#list>
  }

  private MaterializedField field;
  private BufferAllocator allocator;
  private Accessor accessor = new Accessor();
  private Mutator mutator = new Mutator();
  private int valueCount;

  private MapVector internalMap;
  private UInt1Vector typeVector;

  private ValueVector subtypes[] = new ValueVector[MinorType.values().length];

  private FieldReader reader;

  private final CallBack callBack;

  public UnionVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this.field = field.clone();
    this.allocator = allocator;
    internalMap = new MapVector(MAP_NAME, allocator, callBack);
    typeVector = new UInt1Vector(
        MaterializedField.create(TYPES_NAME, Types.required(MinorType.UINT1)),
        allocator);
    this.field.addChild(internalMap.getField().clone());
    this.field.addChild(typeVector.getField());
    this.callBack = callBack;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  public List<MinorType> getSubTypes() {
    return field.getType().getSubTypeList();
  }
  
  @SuppressWarnings("unchecked")
  public <T extends ValueVector> T subtype(MinorType type) {
    return (T) subtypes[type.ordinal()];
  }
  
  /**
   * Add an externally-created subtype vector. The vector must represent a type that
   * does not yet exist in the union, and must be of OPTIONAL mode. Does not call
   * the callback since the client (presumably) knows that it is adding the type.
   * The caller must also allocate the buffer for the vector.
   * 
   * @param vector subtype vector to add
   */
  
  public void addType(ValueVector vector) {
    MinorType type = vector.getField().getType().getMinorType();
    assert subtype(type) == null;
    assert vector.getField().getType().getMode() == DataMode.OPTIONAL;
    subtypes[type.ordinal()] = vector;
    internalMap.putChild(type.name(), vector);
    addSubtypeMetadata(type);
  }
  
  // Called from SchemaUtil
  
  public void addSubType(MinorType type) {
    if (subtype(type) != null) {
      return;
    }
    addSubtypeMetadata(type);
  }
  
  // Called from SchemaUtil
  
  public ValueVector addVector(ValueVector v) {
    MajorType majorType = v.getField().getType();
    MinorType type = majorType.getMinorType();
    String name = type.name();
    Preconditions.checkState(internalMap.getChild(name) == null, String.format("%s vector already exists", name));
    final ValueVector newVector = internalMap.addOrGet(name, majorType, BasicTypeHelper.getValueVectorClass(type, majorType.getMode()));
    v.makeTransferPair(newVector).transfer();
    internalMap.putChild(name, newVector);
    subtypes[type.ordinal()] = v;
    addSubType(type);
    return newVector;
  }

  private void addSubtypeMetadata(MinorType type) {
    field = MaterializedField.create(field.getName(),
        MajorType.newBuilder(field.getType()).addSubType(type).build());
  }

  /**
   * "Classic" way to add a subtype when working directly with a union vector.
   * Creates the vector, adds it to the internal structures and creates a
   * new buffer of the default size.
   * 
   * @param type the type to add
   * @param vectorClass class of the vector to create
   * @return typed form of the new value vector
   */
  
  private <T extends ValueVector> T classicAddType(MinorType type, Class<? extends ValueVector> vectorClass) {
    @SuppressWarnings("unchecked")
    T vector = (T) internalMap.addOrGet(type.name(), MAJOR_TYPES[type.ordinal()], vectorClass);
    vector.allocateNew();
    subtypes[type.ordinal()] = vector;
    addSubtypeMetadata(type);
    if (callBack != null) {
      callBack.doWork();
    }
    return vector;
  }

  public MapVector getMap() {
    MapVector mapVector = subtype(MinorType.MAP);
    if (mapVector == null) {
      mapVector = classicAddType(MinorType.MAP, MapVector.class);
    }
    return mapVector;
  }

  public ListVector getList() {
    ListVector listVector = subtype(MinorType.LIST);
    if (listVector == null) {
      listVector = classicAddType(MinorType.LIST, ListVector.class);
    }
    return listVector;
  }
  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.class?starts_with("Decimal")>

  public Nullable${name}Vector get${name}Vector() {
    Nullable${name}Vector vector = subtype(MinorType.${name?upper_case});
    if (vector == null) {
      vector = classicAddType(MinorType.${name?upper_case}, Nullable${name}Vector.class);
    }
    return vector;
  }
      </#if>
    </#list>
  </#list>

  /**
   * Add or get a type member given the type.
   * 
   * @param type the type of the vector to retrieve
   * @return the (potentially newly created) vector that backs the given type
   */
  
  public ValueVector getMember(MinorType type) {
    switch (type) {
    case MAP:
      return getMap();
    case LIST:
      return getList();
  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.class?starts_with("Decimal")>
    case ${name?upper_case}:
      return get${name}Vector();
      </#if>
    </#list>
  </#list>
    default:
      throw new UnsupportedOperationException(type.toString());
    }
  }
  
  @SuppressWarnings("unchecked")
  public <T extends ValueVector> T member(MinorType type) {
    return (T) getMember(type);
  }

  public int getTypeValue(int index) {
    return typeVector.getAccessor().get(index);
  }

  public UInt1Vector getTypeVector() {
    return typeVector;
  }
  
  @VisibleForTesting
  public MapVector getTypeMap() {
    return internalMap;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    internalMap.allocateNew();
    if (typeVector != null) {
      typeVector.allocateNew();
    }
  }

  public void allocateNew(int rowCount) throws OutOfMemoryException {
    internalMap.allocateNew();
    if (typeVector != null) {
      typeVector.allocateNew(rowCount);
    }
  }

  @Override
  public boolean allocateNewSafe() {
    boolean safe = internalMap.allocateNewSafe();
    if (safe) {
      if (typeVector != null) {
        typeVector.allocateNewSafe();
      }
    }
    return safe;
  }

  @Override
  public void setInitialCapacity(int numRecords) { }

  @Override
  public int getValueCapacity() {
    return Math.min(typeVector.getValueCapacity(), internalMap.getValueCapacity());
  }

  @Override
  public void close() { }

  @Override
  public void clear() {
    typeVector.clear();
    internalMap.clear();
  }

  @Override
  public MaterializedField getField() { return field; }

  @Override
  public void collectLedgers(Set<BufferLedger> ledgers) {
    internalMap.collectLedgers(ledgers);
    typeVector.collectLedgers(ledgers);
  }

  @Override
  public int getPayloadByteCount(int valueCount) {
    return typeVector.getPayloadByteCount(valueCount) +
           internalMap.getPayloadByteCount(valueCount);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(field, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(field.withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((UnionVector) target);
  }

  public void transferTo(UnionVector target) {
    internalMap.makeTransferPair(target.internalMap).transfer();
    typeVector.makeTransferPair(target.typeVector).transfer();
    target.valueCount = valueCount;
    target.field = field;
  }

  public void copyFrom(int inIndex, int outIndex, UnionVector from) {
    typeVector.copyFrom(inIndex, outIndex, from.typeVector);
    from.getReader().setPosition(inIndex);
    getWriter().setPosition(outIndex);
    ComplexCopier.copy(from.reader, mutator.writer);
  }

  public void copyFromSafe(int inIndex, int outIndex, UnionVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    copyFromSafe(fromIndex, toIndex, (UnionVector) from);
  }

  @Override
  public void toNullable(ValueVector nullableVector) {
    throw new UnsupportedOperationException();
  }

  private class TransferImpl implements TransferPair {

    private final UnionVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator) {
      to = new UnionVector(field, allocator, null);
    }

    public TransferImpl(UnionVector to) {
      this.to = to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) { }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, UnionVector.this);
    }
  }

  @Override
  public Accessor getAccessor() { return accessor; }

  @Override
  public Mutator getMutator() { return mutator; }

  @Override
  public FieldReader getReader() {
    if (reader == null) {
      reader = new UnionReader(this);
    }
    return reader;
  }

  public FieldWriter getWriter() {
    if (mutator.writer == null) {
      mutator.writer = new UnionWriter(this);
    }
    return mutator.writer;
  }

  @Override
  public UserBitShared.SerializedField getMetadata() {
    SerializedField.Builder b = getField() //
            .getAsBuilder() //
            .setBufferLength(getBufferSize()) //
            .setValueCount(valueCount);

    b.addChild(internalMap.getMetadata());
    return b.build();
  }

  @Override
  public int getBufferSize() {
    return internalMap.getBufferSize();
  }

  @Override
  public int getAllocatedSize() {
    return internalMap.getAllocatedSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = 0;
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize;
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    return internalMap.getBuffers(clear);
  }

  @Override
  public void load(UserBitShared.SerializedField metadata, DrillBuf buffer) {
    valueCount = metadata.getValueCount();

    internalMap.load(metadata.getChild(0), buffer);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    List<ValueVector> vectors = Lists.newArrayList(internalMap.iterator());
    vectors.add(typeVector);
    return vectors.iterator();
  }

  public class Accessor extends BaseValueVector.BaseAccessor {

    @Override
    public Object getObject(int index) {
      int type = typeVector.getAccessor().get(index);
      switch (type) {
      case NULL_MARKER:
        return null;
      <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.class?starts_with("Decimal")>
      case MinorType.${name?upper_case}_VALUE:
        return get${name}Vector().getAccessor().getObject(index);
      </#if>
      </#list></#list>
      case MinorType.MAP_VALUE:
        return getMap().getAccessor().getObject(index);
      case MinorType.LIST_VALUE:
        return getList().getAccessor().getObject(index);
      default:
        throw new UnsupportedOperationException("Cannot support type: " + MinorType.valueOf(type));
      }
    }

    public byte[] get(int index) { return null; }

    public void get(int index, ComplexHolder holder) { }

    public void get(int index, UnionHolder holder) {
      FieldReader reader = new UnionReader(UnionVector.this);
      reader.setPosition(index);
      holder.reader = reader;
    }

    @Override
    public int getValueCount() { return valueCount; }

    @Override
    public boolean isNull(int index) {
      
      // Note that type code == 0 is used to indicate a null.
      // This corresponds to the LATE type, not the NULL type.
      // This is presumably an artifact of an earlier implementation...
      
      return typeVector.getAccessor().get(index) == NULL_MARKER;
    }

    public int isSet(int index) {
      return isNull(index) ? 0 : 1;
    }
  }

  public class Mutator extends BaseValueVector.BaseMutator {

    protected UnionWriter writer;

    @Override
    public void setValueCount(int valueCount) {
      UnionVector.this.valueCount = valueCount;
      internalMap.getMutator().setValueCount(valueCount);
    }

    public void setSafe(int index, UnionHolder holder) {
      FieldReader reader = holder.reader;
      if (writer == null) {
        writer = new UnionWriter(UnionVector.this);
      }
      writer.setPosition(index);
      MinorType type = reader.getType().getMinorType();
      switch (type) {
      <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.class?starts_with("Decimal")>
      case ${name?upper_case}:
        Nullable${name}Holder ${uncappedName}Holder = new Nullable${name}Holder();
        reader.read(${uncappedName}Holder);
        setSafe(index, ${uncappedName}Holder);
        break;
      </#if>
      </#list></#list>
      case MAP:
        ComplexCopier.copy(reader, writer);
        break;
      case LIST:
        ComplexCopier.copy(reader, writer);
        break;
      default:
        throw new UnsupportedOperationException();
      }
    }

    <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
    <#assign fields = minor.fields!type.fields />
    <#assign uncappedName = name?uncap_first/>
    <#if !minor.class?starts_with("Decimal")>
    public void setSafe(int index, Nullable${name}Holder holder) {
      setType(index, MinorType.${name?upper_case});
      get${name}Vector().getMutator().setSafe(index, holder);
    }

    </#if>
    </#list></#list>

    public void setType(int index, MinorType type) {
      typeVector.getMutator().setSafe(index, type.getNumber());
    }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) { }
  }

  @Override
  public void exchange(ValueVector other) {
    throw new UnsupportedOperationException("Union vector does not yet support exchange()");
  }
}
