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
package org.apache.drill.exec.physical.rowSet.model.single;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.VectorDescrip;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessorUtils;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.ColumnReaderFactory;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.ObjectArrayReader;
import org.apache.drill.exec.vector.accessor.reader.UnionReaderImpl;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor.SingleVectorAccessor;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.UnionVector;

public abstract class BaseReaderBuilder {

  protected List<AbstractObjectReader> buildContainerChildren(
      VectorContainer container, MetadataProvider mdProvider) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      VectorDescrip descrip = new VectorDescrip(mdProvider, i, vector.getField());
      readers.add(buildVectorReader(new SingleVectorAccessor(vector), descrip));
    }
    return readers;
  }

  protected AbstractObjectReader buildVectorReader(VectorAccessor va, VectorDescrip descrip) {
    MajorType type = va.type();

    switch(type.getMinorType()) {
    case MAP:
      return buildMap(va, type, descrip);
    case UNION:
      return buildUnion(va, descrip);
    case LIST:
      return buildList(va, descrip);
    default:
      return ColumnReaderFactory.buildColumnReader(va);
    }
  }

  private AbstractObjectReader buildMap(VectorAccessor va, MajorType type, VectorDescrip descrip) {

    // Map type

    AbstractObjectReader mapReader = MapReader.build(
        descrip.metadata.mapSchema(),
        buildMap(va,
            descrip.parent.childProvider(descrip.metadata)));

    // Single map

    if (type.getMode() != DataMode.REPEATED) {
      return mapReader;
    }

    // Repeated map

    return ObjectArrayReader.build(va, mapReader);
  }

  @SuppressWarnings("resource")
  protected List<AbstractObjectReader> buildMap(VectorAccessor mapAccessor, MetadataProvider provider) {
    AbstractMapVector mapVector = mapAccessor.vector();
    List<AbstractObjectReader> readers = new ArrayList<>();
    int i = 0;
    for (ValueVector vector : mapVector) {
      VectorDescrip descrip = new VectorDescrip(provider, i, vector.getField());
      readers.add(buildVectorReader(new SingleVectorAccessor(vector), descrip));
      i++;
    }
    return readers;
  }

  private AbstractObjectReader buildUnion(VectorAccessor unionAccessor, VectorDescrip descrip) {
    MetadataProvider provider = descrip.childProvider();
    UnionVector vector = unionAccessor.vector();
    final AbstractObjectReader variants[] = new AbstractObjectReader[MinorType.values().length];
    int i = 0;
    for (MinorType type : vector.getField().getType().getSubTypeList()) {

      // This call will create the vector if it does not yet exist.
      // Will throw an exception for unsupported types.
      // so call this only if the MajorType reports that the type
      // already exists.

      @SuppressWarnings("resource")
      ValueVector memberVector = ColumnAccessorUtils.getUnionMember(vector, type);
      VectorDescrip memberDescrip = new VectorDescrip(provider, i++, memberVector.getField());
      variants[type.ordinal()] = buildVectorReader(memberVector, memberDescrip);
    }
    return UnionReaderImpl.buildSingle(descrip.metadata.variantSchema(), vector, variants);
  }

  @SuppressWarnings("resource")
  private AbstractObjectReader buildList(VectorAccessor listAccessor,
      VectorDescrip listDescrip) {
    ListVector vector = listAccessor.vector();
    ValueVector dataVector = vector.getDataVector();
    VectorDescrip dataMetadata = new VectorDescrip(listDescrip.childProvider(), 0, dataVector.getField());
    MajorType type = dataVector.getField().getType();
    switch(type.getMinorType()) {
    case MAP:
      return ListReaderImpl.build(vector, buildMap(dataVector, type, dataMetadata));
    case UNION:
    case LIST:
      return ListReaderImpl.build(vector, buildUnion((UnionVector) dataVector, dataMetadata));
    default:
      return ColumnReaderFactory.buildScalarList(vector, dataVector);
    }
  }
}
