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
import org.apache.drill.exec.vector.accessor.reader.ArrayReaderImpl;
import org.apache.drill.exec.vector.accessor.reader.BaseScalarReader;
import org.apache.drill.exec.vector.accessor.reader.ColumnReaderFactory;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.UnionReaderImpl;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessors.SingleVectorAccessor;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.ListVector;
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
      return buildScalarReader(va);
    }
  }

  private AbstractObjectReader buildScalarReader(VectorAccessor va) {
    BaseScalarReader scalarReader = ColumnReaderFactory.buildColumnReader(va);
    scalarReader.bindVector(va);
    DataMode mode = va.type().getMode();
    switch (mode) {
    case OPTIONAL:
      return BaseScalarReader.buildOptional(va, scalarReader);
    case REQUIRED:
      return BaseScalarReader.buildRequired(scalarReader);
    case REPEATED:
      return ArrayReaderImpl.buildScalar(va, scalarReader);
    default:
      throw new UnsupportedOperationException(mode.toString());
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

    return ArrayReaderImpl.buildTuple(va, mapReader);
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

  @SuppressWarnings("resource")
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

      ValueVector memberVector = ColumnAccessorUtils.getUnionMember(vector, type);
      VectorDescrip memberDescrip = new VectorDescrip(provider, i++, memberVector.getField());
      variants[type.ordinal()] = buildVectorReader(
          new SingleVectorAccessor(memberVector), memberDescrip);
    }
    return UnionReaderImpl.build(
        descrip.metadata.variantSchema(),
        new SingleVectorAccessor(vector),
        variants);
  }

  /**
   * Build a list vector.
   * <p>
   * The list vector is a complex, somewhat ad-hoc structure. It can
   * take the place of repeated vectors, with some extra features.
   * The four "modes" of list vector, and thus list reader, are:
   * <ul>
   * <li>Similar to a scalar array.</li>
   * <li>Similar to a map (tuple) array.</li>
   * <li>The only way to represent an array of unions.</li>
   * <li>The only way to represent an array of lists.</li>
   * </ul>
   * Lists add an extra feature compared to the "regular" scalar or
   * map arrays. Each array entry can be either null or empty (regular
   * arrays can only be empty.)
   * <p>
   * When working with unions, this introduces an ambiguity: both the
   * list and the union have a null flag. Here, we assume that the
   * list flag has precedence, and that if the list entry is not null
   * then the union must also be not null. (Experience will show whether
   * existing code does, in fact, follow that convention.)
   */

  @SuppressWarnings("resource")
  private AbstractObjectReader buildList(VectorAccessor listAccessor,
      VectorDescrip listDescrip) {
    ListVector vector = listAccessor.vector();
    ValueVector dataVector = vector.getDataVector();
    VectorDescrip dataMetadata;
    if (dataVector.getField().getType().getMinorType() == MinorType.UNION) {

      // If the list holds a union, then the list and union are collapsed
      // together in the metadata layer.

      dataMetadata = listDescrip;
    } else {
      dataMetadata = new VectorDescrip(listDescrip.childProvider(), 0, dataVector.getField());
    }
    VectorAccessor dataAccessor = new SingleVectorAccessor(dataVector);
    return ArrayReaderImpl.buildList(listAccessor, buildVectorReader(dataAccessor, dataMetadata));
  }
}
