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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.VectorDescrip;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessorUtils;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.ColumnWriterFactory;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.UnionVector;

/**
 * Build a set of writers for a single (non-hyper) vector container.
 */

public abstract class BaseWriterBuilder {

  protected List<AbstractObjectWriter> buildContainerChildren(VectorContainer container, MetadataProvider mdProvider) {
    List<AbstractObjectWriter> writers = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      VectorDescrip descrip = new VectorDescrip(mdProvider, i, vector.getField());
      writers.add(buildVectorWriter(vector, descrip));
    }
    return writers;
  }

  private AbstractObjectWriter buildVectorWriter(ValueVector vector, VectorDescrip descrip) {
    MajorType type = vector.getField().getType();
    switch (type.getMinorType()) {
    case MAP:
      return ColumnWriterFactory.buildMapWriter(descrip.metadata,
          (AbstractMapVector) vector,
          buildMap((AbstractMapVector) vector, descrip));

    case UNION:
      return buildUnion((UnionVector) vector, descrip);

    case LIST:
      return buildList((ListVector) vector, descrip);

    default:
      return ColumnWriterFactory.buildColumnWriter(descrip.metadata, vector);
    }
  }

  private List<AbstractObjectWriter> buildMap(AbstractMapVector vector, VectorDescrip descrip) {
    List<AbstractObjectWriter> writers = new ArrayList<>();
    MetadataProvider provider = descrip.parent.childProvider(descrip.metadata);
    int i = 0;
    for (ValueVector child : vector) {
      VectorDescrip childDescrip = new VectorDescrip(provider, i, child.getField());
      writers.add(buildVectorWriter(child, childDescrip));
      i++;
    }
    return writers;
  }

  private AbstractObjectWriter buildUnion(UnionVector vector, VectorDescrip descrip) {
    final AbstractObjectWriter variants[] = new AbstractObjectWriter[MinorType.values().length];
    MetadataProvider mdProvider = descrip.childProvider();
    int i = 0;
    for (MinorType type : vector.getField().getType().getSubTypeList()) {

      // This call will create the vector if it does not yet exist.
      // Will throw an exception for unsupported types.
      // so call this only if the MajorType reports that the type
      // already exists.

      @SuppressWarnings("resource")
      ValueVector memberVector = ColumnAccessorUtils.getUnionMember(vector, type);
      VectorDescrip memberDescrip = new VectorDescrip(mdProvider, i++, memberVector.getField());
      variants[type.ordinal()] = buildVectorWriter(memberVector, memberDescrip);
    }
    return ColumnWriterFactory.buildUnionWriter(descrip.metadata, vector, variants);
  }

  @SuppressWarnings("resource")
  private AbstractObjectWriter buildList(ListVector vector,
      VectorDescrip descrip) {
    ValueVector dataVector = vector.getDataVector();
    VectorDescrip dataMetadata = new VectorDescrip(descrip.childProvider(), 0, dataVector.getField());
    AbstractObjectWriter dataWriter = buildVectorWriter(dataVector, dataMetadata);
    return ColumnWriterFactory.buildListWriter(descrip.metadata, vector, dataWriter);
  }
}
