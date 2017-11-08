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
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.ColumnReaderFactory;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.ObjectArrayReader;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

public abstract class BaseReaderBuilder {

  protected List<AbstractObjectReader> buildContainerChildren(
      VectorContainer container, MetadataProvider mdProvider) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      VectorDescrip descrip = new VectorDescrip(mdProvider, i, vector.getField());
      readers.add(buildVectorReader(vector, descrip));
    }
    return readers;
  }

  protected AbstractObjectReader buildVectorReader(ValueVector vector, VectorDescrip descrip) {
    MajorType type = vector.getField().getType();

    // Primitive type

    if (type.getMinorType() != MinorType.MAP) {
      return ColumnReaderFactory.buildColumnReader(vector);
    }

    // Map type

    AbstractObjectReader mapReader = MapReader.build(
        descrip.metadata.mapSchema(),
        buildMap(
            (AbstractMapVector) vector,
            descrip.parent.childProvider(descrip.metadata)));

    // Single map

    if (type.getMode() != DataMode.REPEATED) {
      return mapReader;
    }

    // Repeated map

    return ObjectArrayReader.build((RepeatedMapVector) vector, mapReader);
  }

  protected List<AbstractObjectReader> buildMap(AbstractMapVector mapVector, MetadataProvider provider) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    int i = 0;
    for (ValueVector vector : mapVector) {
      VectorDescrip descrip = new VectorDescrip(provider, i, vector.getField());
      readers.add(buildVectorReader(vector, descrip));
      i++;
    }
    return readers;
  }
}
