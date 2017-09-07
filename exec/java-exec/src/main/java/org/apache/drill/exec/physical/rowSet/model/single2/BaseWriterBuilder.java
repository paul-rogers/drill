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
package org.apache.drill.exec.physical.rowSet.model.single2;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.single2.MetadataProvider.VectorDescrip;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.accessor.writer.ObjectArrayWriter;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

/**
 * Build a set of writers for a single (non-hyper) vector container.
 */

public abstract class BaseWriterBuilder {

  protected List<AbstractObjectWriter> buildContainerChildren(VectorContainer container, MetadataProvider mdProvider) {
    List<AbstractObjectWriter> writers = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      VectorDescrip descrip = new VectorDescrip(mdProvider, i, vector);
      writers.add(buildVectorWriter(vector, descrip));
    }
    return writers;
  }

  private AbstractObjectWriter buildVectorWriter(ValueVector vector, VectorDescrip descrip) {
    MajorType type = vector.getField().getType();
    if (type.getMinorType() == MinorType.MAP) {
      if (type.getMode() == DataMode.REPEATED) {
        return buildMapArrayWriter((RepeatedMapVector) vector, descrip);
      } else {
        return buildMapWriter((MapVector) vector, descrip);
      }
    } else {
      return buildPrimitiveWriter(vector, descrip);
    }
  }

  private AbstractObjectWriter buildMapArrayWriter(RepeatedMapVector vector, VectorDescrip descrip) {
    AbstractObjectWriter tupleWriter = MapWriter.buildMapArray(descrip.metadata, vector, buildMap(vector, descrip));
    return ObjectArrayWriter.build(descrip.metadata, vector, tupleWriter);
  }

  private AbstractObjectWriter buildMapWriter(MapVector vector, VectorDescrip descrip) {
    return MapWriter.build(descrip.metadata, vector, buildMap(vector, descrip));
  }

  private AbstractObjectWriter buildPrimitiveWriter(ValueVector vector, VectorDescrip descrip) {
    return ColumnAccessorFactory.buildColumnWriter(descrip.metadata, vector);
  }

  private List<AbstractObjectWriter> buildMap(AbstractMapVector vector, VectorDescrip descrip) {
    List<AbstractObjectWriter> writers = new ArrayList<>();
    MetadataProvider provider = descrip.parent.childProvider(descrip.metadata);
    int i = 0;
    for (ValueVector child : vector) {
      VectorDescrip childDescrip = new VectorDescrip(provider, i, child);
      writers.add(buildVectorWriter(child, childDescrip));
      i++;
    }
    return writers;
  }
}