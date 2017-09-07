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

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.single2.MetadataProvider.MetadataCreator;
import org.apache.drill.exec.physical.rowSet.model.single2.MetadataProvider.VectorDescrip;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

/**
 * Produce a metadata schema from a vector container. Used when given a
 * record batch without metadata.
 */

public class SchemaInference {

  public TupleMetadata infer(VectorContainer container) {
    MetadataCreator mdProvider = new MetadataCreator();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      @SuppressWarnings("resource")
      ValueVector vector = container.getValueVector(i).getValueVector();
      VectorDescrip descrip = new VectorDescrip(mdProvider, i, vector);
      inferVector(vector, descrip);
    }
    return mdProvider.tuple();
  }

  private void inferVector(ValueVector vector, VectorDescrip descrip) {
    MajorType type = vector.getField().getType();
    if (type.getMinorType() == MinorType.MAP) {
      inferMapSchema((AbstractMapVector) vector, descrip);
    }
  }

  private void inferMapSchema(AbstractMapVector vector, VectorDescrip descrip) {
    MetadataProvider provider = descrip.parent.childProvider(descrip.metadata);
    int i = 0;
    for (ValueVector child : vector) {
      VectorDescrip childDescrip = new VectorDescrip(provider, i, child);
      inferVector(child, childDescrip);
      i++;
    }
  }
}
