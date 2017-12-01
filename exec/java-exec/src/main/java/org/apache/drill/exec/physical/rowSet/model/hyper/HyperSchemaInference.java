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
package org.apache.drill.exec.physical.rowSet.model.hyper;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

public class HyperSchemaInference {

  public TupleMetadata infer(VectorContainer container) throws SchemaChangeException {
    TupleSchema schema = new TupleSchema();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      VectorWrapper<?> vw = container.getValueVector(i);
      schema.addColumn(buildColumn(vw));
    }
    return schema;
  }

  private ColumnMetadata buildColumn(VectorWrapper<?> vw) throws SchemaChangeException {
    ColumnMetadata commonSchema = null;
    for (ValueVector vector : vw.getValueVectors()) {
      ColumnMetadata mapSchema = TupleSchema.fromField(vector.getField());
      if (commonSchema == null) {
        commonSchema = mapSchema;
      } else if (! commonSchema.isEquivalent(mapSchema)) {
        throw new SchemaChangeException("Maps are not consistent");
      }
    }
    return commonSchema;
  }
}
