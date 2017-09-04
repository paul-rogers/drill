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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.AbstractSingleTupleModel;
import org.apache.drill.exec.physical.rowSet.model.single.BaseStructureBuilder;
import org.apache.drill.exec.physical.rowSet.model.single.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.PrimitiveColumnModel;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.accessor.writer.ObjectArrayWriter;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

public class Exp {

  public static class ResultSetStructureBuilder extends BaseStructureBuilder {

    public ResultSetStructureBuilder() {
      super((BufferAllocator) null);

    }

    @Override
    public AbstractSingleColumnModel addColumn(AbstractSingleTupleModel tupleModel, ColumnMetadata columnSchema) {

      // Verify name is not a (possibly case insensitive) duplicate.

      TupleMetadata tupleSchema = tupleModel.schema();
      String colName = columnSchema.name();
      if (tupleSchema.column(colName) != null) {
        throw new IllegalArgumentException("Duplicate column: " + colName);
      }
      return super.addColumn(tupleModel, columnSchema);
    }

    @Override
    protected void preparePrimitiveColumn(AbstractSingleTupleModel tupleModel,
        PrimitiveColumnModel colModel) {
      // Create the writer. Will be returned to the tuple writer.

      AbstractObjectWriter colWriter = ColumnAccessorFactory.buildColumnWriter(colModel.vector());

      // Bind the writer to the model.

      colModel.bindWriter(colWriter);

      prepareColumn(colModel);
    }

    @Override
    protected void prepareMapColumn(AbstractSingleTupleModel tupleModel,
        MapColumnModel mapColModel) {

      // Create the writer. Will be returned to the tuple writer.

      AbstractObjectWriter mapWriter = MapWriter.build(mapColModel.schema(), mapColModel.vector());
      if (mapColModel.schema().isArray()) {
        mapWriter = ObjectArrayWriter.build((RepeatedMapVector) mapColModel.vector(), mapWriter);
      }

      // Bind the writer to the model.

      mapColModel.bindWriter(mapWriter);
      mapColModel.mapModelImpl().bindWriter(mapWriter);

      prepareColumn(mapColModel);
    }

    private void prepareColumn(AbstractSingleColumnModel colModel) {
      // TODO Auto-generated method stub

    }

  }

}
