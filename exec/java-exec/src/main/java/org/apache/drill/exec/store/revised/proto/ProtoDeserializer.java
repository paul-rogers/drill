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
package org.apache.drill.exec.store.revised.proto;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.revised.Sketch.ResultSetMaker;
import org.apache.drill.exec.store.revised.Sketch.RowBatch;
import org.apache.drill.exec.store.revised.Sketch.RowBatchMaker;
import org.apache.drill.exec.store.revised.Sketch.RowMaker;
import org.apache.drill.exec.store.revised.Sketch.RowSchemaBuilder;
import org.apache.drill.exec.store.revised.Sketch.ScanOperation;
import org.apache.drill.exec.store.revised.exec.AbstractDeserializer;
import org.apache.drill.exec.store.revised.proto.ProtoPlugin.ProtoSubScanPop;

public class ProtoDeserializer extends AbstractDeserializer<ProtoSubScanPop> {

  public ProtoDeserializer(ProtoSubScanPop subScanOp, ScanOperation scanOp) {
    super(subScanOp, scanOp);
  }

  @Override
  public RowBatch readBatch() throws Exception {
    ResultSetMaker resultSet = resultSet();
    if (resultSet.batchCount() > 0) {
      return null;
    }

    RowSchemaBuilder schemaBuilder = resultSet.newSchema()
        .column("num", MinorType.INT, DataMode.OPTIONAL);
    rowSet = resultSet.rowSet(schemaBuilder.build());

    RowBatchMaker rowBatch = rowSet.batch();
    for (int i = 0; i < 10; i++) {
      RowMaker row = rowBatch.row();
      row.column(0).setInt(i);
      row.accept();
    }
    return rowBatch.build();
  }
}
