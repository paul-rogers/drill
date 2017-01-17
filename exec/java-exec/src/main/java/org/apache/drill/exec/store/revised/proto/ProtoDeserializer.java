package org.apache.drill.exec.store.revised.proto;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.revised.Sketch.ResultSetMaker;
import org.apache.drill.exec.store.revised.Sketch.RowBatch;
import org.apache.drill.exec.store.revised.Sketch.RowBatchMaker;
import org.apache.drill.exec.store.revised.Sketch.RowMaker;
import org.apache.drill.exec.store.revised.Sketch.RowSchemaBuilder;
import org.apache.drill.exec.store.revised.proto.ProtoPlugin.ProtoSubScanPop;

public class ProtoDeserializer extends AbstractDeserializer<ProtoSubScanPop> {

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
