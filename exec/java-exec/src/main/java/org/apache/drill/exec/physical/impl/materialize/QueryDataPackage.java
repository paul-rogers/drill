package org.apache.drill.exec.physical.impl.materialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.impl.ScreenCreator.ScreenRoot.Metric;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import io.netty.buffer.DrillBuf;

public interface QueryDataPackage {

  QueryId queryId();
  QueryWritableBatch toWritableBatch();
  VectorContainer batch();
  List<SerializedField> fields();

  /**
   * Package that contains only a query ID. Send for a query that
   * finishes with no data. The results are null: no data, no schema.
   */
  public static class EmptyResultsPackage implements QueryDataPackage {

    private final QueryId queryId;

    public EmptyResultsPackage(QueryId queryId) {
      this.queryId = queryId;
    }

    @Override
    public QueryId queryId() { return queryId; }

    /**
     * Creates a message that sends only the query ID to the
     * client.
     */
    @Override
    public QueryWritableBatch toWritableBatch() {
      QueryData header = QueryData.newBuilder()
        .setQueryId(queryId)
        .setRowCount(0)
        .setDef(RecordBatchDef.getDefaultInstance())
        .build();
      return new QueryWritableBatch(header);
    }

    @Override
    public VectorContainer batch() { return null; }

    @Override
    public List<SerializedField> fields() {
      return Collections.emptyList();
    }
  }

  /**
   * Represents a batch of data with a schema.
   */
  public static class DataPackage implements QueryDataPackage {
    private final RecordMaterializer materializer;
    private final OperatorStats stats;

    public DataPackage(RecordMaterializer materializer, OperatorStats stats) {
      this.materializer = materializer;
      this.stats = stats;
    }

    @Override
    public QueryId queryId() { return materializer.queryId(); }

    @Override
    public QueryWritableBatch toWritableBatch() {
      QueryWritableBatch batch = materializer.convertNext();
      stats.addLongStat(Metric.BYTES_SENT, batch.getByteCount());
      return batch;
    }

    @Override
    public VectorContainer batch() {
      return materializer.incoming();
    }

    @Override
    public List<SerializedField> fields() {
      List<SerializedField> metadata = new ArrayList<>();
      for (VectorWrapper<?> vw : batch()) {
        metadata.add(vw.getValueVector().getMetadata());
      }
      return metadata;
    }
  }
}
