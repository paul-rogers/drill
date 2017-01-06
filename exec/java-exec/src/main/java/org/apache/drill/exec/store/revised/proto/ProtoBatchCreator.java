package org.apache.drill.exec.store.revised.proto;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.mock.MockSubScanPOP;
import org.apache.drill.exec.store.revised.proto.ProtoPlugin.ProtoSubScanPop;

public class ProtoBatchCreator implements BatchCreator<ProtoSubScanPop> {

  @Override
  public CloseableRecordBatch getBatch(FragmentContext context,
      ProtoSubScanPop config, List<RecordBatch> children)
      throws ExecutionSetupException {
    // TODO Auto-generated method stub
    return new ProtoScanBatch(context, config);
  }

}
