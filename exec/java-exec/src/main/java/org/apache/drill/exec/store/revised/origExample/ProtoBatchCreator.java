package org.apache.drill.exec.store.revised.origExample;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.revised.origExample.ProtoPlugin.ProtoSubScanPop;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ProtoBatchCreator implements BatchCreator<ProtoSubScanPop> {

  @SuppressWarnings("resource")
  @Override
  public CloseableRecordBatch getBatch(FragmentContext context,
      ProtoSubScanPop config, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    final List<RecordReader> readers = Lists.newArrayList();
    readers.add(new ProtoRecordReader(context, config));
    return new ScanBatch(config, context, readers.iterator());
  }
}
