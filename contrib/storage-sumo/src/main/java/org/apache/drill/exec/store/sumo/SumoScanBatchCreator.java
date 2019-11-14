package org.apache.drill.exec.store.sumo;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class SumoScanBatchCreator implements BatchCreator<SumoSubScan> {

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context,
      SumoSubScan scanDef, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children == null || children.isEmpty());
    SumoStoragePlugin plugin = (SumoStoragePlugin) context.getStorageRegistry().getPlugin(scanDef.getConfig());
    return plugin.createScan(context, scanDef);
  }
}
