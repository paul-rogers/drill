package org.apache.drill.exec.physical.impl.svremover;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.protocol.BatchAccessor;
import org.apache.drill.exec.physical.impl.protocol.IncomingBatchAccessor;
import org.apache.drill.exec.physical.impl.protocol.OperatorExec;
import org.apache.drill.exec.physical.impl.protocol.SingleOutgoingContainerAccessor;
import org.apache.drill.exec.physical.resultSet.ResultSetCopier;
import org.apache.drill.exec.physical.resultSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetCopierImpl;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;

public class RemovingOperator implements OperatorExec {

  private static final int TARGET_BATCH_SIZE_BYTES = 4 * 1024 * 1024;
  private static final int TARGET_BATCH_SIZE_ROWS = 4096;

  private final RecordBatch upstream;
  private final IncomingBatchAccessor input;
  private final SingleOutgoingContainerAccessor output;
  private final ResultSetCopier copier;
  private boolean upstreamDone;

  public RemovingOperator(BufferAllocator allocator, RecordBatch upstream) {
    this.upstream = upstream;
    input = new IncomingBatchAccessor(upstream);
    OptionBuilder outputOptions = new OptionBuilder()
        .setBatchSizeLimit(TARGET_BATCH_SIZE_BYTES)
        .setRowCountLimit(TARGET_BATCH_SIZE_ROWS);
    copier = new ResultSetCopierImpl(allocator, input, outputOptions);
    output = new SingleOutgoingContainerAccessor(copier.outputContainer());
  }

  @Override
  public void bind(OperatorContext context) { }

  @Override
  public BatchAccessor batchAccessor() { return output; }

  @Override
  public boolean buildSchema() { return true; }

  @Override
  public boolean next() {
    copier.startOutputBatch();
    while (! copier.isOutputFull()) {
      copier.releaseInputBatch();
      if (! nextUpstream()) {
        break;
      }
      copier.startInputBatch();
      copier.copyAllRows();
    }
    if (! copier.hasOutputRows()) {
      return false;
    }

    copier.harvestOutput();
    output.registerBatch();
    return true;
  }

  private boolean nextUpstream() {
    if (upstreamDone) {
      return false;
    }
    for (;;) {
      IterOutcome result = upstream.next();
      switch (result) {
      case EMIT:
      case OK:
      case OK_NEW_SCHEMA:
        input.acceptBatch();
        return true;
      case NOT_YET:
        break;
      case NONE:
      case OUT_OF_MEMORY:
      case STOP:
        upstreamDone = true;
        return false;
      default:
        throw new IllegalStateException("Unexpected iter status: " + result.name());
      }
    }
  }

  @Override
  public void cancel() { }

  @Override
  public void close() {
    copier.close();
    output.release();
  }
}
