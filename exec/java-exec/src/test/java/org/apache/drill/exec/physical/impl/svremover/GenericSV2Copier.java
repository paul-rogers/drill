package org.apache.drill.exec.physical.impl.svremover;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.svremover.CopierTemplate2;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

public class GenericSV2Copier extends CopierTemplate2 {

  private ValueVector[] vvOut;
  private ValueVector[] vvIn;

  @SuppressWarnings("unused")
  @Override
  public void doSetup(FragmentContext context, RecordBatch incoming,
      RecordBatch outgoing) throws SchemaChangeException {

    int count = 0;
    for(VectorWrapper<?> vv : incoming) {
      count++;
    }
    vvIn = new ValueVector[count];
    vvOut = new ValueVector[count];
    int i = 0;
    for(VectorWrapper<?> vv : incoming) {
      vvIn[i] = incoming.getValueAccessorById(ValueVector.class, i).getValueVector();
      vvOut[i] = outgoing.getValueAccessorById(ValueVector.class, i).getValueVector();
      i++;
    }
  }

  @Override
  public void doEval(int inIndex, int outIndex) throws SchemaChangeException {
    for ( int i = 0;  i < vvIn.length;  i++ ) {
      vvOut[i].copyEntry(outIndex, vvIn[i], inIndex);
    }
  }
}
