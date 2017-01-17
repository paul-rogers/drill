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
package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.filter.FilterTemplate2;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.IntVector;

import io.netty.util.internal.PlatformDependent;

public class FilterExp extends FilterTemplate2
{

    IntVector vv0;
    IntHolder constant5;
    private long addr0;

    public FilterExp() {
        try {
            __DRILL_INIT__();
        } catch (SchemaChangeException e) {
            throw new UnsupportedOperationException(e);
        }
    }

//    @Override
//    protected void filterBatchNoSV(int recordCount) throws SchemaChangeException {
//      long addr0 = vv0.getBuffer().memoryAddress();
//      long baseSV = outgoingSelectionVector.getDataAddr();
//      long addrSV = baseSV;
//      for(short i = 0; i < recordCount; i++) {
//        int value = PlatformDependent.getInt(addr0);
//        addr0 += 4;
//        if(value > 0) {
//          PlatformDependent.putShort(addrSV, i);
//          addrSV += 2;
//        }
//      }
//      int count = (int) (addrSV - baseSV)/2;
//      outgoingSelectionVector.setRecordCount(count);
//    }

    @Override
    protected void filterBatchNoSV(int recordCount) throws SchemaChangeException {
      long addr0 = vv0.getBuffer().memoryAddress();
      long addrSV = outgoingSelectionVector.getDataAddr();
      int svIndex = 0;
      for(int i = 0; i < recordCount; i++) {
        int value = PlatformDependent.getInt(addr0 + (i<<2));
        if(value > 0) {
          PlatformDependent.putShort(addrSV + (svIndex<<1), (short) i);
          svIndex++;
        }
      }
      outgoingSelectionVector.setRecordCount(svIndex);
    }

//    @Override
//    protected void filterBatchNoSV(int recordCount) throws SchemaChangeException {
//      int svIndex = 0;
//      for(int i = 0; i < recordCount; i++){
//        int value = PlatformDependent.getInt(addr0 + (i<<2));
//        if(value > 0){
//          outgoingSelectionVector.setIndex(svIndex, (char)i);
//          svIndex++;
//        }
//      }
//      outgoingSelectionVector.setRecordCount(svIndex);
//    }

//    @Override
//    protected void filterBatchNoSV(int recordCount) throws SchemaChangeException {
////      long addrSV = outgoingSelectionVector.getBuffer().memoryAddress();
//      long addrSV = outgoingSelectionVector.getDataAddr();
//      int svIndex = 0;
//      for(int i = 0; i < recordCount; i++){
//        if(doEval(i, 0)){
//          PlatformDependent.putShort(addrSV + (svIndex<<1), (short) i);
////          outgoingSelectionVector.setIndex(svIndex, (char)i);
//          svIndex++;
//        }
//      }
//      outgoingSelectionVector.setRecordCount(svIndex);
//    }

    @Override
    public boolean doEval(int inIndex, int outIndex)
        throws SchemaChangeException
    {
//      assert vv0.getBuffer().memoryAddress() == addr0;
//      int x = vv0 .getAccessor().get((inIndex));
      int value = PlatformDependent.getInt(addr0 + (inIndex<<2));
//      assert value == x;
      return value > 0;
//      return vv0 .getAccessor().get((inIndex)) > 0;
//        {
//            IntHolder out3 = new IntHolder();
//            {
//                out3 .value = vv0 .getAccessor().get((inIndex));
//            }
//            //---- start of eval portion of greater_than function. ----//
//            BitHolder out6 = new BitHolder();
//            {
//                final BitHolder out = new BitHolder();
//                IntHolder left = out3;
//                IntHolder right = constant5;
//
//GCompareIntVsInt$GreaterThanIntVsInt_eval: {
//    out.value = left.value > right.value ? 1 : 0;
//}
//
//                out6 = out;
//            }
//            //---- end of eval portion of greater_than function. ----//
//            return (out6 .value == 1);
//        }
    }

    @Override
    public void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing)
        throws SchemaChangeException
    {
        {
            int[] fieldIds1 = new int[ 1 ] ;
            fieldIds1 [ 0 ] = 0;
            Object tmp2 = (incoming).getValueAccessorById(IntVector.class, fieldIds1).getValueVector();
            if (tmp2 == null) {
                throw new SchemaChangeException("Failure while loading vector vv0 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv0 = ((IntVector) tmp2);
            IntHolder out4 = new IntHolder();
            out4 .value = 0;
            constant5 = out4;
            /** start SETUP for function greater_than **/
            {
                IntHolder right = constant5;
                 {}
            }
            /** end SETUP for function greater_than **/
            addr0 = vv0.getBuffer().memoryAddress();
        }
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

}
