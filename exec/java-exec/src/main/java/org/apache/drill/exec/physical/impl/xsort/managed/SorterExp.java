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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.lang.reflect.Field;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.xsort.SingleBatchSorterTemplate;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.vector.IntVector;

import io.netty.util.internal.PlatformDependent;
import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class SorterExp
    extends SingleBatchSorterTemplate
{
    public static Unsafe UNSAFE;
    {
      try {
        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        UNSAFE = (Unsafe) unsafeField.get(null);
      } catch (Throwable cause) {
        throw new IllegalStateException(cause);
      }
    }
    
    IntVector vv0;
    IntVector vv4;
//    IntVector.Accessor va0;
//    IntVector.Accessor va4;
//    DrillBuf vb0;
//    DrillBuf vb4;
    long addr0;
    long addr4;
//    DrillBuf svb;
    long addrSv;

    @Override
    public int doEval(char leftIndex, char rightIndex)
        throws SchemaChangeException
    {
      return 0;
    }

//    @Override
//    public int compare(int leftIndex, int rightIndex) {
//      int sv1 = vector2.getIndex(leftIndex);
//      int sv2 = vector2.getIndex(rightIndex);
    @Override
    public int compare(int leftIndex, int rightIndex) {
      int sv1 = PlatformDependent.getShort(addrSv + (leftIndex << 1)) & 0xFFFF;
      int sv2 = PlatformDependent.getShort(addrSv + (rightIndex << 1)) & 0xFFFF;
//      int sv1 = UNSAFE.getShort(addrSv + (leftIndex << 1)) & 0xFFFF;
//      int sv2 = UNSAFE.getShort(addrSv + (rightIndex << 1)) & 0xFFFF;
//      assert sv1 == usv1;
//      assert sv1 == vector2.getIndex(leftIndex);
//      assert sv2 == vector2.getIndex(rightIndex);
//      int sv1 = svb.getChar(leftIndex * 2);
//      int sv2 = svb.getChar(rightIndex * 2);
//      int sv1 = vector2.getIndex(leftIndex);
//      int sv2 = vector2.getIndex(rightIndex);
      int left = PlatformDependent.getInt(addr0 + (sv1<<2));
      int right = PlatformDependent.getInt(addr4 + (sv2<<2));
//      int left = UNSAFE.getInt(addr0 + (sv1<<2));
//      int right = UNSAFE.getInt(addr4 + (sv2<<2));
//      assert left == vv0.getAccessor().get(sv1);
//      assert right == vv4.getAccessor().get(sv2);
//      int left = vb0.getInt(leftIndex<<2);
//      int right = vb4.getInt(rightIndex<<2);
      return left < right ? -1 : (left == right) ? 0 : 1;
//      return left - right;
//      return Integer.compare(left, right);
//        {
//            IntHolder out3 = new IntHolder();
//            {
//                out3 .value = vv0 .getAccessor().get((leftIndex));
//            }
//            IntHolder out7 = new IntHolder();
//            {
//                out7 .value = vv4 .getAccessor().get((rightIndex));
//            }
//            //---- start of eval portion of compare_to_nulls_high function. ----//
//            IntHolder out8 = new IntHolder();
//            {
//                final IntHolder out = new IntHolder();
//                IntHolder left = out3;
//                IntHolder right = out7;
//
//GCompareIntVsInt$GCompareIntVsIntNullHigh_eval: {
//    outside:
//    {
//        out.value = left.value < right.value ? -1 : (left.value == right.value ? 0 : 1);
//    }
//}
//
//                out8 = out;
//            }
//            //---- end of eval portion of compare_to_nulls_high function. ----//
//            if (out8 .value!= 0) {
//                return out8 .value;
//            }
//        }
//        {
//            return  0;
//        }
    }

    @Override
    public void doSetup(FragmentContext context, VectorAccessible incoming, RecordBatch outgoing)
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
//            va0 = vv0 .getAccessor();
//            vb0 = vv0.getBuffer();
            addr0 = vv0.getBuffer().memoryAddress();
            int[] fieldIds5 = new int[ 1 ] ;
            fieldIds5 [ 0 ] = 0;
            Object tmp6 = (incoming).getValueAccessorById(IntVector.class, fieldIds5).getValueVector();
            if (tmp6 == null) {
                throw new SchemaChangeException("Failure while loading vector vv4 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv4 = ((IntVector) tmp6);
//            va4 = vv4 .getAccessor();
//            vb4 = vv4.getBuffer();
            addr4 = vv4.getBuffer().memoryAddress();
            /** start SETUP for function compare_to_nulls_high **/
            {
                 {}
            }
            /** end SETUP for function compare_to_nulls_high **/
        }
//        svb = vector2.getBuffer(false);
        addrSv = vector2.getBuffer(false).memoryAddress();
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

}
