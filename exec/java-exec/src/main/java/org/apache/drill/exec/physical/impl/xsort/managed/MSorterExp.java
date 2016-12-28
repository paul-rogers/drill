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


import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.IntVector;

import io.netty.util.internal.PlatformDependent;

public class MSorterExp
    extends MSortTemplate2
{

    IntVector[] vv0;
    IntVector[] vv4;
    long addr0[];
//    long addr4[];

    @Override
    protected void copyRun(final int start, final int end) {
      assert addrAux == aux.getBuffer().memoryAddress();
      assert addrSv == vector4.getBuffer().memoryAddress();
      long destAddr = addrAux + (start << 2);
      long srcAddr = addrSv + (start << 2);
      PlatformDependent.copyMemory(srcAddr, destAddr, 4 * (end - start));
//      for (int i = start; i < end; i++) {
//        PlatformDependent.copyMemory(srcAddr, destAddr, 4);
////        PlatformDependent.putInt(destAddr, PlatformDependent.getInt(srcAddr));
////        aux.set(i, vector4.get(i));
//        destAddr += 4; srcAddr += 4;
//      }
    }

    @Override
    protected int merge(final int leftStart, final int rightStart, final int rightEnd, final int outStart) {
      int l = leftStart;
      int r = rightStart;
      int o = outStart;
      assert addrAux == aux.getBuffer().memoryAddress();
      assert addrSv == vector4.getBuffer().memoryAddress();
      long auxAddr = addrAux + (outStart << 2);
      long lAddr = addrSv + (leftStart << 2);
      long rAddr = addrSv + (rightStart << 2);
      while (l < rightStart && r < rightEnd) {
        assert auxAddr == (addrAux + (o << 2));
        assert lAddr == (addrSv + (l << 2));
        assert rAddr == (addrSv + (r << 2));
        int lIndex = PlatformDependent.getInt(lAddr);
        int rIndex = PlatformDependent.getInt(rAddr);
//        if (compare(l, r) <= 0) {
        if (compare2(lIndex, rIndex) <= 0) {
          assert PlatformDependent.getInt(lAddr) == vector4.get(l);
          PlatformDependent.putInt(auxAddr, lIndex);
          l++; lAddr += 4;
//          aux.set(o++, vector4.get(l++));
        } else {
          assert PlatformDependent.getInt(rAddr) == vector4.get(r);
          PlatformDependent.putInt(auxAddr, rIndex);
//          aux.set(o++, vector4.get(r++));
          r++; rAddr += 4;
        }
        o++; auxAddr += 4;
      }
      if (l < rightStart) {
        final int n = rightStart - l;
        PlatformDependent.copyMemory(lAddr, auxAddr, 4 * n);
        o += n;
      } else {
        final int n = rightEnd - r;
        PlatformDependent.copyMemory(rAddr, auxAddr, 4 * n);
        o += n;
      }
//      while (l < rightStart) {
//        assert PlatformDependent.getInt(addrSv + (l << 2)) == vector4.get(l);
//        PlatformDependent.putInt(addrAux + (o++ <<2), PlatformDependent.getInt(addrSv + (l++ << 2)));
////        aux.set(o++, vector4.get(l++));
//      }
//      while (r < rightEnd) {
//        assert PlatformDependent.getInt(addrSv + (r << 2)) == vector4.get(r);
//        PlatformDependent.putInt(addrAux + (o++ <<2), PlatformDependent.getInt(addrSv + (r++ << 2)));
////        aux.set(o++, vector4.get(r++));
//      }
      assert o == outStart + (rightEnd - leftStart);
      return o;
    }

    @Override
    public void swap(final int sv0, final int sv1) {
      final long addr0 = addrSv + (sv0 << 2);
      final long addr1 = addrSv + (sv1 << 2);
      final int tmp = PlatformDependent.getInt(addr0);
      PlatformDependent.putInt(addr0, PlatformDependent.getInt(addr1));
      PlatformDependent.putInt(addr1, tmp);
//      final int tmp = vector4.get(sv0);
//      vector4.set(sv0, vector4.get(sv1));
//      vector4.set(sv1, tmp);
    }

   @Override
    public int doEval(int leftIndex, int rightIndex)
        throws SchemaChangeException
    {
      return 0;
    }

//   @Override
//   public int compare(final int leftIndex, final int rightIndex) {
//     final int leftSv = PlatformDependent.getInt(addrSv + (leftIndex << 2));
//     final int rightSv = PlatformDependent.getInt(addrSv + (rightIndex << 2));
    public int compare2(final int leftSv, final int rightSv) {
//      final int leftSv = PlatformDependent.getInt(leftAddr);
//      final int rightSv = PlatformDependent.getInt(rightAddr);
//      assert addrSv == vector4.getBuffer().memoryAddress();
//      assert leftSv == vector4.get(leftIndex);
//      assert rightSv == vector4.get(rightIndex);
//      final int leftSv = vector4.get(leftIndex);
//      final int rightSv = vector4.get(rightIndex);
//      compares++;
      final int left = PlatformDependent.getInt(addr0[(leftSv)>>> 16] + (((leftSv)& 65535)<<2));
//      assert addr0[((leftSv)>>> 16)] == vv0[((leftSv)>>> 16)].getBuffer().memoryAddress();
//      assert left == vv0 [((leftSv)>>> 16)].getAccessor().get(((leftSv)& 65535));
      final int right = PlatformDependent.getInt(addr0[(rightSv)>>> 16] + (((rightSv)& 65535)<<2));
//      assert right == vv4 [((rightSv)>>> 16)].getAccessor().get(((rightSv)& 65535));
      return left < right ? -1 : (left == right) ? 0 : 1;
//      return left - right;
//        {
//             IntHolder out3 = new IntHolder();
//            {
//                out3 .value = vv0 [((leftIndex)>>> 16)].getAccessor().get(((leftIndex)& 65535));
//            }
//            IntHolder out7 = new IntHolder();
//            {
//                out7 .value = vv4 [((rightIndex)>>> 16)].getAccessor().get(((rightIndex)& 65535));
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
    public void doSetup(FragmentContext context, VectorContainer incoming, RecordBatch outgoing)
        throws SchemaChangeException
    {
        {
            int[] fieldIds1 = new int[ 1 ] ;
            fieldIds1 [ 0 ] = 0;
            Object tmp2 = (incoming).getValueAccessorById(IntVector.class, fieldIds1).getValueVectors();
            if (tmp2 == null) {
                throw new SchemaChangeException("Failure while loading vector vv0 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv0 = ((IntVector[]) tmp2);
            int[] fieldIds5 = new int[ 1 ] ;
            fieldIds5 [ 0 ] = 0;
            Object tmp6 = (incoming).getValueAccessorById(IntVector.class, fieldIds5).getValueVectors();
            if (tmp6 == null) {
                throw new SchemaChangeException("Failure while loading vector vv4 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv4 = ((IntVector[]) tmp6);
//            /** start SETUP for function compare_to_nulls_high **/
//            {
//                 {}
//            }
            /** end SETUP for function compare_to_nulls_high **/
            addr0 = new long[vv0.length];
//            addr4 = new long[vv4.length];
            for ( int i = 0; i < vv0.length; i++ ) {
              addr0[i] = vv0[i].getBuffer().memoryAddress();
//              addr4[i] = vv4[i].getBuffer().memoryAddress();
            }
            addrSv = vector4.getBuffer().memoryAddress();
        }
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

}
