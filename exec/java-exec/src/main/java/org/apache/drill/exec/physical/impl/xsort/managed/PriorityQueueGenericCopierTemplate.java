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

import java.util.List;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Implementation of a priority queue internals that uses generated
 * code for comparisons, but generic code for copying columns.
 */

public abstract class PriorityQueueGenericCopierTemplate implements PriorityQueueCopier {

  private ValueVector[] vvOut;
  private ValueVector[][] vvIn;

  @Override
  public void setup(BufferAllocator allocator, VectorAccessible hyperBatch, List<BatchGroup> batchGroups,
                    VectorAccessible outgoing) throws SchemaChangeException {
    // Seems to be no way to get the vector count without iterating...

    int count = 0;
    for(@SuppressWarnings("unused") VectorWrapper<?> vv : hyperBatch) {
      count++;
    }
    vvIn = new ValueVector[count][];
    vvOut = new ValueVector[count];
    int i = 0;
    for(@SuppressWarnings("unused") VectorWrapper<?> vv : hyperBatch) {
      vvIn[i] = hyperBatch.getValueAccessorById(ValueVector.class, i).getValueVectors();
      vvOut[i] = outgoing.getValueAccessorById(ValueVector.class, i).getValueVector();
      i++;
    }
  }

  @Override
  public abstract void doSetup(@Named("incoming") VectorAccessible incoming,
                               @Named("outgoing") VectorAccessible outgoing)
                       throws SchemaChangeException;
  @Override
  public abstract int doEval(@Named("leftIndex") int leftIndex,
                             @Named("rightIndex") int rightIndex)
                      throws SchemaChangeException;

  @Override
  public void doCopy(int inIndex, int outIndex)
                       throws SchemaChangeException {
    int inOffset = inIndex & 0xFFFF;
    int inVector = inIndex >>> 16;
    for ( int i = 0;  i < vvIn.length;  i++ ) {
      vvOut[i].copyEntry(outIndex, vvIn[i][inVector], inOffset);
    }
  }
}
