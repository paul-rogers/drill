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
package org.apache.drill.exec.vector.complex;

import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Tracks and populate empty values in repeated value vectors.
 */
public class EmptyValuePopulator {
  private final UInt4Vector offsets;

  public EmptyValuePopulator(UInt4Vector offsets) {
    this.offsets = Preconditions.checkNotNull(offsets, "offsets cannot be null");
  }

  /**
   * Marks all values since the last set as empty. The last set value is
   * obtained from underlying offsets vector.
   *
   * @param lastIndex
   *          the last index (inclusive) in the offsets vector until which empty
   *          population takes place
   * @throws java.lang.IndexOutOfBoundsException
   *           if lastIndex is negative or greater than offsets capacity.
   */
  public void populate(int lastIndex) {
    Preconditions.checkElementIndex(lastIndex, Integer.MAX_VALUE);
    UInt4Vector.Accessor accessor = offsets.getAccessor();
    int valueCount = accessor.getValueCount();
    if (valueCount == 0) {
      return;
    }
    int previousEnd = accessor.get(valueCount);
    UInt4Vector.Mutator mutator = offsets.getMutator();
    for (int i = valueCount; i <= lastIndex; i++) {
      mutator.setSafe(i + 1, previousEnd);
    }
    mutator.setValueCount(lastIndex+1);
  }
}
