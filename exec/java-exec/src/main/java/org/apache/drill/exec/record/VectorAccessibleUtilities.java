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
package org.apache.drill.exec.record;

import org.apache.drill.exec.vector.AllocationHelper;

public class VectorAccessibleUtilities {

  private VectorAccessibleUtilities() { }

  public static void clear(VectorAccessible va) {
    for (final VectorWrapper<?> w : va) {
      w.clear();
    }
  }

  public static void setValueCount(VectorAccessible va, int count) {
    for (VectorWrapper<?> w: va) {
      w.getValueVector().getMutator().setValueCount(count);
    }
  }

  public static void allocateVectors(VectorAccessible va, int targetRecordCount) {
    for (VectorWrapper<?> w: va) {
      AllocationHelper.allocateNew(w.getValueVector(), targetRecordCount);
    }
  }

}
