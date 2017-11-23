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
package org.apache.drill.exec.vector.accessor.reader;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;

public interface VectorAccessor {
  boolean isHyper();
  MajorType type();
  void bind(ColumnReaderIndex index);
  <T extends ValueVector> T vector();

  public class SingleVectorAccessor implements VectorAccessor {

    private final ValueVector vector;

    public SingleVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    @Override
    public boolean isHyper() { return false; }

    @Override
    public void bind(ColumnReaderIndex index) { }

    @Override
    public MajorType type() { return vector.getField().getType(); }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() { return (T) vector; }
  }

  /**
   * Vector accessor used by the column accessors to obtain the vector for
   * each column value. That is, position 0 might be batch 4, index 3,
   * while position 1 might be batch 1, index 7, and so on.
   */

  public abstract class BaseHyperVectorAccessor implements VectorAccessor {

    private final MajorType type;

    public BaseHyperVectorAccessor(MajorType type) {
      this.type = type;
    }

    @Override
    public boolean isHyper() { return true; }

    @Override
    public void bind(ColumnReaderIndex index) { }

    @Override
    public MajorType type() { return type; }
  }
}
