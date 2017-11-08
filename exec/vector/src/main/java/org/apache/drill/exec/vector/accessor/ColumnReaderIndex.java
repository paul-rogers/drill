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
package org.apache.drill.exec.vector.accessor;

/**
 * Column reader index: provides the read position within a batch or
 * an array.
 */

public interface ColumnReaderIndex {

  /**
   * Ordinal index within the batch; increments from 0. Identifies
   * the row number of top-level records.
   *
   * @return external read index
   */

  int batchIndex();

  /**
   * Vector index, possibly mapped through an indirection vector. When used
   * with an array, indicates the index into the offset vector of the array
   * using an absolute index; not a relative position.
   *
   * @return vector read index
   */

  int vectorIndex();

  /**
   * Advances the index to the next position. Used at the top level
   * (only) for normal readers; at a nested level for implicit join
   * readers.
   *
   * @return true if another value is available, false if EOF
   */

  boolean next();

  /**
   * Return the number of items that this index indexes: top-level record
   * count for the root index; total element count for nested arrays.
   *
   * @return element count at this index level
   */

  int size();
}
