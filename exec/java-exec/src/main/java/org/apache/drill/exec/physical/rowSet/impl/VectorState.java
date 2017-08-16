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
package org.apache.drill.exec.physical.rowSet.impl;

/**
 * Handles batch and overflow operation for a (possibly compound) vector.
 */

public interface VectorState {

  /**
   * Allocate a new vector with the number of elements given. If the vector
   * is an array, then the cardinality given is the number of arrays.
   * @param cardinality number of elements desired in the allocated
   * vector
   */

  void allocate(int cardinality);

  /**
   * A vector has overflowed. Create a new look-ahead vector of the given
   * cardinality, then copy the overflow values from the main vector to the
   * look-ahead vector.
   *
   * @param sourceStartIndex the position in the vector from which to move
   * @param cardinality the number of elements in the new vector. If this
   * vector is an array, then this is the number of arrays
   * @return the new next write position for the vector index associated
   * with the writer for this vector
   */

  int rollOver(int sourceStartIndex, int cardinality);

  /**
   * A batch is being harvested after an overflow. Put the full batch
   * back into the main vector so it can be harvested.
   */

  void harvestWithLookAhead();

  /**
   * A new batch is starting while an look-ahead vector exists. Move
   * the look-ahead buffers into the main vector to prepare for writing
   * the rest of the batch.
   */

  void startBatchWithLookAhead();

  /**
   * Clear the vector(s) associated with this state.
   */

  void reset();
}