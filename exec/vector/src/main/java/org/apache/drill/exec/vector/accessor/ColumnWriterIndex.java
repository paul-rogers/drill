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
 * A Drill record batch consists of a variety of vectors, including maps and lists.
 * Each vector is written independently. A reader may skip some values in each row
 * if no values appear for those columns.
 * <p>
 * This index provides a single view of the "current row" or "current array index"
 * across a set of vectors. Each writer consults this index to determine:
 * <ul>
 * <li>The position to which to write a value.</li>
 * <li>Whether the write position is beyond the "last write" position, which
 * would require filling in any "missing" values.</li>
 * </ul>
 */

public interface ColumnWriterIndex {

  /**
   * Current row or array index.
   * @return row or array index
   */

  int vectorIndex();

  /**
   * Index for array elements that allows the caller to increment the
   * index. For arrays, writing (or saving) one value automatically
   * moves to the next value. Ignored for non-element indexes.
   */

  void nextElement();

  /**
   * When handling overflow, the index must be reset a specific point based
   * on the data that is rolled over to the look-ahead vectors. While a
   * vector index is, in general, meant to provide a top-down, synchronized
   * idea of the current write position, this reset method is a bottom-up
   * adjustment based on actual data. As such, it should only be used
   * during overflow processing as care must be taken to ensure all levels
   * of indexes agree on a consistent set of positions. For internal and
   * leaf indexes (those that correspond to arrays), the index must
   * correspond with the associated offset vector.
   *
   * @param newIndex the new write index position after roll-over
   */

  void resetTo(int newIndex);
}
