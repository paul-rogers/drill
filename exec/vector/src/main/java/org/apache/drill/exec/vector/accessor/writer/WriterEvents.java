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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;

/**
 * Internal interface used to control the behavior
 * of writers. Consumers of writers never use this method; it is
 * instead used by the code that implements writers.
 */

public interface WriterEvents {

  /**
   * Bind the writer to a writer index.
   *
   * @param index the writer index (top level or nested for
   * arrays)
   */

  void bindIndex(ColumnWriterIndex index);

  /**
   * Start a write (batch) operation. Performs any vector initialization
   * required at the start of a batch (especially for offset vectors.)
   */

  void startWrite();

  /**
   * Start a new value: row (for top-level tuple) or array value (for
   * an element within a repeated map or list.)
   */

  void startValue();

  /**
   * End a value: a row (top-level) or element (for nested element within
   * a list.)
   */

  void endValue();

  /**
   * End a batch: finalize any vector values.
   */

  void endWrite();

  /**
   * During a write, rewind the the current index position.
   * Done when abandoning the current row, such as when filtering out
   * a row at read time.
   */

  void rewind();

  /**
   * Start the writer with a new buffer and offset: used during vector
   * overflow processing.
   *
   * @param index initial value for the last write position, which may
   * reflect overflow values copied into the vector from a prior batch
   */

  void startWriteAt(int index);

  /**
   * Return the last write position in the vector. This may be the
   * same as the writer index position (if the vector was written at
   * that point), or an earlier point. In either case, this value
   * points to the last valid value in the vector.
   *
   * @return index of the last valid value in the vector
   */

  int lastWriteIndex();
}
