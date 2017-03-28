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

public interface ArrayWriter {
  ColumnWriter entry();

  /**
   * Save the current item and advance to another. Allows writers
   * to write to an item, and then "abandon" it by not saving.
   * The writer starts positioned at the first slot.
   * @return if the backing vectors have space for another
   * item, false if the backing vector is full.
   */

  void save();
  int size();

  /**
   * Determine if the next position is valid for writing. Will be invalid
   * if the writer hits a size or other limit.
   *
   * @return true if another item is available and the reader is positioned
   * at that item, false if no more items are available and the reader
   * is no longer valid
   */

  boolean valid();
}
