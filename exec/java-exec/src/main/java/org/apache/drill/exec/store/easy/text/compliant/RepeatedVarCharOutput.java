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
package org.apache.drill.exec.store.easy.text.compliant;

import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a single vector of type repeated varchar vector. Each record is a single
 * value within the vector containing all the fields in the record as individual array elements.
 */
class RepeatedVarCharOutput extends BaseFieldOutput {

  private final boolean[] projectionMask;
  private final int maxField;
  private final ScalarWriter columnWriter;
  private final ArrayWriter arrayWriter;

  /**
   * We initialize and add the repeated varchar vector to the record batch in this
   * constructor. Perform some sanity checks if the selected columns are valid or not.
   * @param projectionMask
   * @param tupleLoader
   */
  public RepeatedVarCharOutput(RowSetLoader loader, boolean[] projectionMask) {
    super(loader);
    this.projectionMask = projectionMask;
    arrayWriter = writer.array(0);
    columnWriter = arrayWriter.scalar();
    if (projectionMask == null) {
      maxField = TextReader.MAXIMUM_NUMBER_COLUMNS;
    } else {
      int end = projectionMask.length - 1;
      while (end >= 0 && ! projectionMask[end]) {
        end--;
      }
      if (end == -1) {
        throw new IllegalStateException("No columns[] indexes selected");
      }
      maxField = end;
    }
  }

  /**
   * Write the value into an array position. Rules:
   * <ul>
   * <li>If there is no projection mask, collect all columns.</li>
   * <li>If a selection mask is present, we previously found the index
   * of the last projection column (<tt>maxField</tt>). If the current
   * column is beyond that number, ignore the data and stop accepting
   * columns.</li>
   * <li>If the column is projected, add the data to the array.</li>
   * <li>If the column is not projected, add a blank value to the
   * array.</li>
   * </ul>
   * The above ensures that we leave no holes in the portion of the
   * array that is projected (by adding blank columns where needed),
   * and we just ignore columns past the end of the projected part
   * of the array. (No need to fill holes at the end.)
   */

  @Override
  public boolean endField() {
    super.endField();

    if (projectionMask == null) {
      columnWriter.setBytes(fieldBytes, currentDataPointer);
      return true;
    }
    if (currentFieldIndex > maxField) {
      if (currentFieldIndex > TextReader.MAXIMUM_NUMBER_COLUMNS) {
        throw new IndexOutOfBoundsException("Field index: " + currentFieldIndex);
      }
      return false;
    }
    if (projectionMask[currentFieldIndex]) {
      columnWriter.setBytes(fieldBytes, currentDataPointer);
    } else {
      columnWriter.setBytes(fieldBytes, 0);
    }
    return currentFieldIndex < maxField;
  }
}
