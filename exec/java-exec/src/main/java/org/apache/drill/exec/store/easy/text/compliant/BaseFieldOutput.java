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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;

public abstract class BaseFieldOutput extends TextOutput {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseFieldOutput.class);

  // track which field is getting appended
  protected int currentFieldIndex = -1;
  // track chars within field
  protected int currentDataPointer = 0;
  // track if field is still getting appended
  private boolean fieldOpen = true;
  // holds chars for a field
  protected byte[] fieldBytes;
  private static final int MAX_FIELD_LENGTH = 1024 * 64;
  protected final RowSetLoader writer;

  public BaseFieldOutput(RowSetLoader writer) {
    this.writer = writer;
    fieldBytes = new byte[MAX_FIELD_LENGTH];
  }

  /**
   * Start a new record record. Resets all pointers
   */

  @Override
  public void startRecord() {
    currentFieldIndex = -1;
    fieldOpen = false;
    writer.start();
  }

  @Override
  public void startField(int index) {
    assert index == currentFieldIndex + 1;
    currentFieldIndex = index;
    currentDataPointer = 0;
    fieldOpen = true;
  }

  @Override
  public void append(byte data) {
    if (currentDataPointer >= MAX_FIELD_LENGTH - 1) {
      throw UserException
          .unsupportedError()
          .message("Trying to write something big in a column")
          .addContext("columnIndex", currentFieldIndex)
          .addContext("Limit", MAX_FIELD_LENGTH)
          .build(logger);
    }

    fieldBytes[currentDataPointer++] = data;
  }

  @Override
  public boolean endField() {
    fieldOpen = false;
    return true;
  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

  @Override
  public void finishRecord() {
    // Don't save the row if no fields written.

    if (currentFieldIndex == -1) {
      return;
    }
    if (fieldOpen) {
      endField();
    }
    writer.save();
  }

  @Override
  public long getRecordCount() {
    return writer.rowCount();
  }

  @Override
  public boolean isFull() {
    return writer.isFull();
  }
}
