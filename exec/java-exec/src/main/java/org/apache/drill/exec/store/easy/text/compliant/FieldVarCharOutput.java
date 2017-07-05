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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.VarCharVector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a set of varchar vectors. A varchar vector contains all the field
 * values for a given column. Each record is a single value within each vector of the set.
 */
class FieldVarCharOutput extends TextOutput {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldVarCharOutput.class);
//  public static final String COL_NAME = "columns";

  // array of output vector
//  private final VarCharVector [] vectors;
  // boolean array indicating which fields are selected (if star query entire array is set to true)
//  private final boolean[] selectedFields;
  // current vector to which field will be added
//  private VarCharVector currentVector;
  // track which field is getting appended
  private int currentFieldIndex = -1;
  // track chars within field
  private int currentDataPointer = 0;
  // track if field is still getting appended
  private boolean fieldOpen = true;
  // holds chars for a field
  private byte[] fieldBytes;

//  private boolean collect = true;
//  private boolean rowHasData= false;
  private static final int MAX_FIELD_LENGTH = 1024 * 64;
//  private int recordCount = 0;
//  private int batchIndex = 0;
  private int maxField = 0;
  private final TupleLoader writer;

  /**
   * We initialize and add the varchar vector for each incoming field in this
   * constructor.
   * @param outputMutator  Used to create/modify schema
   * @param fieldNames Incoming field names
   * @param columns  List of columns selected in the query
   * @param isStarQuery  boolean to indicate if all fields are selected or not
   * @throws SchemaChangeException
   */
  public FieldVarCharOutput(TupleLoader writer) throws SchemaChangeException {
    this.writer = writer;
    fieldBytes = new byte[MAX_FIELD_LENGTH];
  }

  /**
   * Start a new record record. Resets all pointers
   */
  @Override
  public void startRecord() {
//    this.recordCount = 0;
//    this.batchIndex = 0;
    this.currentFieldIndex = -1;
//    this.collect = true;
    this.fieldOpen = false;
  }

  @Override
  public void startField(int index) {
    currentFieldIndex = index;
    currentDataPointer = 0;
    fieldOpen = true;
//    collect = selectedFields[index];
//    currentVector = vectors[index];
  }

  @Override
  public void append(byte data) {
//    if (!collect) {
//      return;
//    }

    if (currentDataPointer >= MAX_FIELD_LENGTH -1) {
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

    ColumnLoader colLoader = writer.column(currentFieldIndex);
    if (colLoader != null) {
      colLoader.setBytes(fieldBytes, currentDataPointer);
    }
//    if(collect) {
//      assert currentVector != null;
//      currentVector.getMutator().setSafe(recordCount, fieldBytes, 0, currentDataPointer);
//    }
//
//    if (currentDataPointer > 0) {
//      this.rowHasData = true;
//    }

    return currentFieldIndex < maxField;
  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

  @Override
  public void finishRecord() {
    if(fieldOpen){
      endField();
    }

//    recordCount++;
  }

  @Override
  public boolean rowHasData() {
//    return this.rowHasData;
    return this.currentFieldIndex > 0;
  }
}
