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
package org.apache.drill.exec.store.easy.text.compliant.ex;

import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.TableColumn;
import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;

public class FieldVarCharOutput implements TextOutputEx {

  public static final int NO_FIELD = -1;

  private static class FieldDefn {
    ColumnLoader loader;
    boolean isLastField;

    public FieldDefn(ColumnLoader loader) {
      this.loader = loader;
    }

    public void write(byte[] buffer, int len) {
      if (loader == null) {
        return;
      }
      loader.setBytes(buffer, len);
    }
  }

  private final ResultSetLoader rsLoader;
  private final FieldDefn fields[];
  private int fieldIndex = NO_FIELD;
  private boolean inRow;
  private byte[] buffer;
  private int toPosn;

  public FieldVarCharOutput(ProjectionPlanner projection, ResultSetLoader rsLoader) {
    this.rsLoader = rsLoader;
    TupleLoader writer = rsLoader.writer();

    // Define projected fields. Fields are defined in input order, with the
    // writer defined in output order.

    fields = new FieldDefn[projection.tableCols().size()];
    for (TableColumn col : projection.tableCols()) {
      fields[col.index()] = new FieldDefn(writer.column(col.projection().index()));
    }

    // Fill in dummy fields for non-projected columns.

    for (int i = 0; i < fields.length; i++) {
      if (fields[i] == null) {
        fields[i] = new FieldDefn(null);
      }
    }

    // Fill in the "last field" flag for each field.

    for (int i = fields.length - 1; i >= 0; i--) {
      if (fields[i].loader == null) {
        fields[i].isLastField = true;
      } else {
        break;
      }
    }
  }

  @Override
  public void startField(int index) {
    assert fieldIndex == NO_FIELD;
    if (! inRow) {
      rsLoader.startRow();
      inRow = true;
    }
    fieldIndex = index;
    toPosn = 0;
  }

  @Override
  public boolean endField() {
    if (fieldIndex == NO_FIELD) {
      return true;
    }
    FieldDefn field = fields[fieldIndex];
    field.write(buffer, toPosn);
    return ! field.isLastField;
  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

  @Override
  public void append(byte data) {
    buffer[toPosn++] = data;
  }

  @Override
  public void finishRecord() {
    endField();
    rsLoader.saveRow();
    inRow = false;
  }

  @Override
  public boolean rowHasData() { return inRow; }
}
