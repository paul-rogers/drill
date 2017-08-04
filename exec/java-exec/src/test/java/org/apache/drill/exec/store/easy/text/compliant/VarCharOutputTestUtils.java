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

import java.io.UnsupportedEncodingException;

import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetComparison;

public class VarCharOutputTestUtils {

  public abstract static class TextOutputFixture {

    OperatorFixture fixture;
    OperatorExecContext oContext;
    ResultSetLoader loader;
    TextOutput output;

    public TextOutputFixture(OperatorFixture fixture) {
      this.fixture = fixture;
      oContext = fixture.operatorContext(null);
    }

    public void startBatch() {
      loader.startBatch();
    }

    public void close() {
      loader.close();
      oContext.close();
    }

    private void writeField(int fieldIndex, String value) {
      output.startField(fieldIndex);
      if (value == null) {
        output.endEmptyField();
      }
      byte bytes[];
      try {
        bytes = value.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException(e);
      }
      for (int i = 0; i < bytes.length; i++) {
        output.append(bytes[i]);
      }
      output.endField();
    }

    public void writeRecord(String fields[]) {
      output.startRecord();
      for ( int i = 0; i < fields.length; i++) {
        writeField(i, fields[i] );
      }
      output.finishRecord();
    }

    public int writeRecords(String dataValues[][]) {
      int rowCount = 0;
      for ( int i = 0; i < dataValues.length; i++ ) {
        writeRecord(dataValues[i]);
        rowCount++;
      }
      return rowCount;
    }

    public RowSet finishBatchAsRowSet() {
      return fixture.wrap(loader.harvest());
    }

    public BatchSchema schema() {
      return loader.root().schema().schema();
    }

    public RowSet writeBatch(String dataValues[][]) {
      startBatch();
      writeRecords(dataValues);
      return finishBatchAsRowSet();
    }

    public RowSet makeExpected(String dataValues[][]) {
      return makeExpected(schema(), dataValues);
    }

    public abstract RowSet makeExpected(BatchSchema schema, String dataValues[][]);

    public void compare(String dataValues[][]) {
      compare(writeBatch(dataValues), dataValues);
    }

    public void compare(RowSet actual, String expectedValues[][]) {
      RowSet expected = makeExpected(expectedValues);

      new RowSetComparison(expected)
        .verifyAndClearAll(actual);

      close();
    }
  }
}
