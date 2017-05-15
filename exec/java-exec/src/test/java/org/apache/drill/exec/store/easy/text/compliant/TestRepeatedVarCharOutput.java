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

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.OperatorExecContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorAccessibleUtilities;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.Test;

public class TestRepeatedVarCharOutput extends SubOperatorTest {

  private static class RVCOFixture {

    OperatorExecContext oContext;
    VectorContainer container;
    OutputMutator output;
    RepeatedVarCharOutput rvco;

    public RVCOFixture(OperatorFixture fixture) throws SchemaChangeException {
      this(fixture, (Collection<SchemaPath>) null);
    }

    public RVCOFixture(OperatorFixture fixture, int[] cols) throws SchemaChangeException {
      this(fixture, buildCols(cols));
    }

    private static Collection<SchemaPath> buildCols(int[] cols) {
      Collection<SchemaPath> columns = new ArrayList<SchemaPath>();
      if (cols == null) {
        return columns;
      }
      for (int i = 0; i < cols.length; i++) {
        columns.add(new SchemaPath(new PathSegment.NameSegment("columns", new PathSegment.ArraySegment(cols[i]))));
      }
      return columns;
    }

    public RVCOFixture(OperatorFixture fixture, Collection<SchemaPath> columns) throws SchemaChangeException {
      oContext = fixture.operatorContext(null);

      // Setup: normally done by ScanBatch

      container = new VectorContainer(fixture.allocator());
      output = new ScanBatch.Mutator(oContext, fixture.allocator(), container);

      rvco = new RepeatedVarCharOutput(output, columns, columns == null);
    }

    public void close() {
      container.clear();
      oContext.close();
    }

    private void writeField(int fieldIndex, String value) {
      rvco.startField(fieldIndex);
      if (value == null) {
        rvco.endEmptyField();
      }
      byte bytes[];
      try {
        bytes = value.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException(e);
      }
      for (int i = 0; i < bytes.length; i++) {
        rvco.append(bytes[i]);
      }
      rvco.endField();
    }

    public void writeRecord(String fields[]) {
      for ( int i = 0; i < fields.length; i++) {
        writeField(i, fields[i] );
      }
      rvco.finishRecord();
    }

    public int writeRecords(String dataValues[][]) {
      int rowCount = 0;
      for ( int i = 0; i < dataValues.length; i++ ) {
        writeRecord(dataValues[i]);
        rowCount++;
      }
      return rowCount;
    }

    private void finishBatch(int rowCount) {
      rvco.finishBatch();
      VectorAccessibleUtilities.setValueCount(container, rowCount);
      container.setRecordCount(rowCount);
      container.buildSchema(SelectionVectorMode.NONE);
    }

    private RowSet finishBatchAsRowSet(int rowCount) {
      finishBatch(rowCount);
      return fixture.wrap(container);
    }

  }

  public RowSet makeExpected(BatchSchema schema, String dataValues[][]) {
    RowSetBuilder builder = fixture.rowSetBuilder(schema);
    for (int i = 0; i < dataValues.length; i++) {
      builder.add(new Object[] {dataValues[i]});
    }
    return builder.build();
  }

  @Test
  public void testBasics() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    // Load several normal rows

    rvco.rvco.startBatch();
    int rowCount = rvco.writeRecords(dataValues);
    RowSet actual = rvco.finishBatchAsRowSet(rowCount);

    BatchSchema schema = rvco.container.getSchema();
    RowSet expected = makeExpected(schema, dataValues);

    new RowSetComparison(expected)
      .verifyAndClearAll(actual);

    rvco.close();
  }

  @Test
  public void testMissingFieldsAtEnd() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" },
        { "2.0 aaa" }
    };

    rvco.rvco.startBatch();
    int rowCount = rvco.writeRecords(dataValues);
    RowSet actual = rvco.finishBatchAsRowSet(rowCount);

    BatchSchema schema = rvco.container.getSchema();
    RowSet expected = makeExpected(schema, dataValues);

    new RowSetComparison(expected)
      .verifyAndClearAll(actual);

    rvco.close();
  }

  @Test
  public void testMissingFieldsInMiddle() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { "1.0 aaa" },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    rvco.rvco.startBatch();
    int rowCount = rvco.writeRecords(dataValues);
    RowSet actual = rvco.finishBatchAsRowSet(rowCount);

    BatchSchema schema = rvco.container.getSchema();
    RowSet expected = makeExpected(schema, dataValues);

    new RowSetComparison(expected)
      .verifyAndClearAll(actual);

    rvco.close();
  }

  @Test
  public void testMissingFirstRow() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    rvco.rvco.startBatch();
    int rowCount = rvco.writeRecords(dataValues);
    RowSet actual = rvco.finishBatchAsRowSet(rowCount);

    BatchSchema schema = rvco.container.getSchema();
    // TODO: See DRILL-5486
    // Empty rows not actually ignored
//    String expectedValues[][] = {
//        dataValues[1], dataValues[2]
//    };
//    RowSet expected = makeExpected(schema, expectedValues);
    RowSet expected = makeExpected(schema, dataValues);

    new RowSetComparison(expected)
      .verifyAndClearAll(actual);

    rvco.close();
  }

  @Test
  public void testMissingFirstRows() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { },
        { },
        { },
        { },
        { },
        { },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" }
    };

    rvco.rvco.startBatch();
    int rowCount = rvco.writeRecords(dataValues);
    RowSet actual = rvco.finishBatchAsRowSet(rowCount);

    BatchSchema schema = rvco.container.getSchema();
    // TODO: See DRILL-5486
    // Empty rows not actually ignored
//    String expectedValues[][] = {
//        dataValues[1], dataValues[2]
//    };
//    RowSet expected = makeExpected(schema, expectedValues);
    RowSet expected = makeExpected(schema, dataValues);

    new RowSetComparison(expected)
      .verifyAndClearAll(actual);

    rvco.close();
  }

  @Test
  public void testMissingRow() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    rvco.rvco.startBatch();
    int rowCount = rvco.writeRecords(dataValues);
    RowSet actual = rvco.finishBatchAsRowSet(rowCount);

    BatchSchema schema = rvco.container.getSchema();
    // TODO: See DRILL-5486
    // Empty rows not actually ignored
//    String expectedValues[][] = {
//        dataValues[0], dataValues[2]
//    };
//    RowSet expected = makeExpected(schema, expectedValues);
    RowSet expected = makeExpected(schema, dataValues);

    new RowSetComparison(expected)
      .verifyAndClearAll(actual);

    rvco.close();
  }

  @Test
  public void testSelect() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture, new int[] {1, 2});

    String dataValues[][] = {
        { "0a", "0b", "0c", "0d", "0f" },
        { "1a", "1b", "1c", "1d", "1f" },
        { "2a", "2b", "2c", "2d", "2f" },
    };

    rvco.rvco.startBatch();
    int rowCount = rvco.writeRecords(dataValues);
    RowSet actual = rvco.finishBatchAsRowSet(rowCount);

    String expectedValues[][] = {
        { "", "0b", "0c", "", "" },
        { "", "1b", "1c", "", "" },
        { "", "2b", "2c", "", "" },
    };

    BatchSchema schema = rvco.container.getSchema();
    RowSet expected = makeExpected(schema, expectedValues);

    new RowSetComparison(expected)
      .verifyAndClearAll(actual);

    rvco.close();
  }

  /**
   * Test a star query for the repeated VarChar output used by the
   * easy readers. Assume a simple input of four columns. Try various
   * error conditions to attempt to replicate DRILL-5470.
   * @throws SchemaChangeException
   */

  @Test
  public void testStar() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    // Batch 1

    rvco.rvco.startBatch();

    // Load several normal rows, four columns each

    String pad = "xxxx|xxxx|xxxx|xxxx|";
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 4; j++) {
        rvco.writeField(j, (i + "-" + j + pad));
      }
    }

    //

    rvco.close();
  }

}
