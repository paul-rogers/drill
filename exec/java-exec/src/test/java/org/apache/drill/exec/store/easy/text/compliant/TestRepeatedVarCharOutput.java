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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.easy.text.compliant.VarCharOutputTestUtils.TextOutputFixture;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestRepeatedVarCharOutput extends SubOperatorTest {

  private static class RVCOFixture extends TextOutputFixture {

    public RVCOFixture(OperatorFixture fixture) {
      this(fixture, (boolean[]) null);
    }

    public RVCOFixture(OperatorFixture fixture, int[] cols) {
      this(fixture, buildCols(cols));
    }

    private static boolean[] buildCols(int[] cols) {
      if (cols == null) {
        return new boolean[0];
      }
      int maxField = 0;
      for (int i = 0; i < cols.length; i++) {
        maxField = Math.max(maxField, cols[i]);
      }
      boolean mask[] = new boolean[maxField + 1];
      for (int i = 0; i < cols.length; i++) {
        mask[cols[i]] = true;
      }
      return mask;
    }

    public RVCOFixture(OperatorFixture fixture, boolean projectionMask[]) {
      super(fixture);

      // Setup: normally done by ScanBatch

      loader = new ResultSetLoaderImpl(fixture.allocator());
      loader.root().schema().add(
          SchemaBuilder.columnSchema("columns", MinorType.VARCHAR, DataMode.REPEATED));

      try {
        output = new RepeatedVarCharOutput(loader, projectionMask);
      } catch (Throwable t) {
        loader.close();
        oContext.close();
        throw t;
      }
    }

    @Override
    public RowSet makeExpected(BatchSchema schema, String dataValues[][]) {
      RowSetBuilder builder = fixture.rowSetBuilder(schema);
      for (int i = 0; i < dataValues.length; i++) {
        builder.addSingleCol(dataValues[i]);
      }
      return builder.build();
    }
  }

  @Test
  public void testBasics() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    // Load several normal rows

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    rvco.compare(dataValues);
  }

  @Test
  public void testMissingFieldsAtEnd() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" },
        { "2.0 aaa" }
    };

    rvco.compare(dataValues);
  }

  @Test
  public void testMissingFieldsInMiddle() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { "1.0 aaa" },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    rvco.compare(dataValues);
  }

  @Test
  public void testMissingFirstRow() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    RowSet actual = rvco.writeBatch(dataValues);

    // See DRILL-5486
    String expectedValues[][] = {
        dataValues[1], dataValues[2]
    };
    rvco.compare(actual, expectedValues);
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

    RowSet actual = rvco.writeBatch(dataValues);

    // See DRILL-5486
    String expectedValues[][] = {
        dataValues[6]
    };
    rvco.compare(actual, expectedValues);
  }

  @Test
  public void testMissingRow() throws SchemaChangeException {
    RVCOFixture rvco = new RVCOFixture(fixture);

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    RowSet actual = rvco.writeBatch(dataValues);

    // See DRILL-5486
    String expectedValues[][] = {
        dataValues[0], dataValues[2]
    };
    rvco.compare(actual, expectedValues);
  }

  @Test
  public void testProject() {
    RVCOFixture rvco = new RVCOFixture(fixture, new int[] {1, 2});

    String dataValues[][] = {
        { "0a", "0b", "0c", "0d", "0f" },
        { "1a", "1b", "1c", "1d", "1f" },
        { "2a", "2b", "2c", "2d", "2f" },
    };

    RowSet actual = rvco.writeBatch(dataValues);

    // The revised CSV writer omits projected columns
    // past the last projected one. The previous version would
    // fill the unneeded slots with blanks.

    String expectedValues[][] = {
        { "", "0b", "0c" },
        { "", "1b", "1c" },
        { "", "2b", "2c" },
    };

    rvco.compare(actual, expectedValues);
  }

  @Test
  public void testEmptyProject() {
    try {
      new RVCOFixture(fixture, new int[] { });
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("No columns[] indexes selected"));
    }
  }

//  /**
//   * Test a star query for the repeated VarChar output used by the
//   * easy readers. Assume a simple input of four columns. Try various
//   * error conditions to attempt to replicate DRILL-5470.
//   * @throws SchemaChangeException
//   */
//
//  @Test
//  public void testStar() throws SchemaChangeException {
//    RVCOFixture rvco = new RVCOFixture(fixture);
//
//    // Batch 1
//
//    rvco.startBatch();
//
//    // Load several normal rows, four columns each
//
//    rvco.rvco.startRecord();
//    String pad = "xxxx|xxxx|xxxx|xxxx|";
//    for (int i = 0; i < 10; i++) {
//      for (int j = 0; j < 4; j++) {
//        rvco.writeField(j, (i + "-" + j + pad));
//      }
//    }
//
//    //
//
//    rvco.close();
//  }

}
