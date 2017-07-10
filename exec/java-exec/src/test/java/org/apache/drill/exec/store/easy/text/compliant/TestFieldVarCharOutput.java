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

import java.util.ArrayList;
import java.util.List;

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

public class TestFieldVarCharOutput extends SubOperatorTest {

  private static class FVCOFixture extends TextOutputFixture {

    public FVCOFixture(OperatorFixture fixture, String cols[]) {
      this(fixture, cols, null);
    }

    public FVCOFixture(OperatorFixture fixture, String cols[], String projection[]) {
      super(fixture);

      // Setup: normally done by ScanBatch

      ResultSetLoaderImpl.OptionBuilder builder = new ResultSetLoaderImpl.OptionBuilder();
      if (projection != null) {
        List<String> projCols = new ArrayList<>();
        for (String projCol : projection) {
          projCols.add(projCol);
        }
        builder.setProjection(projCols);
      }
      loader = new ResultSetLoaderImpl(fixture.allocator(), builder.build());
      for (String col : cols) {
         loader.writer().schema().addColumn(
            SchemaBuilder.columnSchema(col, MinorType.VARCHAR, DataMode.REQUIRED));
      }

      try {
        output = new FieldVarCharOutput(loader);
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
        builder.add((Object[]) dataValues[i]);
      }
      return builder.build();
    }
  }

  @Test
  public void testBasics() throws SchemaChangeException {
    FVCOFixture fvco = new FVCOFixture(fixture, new String[] {"a", "b", "c"});

    // Load several normal rows

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    fvco.compare(dataValues);
  }

  @Test
  public void testMissingFieldsAtEnd() throws SchemaChangeException {
    FVCOFixture fvco = new FVCOFixture(fixture, new String[] {"a", "b", "c"});

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" },
        { "2.0 aaa" }
    };
    String expectedValues[][] = {
        dataValues[0],
        dataValues[1],
        { dataValues[2][0], "", "" }
    };

    RowSet actual = fvco.writeBatch(dataValues);
    fvco.compare(actual, expectedValues);
  }

  @Test
  public void testMissingFieldsInMiddle() throws SchemaChangeException {
    FVCOFixture fvco = new FVCOFixture(fixture, new String[] {"a", "b", "c"});

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { "1.0 aaa" },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    fvco.compare(dataValues);
  }

  @Test
  public void testMissingFirstRow() throws SchemaChangeException {
    FVCOFixture fvco = new FVCOFixture(fixture, new String[] {"a", "b", "c"});

    String dataValues[][] = {
        { },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    RowSet actual = fvco.writeBatch(dataValues);

    // See DRILL-5486
    String expectedValues[][] = {
        dataValues[1], dataValues[2]
    };
    fvco.compare(actual, expectedValues);
  }

  @Test
  public void testMissingFirstRows() throws SchemaChangeException {
    FVCOFixture fvco = new FVCOFixture(fixture, new String[] {"a", "b", "c"});

    String dataValues[][] = {
        { },
        { },
        { },
        { },
        { },
        { },
        { "1.0 aaa", "1.1 bbb", "1.2 ccc" }
    };

    RowSet actual = fvco.writeBatch(dataValues);

    // See DRILL-5486
    String expectedValues[][] = {
        dataValues[6]
    };
    fvco.compare(actual, expectedValues);
  }

  @Test
  public void testMissingRow() throws SchemaChangeException {
    FVCOFixture fvco = new FVCOFixture(fixture, new String[] {"a", "b", "c"});

    String dataValues[][] = {
        { "0.0 aaa", "0.1 bbb", "0.2 ccc" },
        { },
        { "2.0 aaa", "2.1 bbb", "2.2 ccc" }
    };

    RowSet actual = fvco.writeBatch(dataValues);

    // See DRILL-5486
    String expectedValues[][] = {
        dataValues[0], dataValues[2]
    };
    fvco.compare(actual, expectedValues);
  }

}
