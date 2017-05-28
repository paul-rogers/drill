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
package org.apache.drill.exec.physical.impl.scan;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ColumnProjectionBuilder;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlan.EarlySchemaProjectPlan;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestResultSetBuilder extends SubOperatorTest {

  /**
   * Test a basic SELECT with a static schema. Select all columns
   * (but in order different than table). No implicit, no partitions.
   * Provides a basic sanity test.
   */

  @Test
  public void testStaticBasicSelect() {

    ColumnProjectionBuilder builder = ProjectionPlanner.builder(fixture.options());

    // SELECT a, b

    SchemaPath aSel = SchemaPath.getSimplePath("a");
    SchemaPath bSel = SchemaPath.getSimplePath("b");
    List<SchemaPath> selected = Lists.newArrayList(aSel, bSel);
    builder.queryCols(selected);

    // Table schema is (b: Int, a: Varchar)

    MaterializedField bCol = SchemaBuilder.columnSchema("b", MinorType.INT, DataMode.OPTIONAL);
    MaterializedField aCol = SchemaBuilder.columnSchema("a", MinorType.VARCHAR, DataMode.REQUIRED);
    List<MaterializedField> dataCols = Lists.newArrayList(bCol, aCol);
    builder.tableColumns(dataCols);

    ProjectionPlanner projection = builder.build();


    // The final output container

    VectorContainer output = new VectorContainer(fixture.allocator());

    EarlySchemaProjectPlan rsBuilder = new EarlySchemaProjectPlan(projection, null, output);

    // Create the result set loader for the table

    ResultSetLoader rsLoader = rsBuilder.tableLoader();

    // The ctor should have create the schema in the loader in table order..

    TupleSchema tableSchema = rsLoader.writer().schema();
    assertEquals(2, tableSchema.columnCount());
    assertEquals(bCol, tableSchema.column(0));
    assertEquals(aCol, tableSchema.column(1));

    // ... and the output in SELECT order

    BatchSchema outSchema = output.getSchema();
    assertEquals(2, outSchema.getFieldCount());
    assertEquals(aCol, tableSchema.column(0));
    assertEquals(bCol, tableSchema.column(1));

    // Create an input batch

    TupleLoader writer = rsLoader.writer();
    rsLoader.startBatch();
    for (int i = 0; i < 3; i++) {
      rsLoader.startRow();
      writer.column(0).setInt(i);
      writer.column(1).setString("Row " + i);
      rsLoader.saveRow();
    }

    // Harvest the row and map to output

    rsBuilder.build();

    // Validate

    SingleRowSet expected = fixture.rowSetBuilder(outSchema)
        .add("Row 0", 0)
        .add("Row 1", 1)
        .add("Row 2", 2)
        .build();

    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(output));
  }

}
