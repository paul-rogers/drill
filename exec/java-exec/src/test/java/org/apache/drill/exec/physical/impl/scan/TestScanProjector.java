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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ImplicitColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.ImplicitColumnDefn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.SelectColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.StaticColumn;
import org.apache.drill.exec.physical.impl.scan.ScanProjector.StaticColumnLoader;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.NullColumn;
import org.apache.drill.exec.physical.impl.scan.ProjectionPlanner.PartitionColumn;
import org.apache.drill.exec.store.ImplicitColumnExplorer.ImplicitFileColumns;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestScanProjector extends SubOperatorTest {

  public SelectColumn buildSelectCol(String name) {
    return new SelectColumn(SchemaPath.getSimplePath(name));
  }

  /**
   * Test the static column loader using one column of each type.
   * The null column is of type int, but the associated value is of
   * type string. This is a bit odd, but works out because we detect that
   * the string value is null and call setNull on the writer, and avoid
   * using the actual data.
   */

  @Test
  public void testStaticColumnLoader() {

    List<StaticColumn> defns = new ArrayList<>();
    defns.add(new NullColumn(buildSelectCol("null"), 0,
        SchemaBuilder.columnSchema("null", MinorType.INT, DataMode.OPTIONAL)));

    ImplicitColumnDefn iDefn = new ImplicitColumnDefn("suffix", ImplicitFileColumns.SUFFIX);
    ImplicitColumn iCol = new ImplicitColumn(buildSelectCol("suffix"), 1, iDefn);
    defns.add(iCol);
    Path path = new Path("hdfs:///w/x/y/z.csv");
    iCol.setValue(path);

    PartitionColumn pCol = new PartitionColumn(buildSelectCol("dir0"), 2, 0);
    defns.add(pCol);
    pCol.setValue("w");

    StaticColumnLoader staticLoader = new StaticColumnLoader(fixture.allocator(), defns);

    // Create a batch

    staticLoader.load(2);

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("null", MinorType.INT)
        .addNullable("suffix", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(null, "csv", "w")
        .add(null, "csv", "w")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(staticLoader.output()));
  }

}
