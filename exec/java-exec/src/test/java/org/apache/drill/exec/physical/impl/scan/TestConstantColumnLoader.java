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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadata;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumnDefn;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedFileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedPartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ConstantColumn;
import org.apache.drill.exec.physical.impl.scan.project.ConstantColumnLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.ColumnExplorer.ImplicitFileColumns;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestConstantColumnLoader extends SubOperatorTest {

  private static class DummyColumn extends ConstantColumn {

    public DummyColumn(String name, MajorType type, SchemaPath source,
        String value) {
      super(name, type, source, value);
    }

    @Override
    public int nodeType() { return 100; }

    @Override
    public ColumnProjection unresolve() { return null; }
  }

  /**
   * Test the static column loader using one column of each type.
   * The null column is of type int, but the associated value is of
   * type string. This is a bit odd, but works out because we detect that
   * the string value is null and call setNull on the writer, and avoid
   * using the actual data.
   */

  @Test
  public void testConstantColumnLoader() {

    MajorType aType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.REQUIRED)
        .build();
    MajorType bType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();

    List<ConstantColumn> defns = new ArrayList<>();
    defns.add(
        new DummyColumn("a", aType, SchemaPath.getSimplePath("a"), "a-value" ));
    defns.add(
        new DummyColumn("b", bType, SchemaPath.getSimplePath("b"), "b-value" ));

    ResultVectorCacheImpl cache = new ResultVectorCacheImpl(fixture.allocator());
    ConstantColumnLoader staticLoader = new ConstantColumnLoader(cache, defns);

    // Create a batch

    staticLoader.load(2);

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", aType)
        .add("b", bType)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("a-value", "b-value")
        .addRow("a-value", "b-value")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(staticLoader.output()));
    staticLoader.close();
  }

  @Test
  public void testFileMetadata() {

    FileMetadata fileInfo = new FileMetadata(new Path("hdfs:///w/x/y/z.csv"), new Path("hdfs:///w"));
    List<ConstantColumn> defns = new ArrayList<>();
    FileMetadataColumnDefn iDefn = new FileMetadataColumnDefn(
        ScanTestUtils.SUFFIX_COL, ImplicitFileColumns.SUFFIX);
    ResolvedFileMetadataColumn iCol = new ResolvedFileMetadataColumn(ScanTestUtils.SUFFIX_COL,
        SchemaPath.getSimplePath(ScanTestUtils.SUFFIX_COL),
        iDefn, fileInfo);
    defns.add(iCol);

    String partColName = ScanTestUtils.partitionColName(1);
    ResolvedPartitionColumn pCol = new ResolvedPartitionColumn(partColName,
        SchemaPath.getSimplePath(partColName), 1, fileInfo);
    defns.add(pCol);

    ResultVectorCacheImpl cache = new ResultVectorCacheImpl(fixture.allocator());
    ConstantColumnLoader staticLoader = new ConstantColumnLoader(cache, defns);

    // Create a batch

    staticLoader.load(2);

    // Verify

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable(ScanTestUtils.SUFFIX_COL, MinorType.VARCHAR)
        .addNullable(partColName, MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("csv", "y")
        .addRow("csv", "y")
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(staticLoader.output()));
    staticLoader.close();
  }
}
