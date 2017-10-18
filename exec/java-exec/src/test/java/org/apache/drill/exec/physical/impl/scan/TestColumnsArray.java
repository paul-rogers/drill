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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayProjection;
import org.apache.drill.exec.physical.impl.scan.file.FileLevelProjection;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedFileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedPartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ProjectedColumn;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestColumnsArray extends SubOperatorTest {


  /**
   * Test columns array. The table must be able to support it by having all
   * Varchar columns.
   */

  @Test
  public void testColumnsArray() {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options())
        .withColumnsArrayParser()
        .projectedCols(
            ScanTestUtils.FILE_NAME_COL,
            ColumnsArrayProjection.COLUMNS_COL,
            ScanTestUtils.partitionColName(0));
    projFixture.metadataParser.useLegacyWildcardExpansion(false);
    projFixture.metadataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();

    FileLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .buildSchema();

    SchemaLevelProjection tableProj = fileProj.resolveFile(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("filename", MinorType.VARCHAR)
        .addArray("columns", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();

    assertTrue(ScanTestUtils.schema(tableProj.output()).isEquivalent(expectedSchema));

    assertEquals(ResolvedFileMetadataColumn.ID, tableProj.output().get(0).nodeType());

    // The columns array is now an actual table column.

    assertEquals(ProjectedColumn.ID, tableProj.output().get(1).nodeType());
    assertEquals(ResolvedPartitionColumn.ID, tableProj.output().get(2).nodeType());

    boolean selMap[] = tableProj.projectionMap();
    assertTrue(selMap[0]);

    int lToPMap[] = tableProj.logicalToPhysicalMap();
    assertEquals(0, lToPMap[0]);

    int projMap[] = tableProj.tableColumnProjectionMap();
    assertEquals(1, projMap[0]);
  }


  /**
   * Test attempting to use the columns array with an early schema with
   * column types not compatible with a varchar array.
   */

  @Test
  public void testColumnsArrayIncompatible() {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options())
        .withColumnsArrayParser()
        .projectedCols(
            ScanTestUtils.FILE_NAME_COL,
            ColumnsArrayProjection.COLUMNS_COL,
            ScanTestUtils.partitionColName(0));
    projFixture.metadataParser.useLegacyWildcardExpansion(false);
    projFixture.metadataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();

    FileLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .add("d", MinorType.FLOAT8)
        .buildSchema();

    try {
      fileProj.resolveFile(tableSchema);
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

}
