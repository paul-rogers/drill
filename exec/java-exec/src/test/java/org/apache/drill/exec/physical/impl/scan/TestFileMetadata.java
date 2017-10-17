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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.file.FileLevelProjection;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedFileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedPartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ProjectedColumn;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFileMetadata extends SubOperatorTest {


  /**
   * Resolve a selection list using SELECT *.
   */

  @Test
  public void testWildcardNew() {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options())
        .projectedCols(
            ScanTestUtils.FILE_NAME_COL,
            SchemaPath.WILDCARD,
            ScanTestUtils.partitionColName(0));
    projFixture.metadataParser.useLegacyWildcardExpansion(false);
    projFixture.metadataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();

    FileLevelProjection fileProj = projFixture.resolve("hdfs:///w/x/y/z.csv");
    assertEquals(3, fileProj.outputCols().size());
    assertTrue(fileProj.hasMetadata());

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    TableLevelProjection tableProj = fileProj.resolveFile(tableSchema);
    assertFalse(tableProj.hasNullColumns());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("filename", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();

    assertTrue(ScanTestUtils.schema(tableProj.output()).isEquivalent(expectedSchema));

    assertEquals(ResolvedFileMetadataColumn.ID, tableProj.output().get(0).nodeType());
    assertEquals(ProjectedColumn.ID, tableProj.output().get(1).nodeType());
    assertEquals(ProjectedColumn.ID, tableProj.output().get(2).nodeType());
    assertEquals(ProjectedColumn.ID, tableProj.output().get(3).nodeType());
    assertEquals(ResolvedPartitionColumn.ID, tableProj.output().get(4).nodeType());

    boolean selMap[] = tableProj.projectionMap();
    assertEquals(3, selMap.length);
    assertTrue(selMap[0]);
    assertTrue(selMap[1]);
    assertTrue(selMap[2]);

    int lToPMap[] = tableProj.logicalToPhysicalMap();
    assertEquals(0, lToPMap[0]);
    assertEquals(1, lToPMap[1]);
    assertEquals(2, lToPMap[2]);

    int projMap[] = tableProj.tableColumnProjectionMap();
    assertEquals(1, projMap[0]);
    assertEquals(2, projMap[1]);
    assertEquals(3, projMap[2]);

    int mdProj[] = tableProj.metadataProjection();
    assertEquals(0, mdProj[0]);
    assertEquals(4, mdProj[1]);
  }

}
