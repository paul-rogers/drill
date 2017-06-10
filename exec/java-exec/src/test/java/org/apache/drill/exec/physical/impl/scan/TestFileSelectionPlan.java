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

import org.apache.drill.exec.physical.impl.scan.OutputColumn.ColumnType;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.MetadataColumn;
import org.apache.drill.exec.physical.impl.scan.OutputColumn.ResolvedFileInfo;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFileSelectionPlan extends SubOperatorTest {

  /**
   * Test the file selection planner with metadata.
   */

  @Test
  public void testWithMetadata() {
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());
    querySelBuilder.useLegacyStarPlan(false);

    querySelBuilder.queryCols(TestQuerySelectionPlan.selectList("filename", "a", "dir0"));
    QuerySelectionPlan querySelPlan = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(querySelPlan, fileInfo);
    assertTrue(fileSel.hasMetadata());
    assertSame(querySelPlan, fileSel.selectionPlan());
    assertEquals(3, fileSel.output().size());

    assertEquals(ColumnType.FILE_METADATA, fileSel.output().get(0).columnType());
    assertEquals("filename", fileSel.output().get(0).name());
    assertEquals("z.csv", ((OutputColumn.MetadataColumn) fileSel.output().get(0)).value());
    assertEquals(ColumnType.TABLE, fileSel.output().get(1).columnType());
    assertEquals("a", fileSel.output().get(1).name());
    assertEquals(ColumnType.PARTITION, fileSel.output().get(2).columnType());
    assertEquals("dir0", fileSel.output().get(2).name());
    assertEquals("x", ((OutputColumn.MetadataColumn) fileSel.output().get(2)).value());

    assertEquals(2, fileSel.metadataColumns().size());
    assertSame(fileSel.output().get(0), fileSel.metadataColumns().get(0));
    assertSame(fileSel.output().get(2), fileSel.metadataColumns().get(1));
    assertEquals(0, fileSel.metadataProjection()[0]);
    assertEquals(2, fileSel.metadataProjection()[1]);
  }

  /**
   * Test the file selection planner without metadata.
   */

  @Test
  public void testWithoutMetadata() {
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());
    querySelBuilder.useLegacyStarPlan(false);

    querySelBuilder.queryCols(TestQuerySelectionPlan.selectList("a"));
    QuerySelectionPlan querySelPlan = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(querySelPlan, fileInfo);
    assertFalse(fileSel.hasMetadata());
    assertEquals(1, fileSel.output().size());

    assertEquals(ColumnType.TABLE, fileSel.output().get(0).columnType());
    assertEquals("a", fileSel.output().get(0).name());

    // Not guaranteed to be null. Actually, if hasMetadata() is false,
    // don't even look at the metadata information.
    assertNull(fileSel.metadataColumns());
  }

  /**
   * For obscure reasons, Drill 1.10 and earlier would add all implicit
   * columns in a SELECT *, then would remove them again in a PROJECT
   * if not needed.
   */

  @Test
  public void testLegacyWildcard() {
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());
    querySelBuilder.useLegacyStarPlan(true);
    querySelBuilder.selectAll();
    QuerySelectionPlan querySelPlan = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(querySelPlan, fileInfo);

    assertTrue(fileSel.hasMetadata());
    assertEquals(7, fileSel.output().size());

    assertEquals(ColumnType.WILDCARD, fileSel.output().get(0).columnType());
    assertEquals("fqn", fileSel.output().get(1).name());
    assertEquals("filepath", fileSel.output().get(2).name());
    assertEquals("filename", fileSel.output().get(3).name());
    assertEquals("suffix", fileSel.output().get(4).name());
    assertEquals("dir0", fileSel.output().get(5).name());
    assertEquals("dir1", fileSel.output().get(6).name());

    assertEquals("hdfs:/w/x/y/z.csv", ((MetadataColumn) fileSel.output().get(1)).value());
    assertEquals("hdfs:/w/x/y", ((MetadataColumn) fileSel.output().get(2)).value());
    assertEquals("z.csv", ((MetadataColumn) fileSel.output().get(3)).value());
    assertEquals("csv", ((MetadataColumn) fileSel.output().get(4)).value());
    assertEquals("x", ((MetadataColumn) fileSel.output().get(5)).value());
    assertEquals("y", ((MetadataColumn) fileSel.output().get(6)).value());
  }


  @Test
  public void testFileMetadata() {
    QuerySelectionPlan.Builder querySelBuilder = new QuerySelectionPlan.Builder(fixture.options());
    querySelBuilder.useLegacyStarPlan(true);
    querySelBuilder.queryCols(TestQuerySelectionPlan.selectList("a", "fqn",
        "filEPath", // Sic, to test case sensitivity
        "filename", "suffix"));
    QuerySelectionPlan querySelPlan = querySelBuilder.build();

    ResolvedFileInfo fileInfo = new ResolvedFileInfo(new Path("hdfs:///w/x/y/z.csv"), "hdfs:///w");
    FileSelectionPlan fileSel = new FileSelectionPlan(querySelPlan, fileInfo);

    assertTrue(fileSel.hasMetadata());
    assertEquals(5, fileSel.output().size());

    assertEquals("fqn", fileSel.output().get(1).name());
    assertEquals("filEPath", fileSel.output().get(2).name());
    assertEquals("filename", fileSel.output().get(3).name());
    assertEquals("suffix", fileSel.output().get(4).name());

    assertEquals("hdfs:/w/x/y/z.csv", ((MetadataColumn) fileSel.output().get(1)).value());
    assertEquals("hdfs:/w/x/y", ((MetadataColumn) fileSel.output().get(2)).value());
    assertEquals("z.csv", ((MetadataColumn) fileSel.output().get(3)).value());
    assertEquals("csv", ((MetadataColumn) fileSel.output().get(4)).value());
  }

  /**
   * Test the obscure case that the partition column contains two digits:
   * dir11. Also tests the obscure case that the output only has partition
   * columns.
   */

  @Test
  public void testPartitionColumnTwoDigits() {
    QuerySelectionPlan.Builder builder = new QuerySelectionPlan.Builder(fixture.options());

    builder.queryCols(TestQuerySelectionPlan.selectList("dir11"));

    QuerySelectionPlan querySelPlan = builder.build();
    assertEquals("dir11", querySelPlan.outputCols().get(0).name());

    Path path = new Path("hdfs:///x/0/1/2/3/4/5/6/7/8/9/10/d11/z.csv");
    ResolvedFileInfo fileInfo = new ResolvedFileInfo(path, "hdfs:///x");
    FileSelectionPlan fileSel = new FileSelectionPlan(querySelPlan, fileInfo);

     assertEquals("d11", ((MetadataColumn) fileSel.output().get(0)).value());
  }

}
