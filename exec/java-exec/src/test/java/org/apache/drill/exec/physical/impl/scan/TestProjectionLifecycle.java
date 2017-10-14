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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.file.ResolvedFileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.NullColumn;
import org.apache.drill.exec.physical.impl.scan.project.ProjectedColumn;
import org.apache.drill.exec.physical.impl.scan.project.ProjectionLifecycle;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestProjectionLifecycle extends SubOperatorTest {

  /**
   * Sanity test for the simple, discrete case. The purpose of
   * discrete is just to run the basic lifecycle in a way that
   * is compatible with the schema-persistence version.
   */

  @Test
  public void testDiscrete() {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options())
        .projectedCols(ScanTestUtils.FILE_NAME_COL, "a", "b");
    projFixture.metdataParser.useLegacyWildcardExpansion(true);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      // Define a file a.csv

      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));

      // Verify

      List<ColumnProjection> fileSchema = lifecycle.fileProjection().outputCols();
      assertEquals(ScanTestUtils.FILE_NAME_COL, fileSchema.get(0).name());
      assertEquals("a.csv", ((ResolvedFileMetadataColumn) fileSchema.get(0)).value());
      assertEquals(UnresolvedColumn.UNRESOLVED, fileSchema.get(1).nodeType());

      // Build the output schema from the (a, b) table schema

      TupleMetadata twoColSchema = new SchemaBuilder()
          .add("a", MinorType.INT)
          .addNullable("b", MinorType.VARCHAR, 10)
          .buildSchema();
      lifecycle.startSchema(twoColSchema);
      assertEquals(1, lifecycle.schemaVersion());

      // Verify the full output schema

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("filename", MinorType.VARCHAR)
          .add("a", MinorType.INT)
          .addNullable("b", MinorType.VARCHAR, 10)
          .buildSchema();

      // Verify

      List<ResolvedColumn> tableSchema = lifecycle.tableProjection().output();
      assertTrue(ScanTestUtils.schema(tableSchema).isEquivalent(expectedSchema));
      assertEquals(ScanTestUtils.FILE_NAME_COL, tableSchema.get(0).name());
      assertEquals("a.csv", ((ResolvedFileMetadataColumn) tableSchema.get(0)).value());
      assertEquals(ProjectedColumn.ID, tableSchema.get(1).nodeType());
    }
    {
      // Define a file b.csv

      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));

      // Verify

      assertNull(lifecycle.tableProjection());
      List<ColumnProjection> fileSchema = lifecycle.fileProjection().outputCols();
      assertEquals(3, fileSchema.size());
      assertEquals(ScanTestUtils.FILE_NAME_COL, fileSchema.get(0).name());
      assertEquals("b.csv", ((ResolvedFileMetadataColumn) fileSchema.get(0)).value());
      assertEquals(UnresolvedColumn.UNRESOLVED, fileSchema.get(1).nodeType());
      assertEquals(UnresolvedColumn.UNRESOLVED, fileSchema.get(2).nodeType());

      // Build the output schema from the (a) table schema

      TupleMetadata oneColSchema = new SchemaBuilder()
          .add("a", MinorType.INT)
          .buildSchema();
      lifecycle.startSchema(oneColSchema);
      assertEquals(2, lifecycle.schemaVersion());

      // Verify the full output schema
      // Since this mode is "discrete", we don't remember the type
      // of the missing column. (Instead, it is filled in at the
      // vector level as part of vector persistence.)

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("filename", MinorType.VARCHAR)
          .add("a", MinorType.INT)
          .addNullable("b", MinorType.NULL)
          .buildSchema();

      // Verify

      List<ResolvedColumn> tableSchema = lifecycle.tableProjection().output();
      assertTrue(ScanTestUtils.schema(tableSchema).isEquivalent(expectedSchema));
      assertEquals(3, tableSchema.size());
      assertEquals(ScanTestUtils.FILE_NAME_COL, tableSchema.get(0).name());
      assertEquals("b.csv", ((ResolvedFileMetadataColumn) tableSchema.get(0)).value());
      assertEquals(ProjectedColumn.ID, tableSchema.get(1).nodeType());
      assertEquals(NullColumn.ID, tableSchema.get(2).nodeType());
    }
  }

  /**
   * Case in which the table schema is a superset of the prior
   * schema. Discard prior schema. Turn off auto expansion of
   * metadata for a simpler test.
   */

  @Test
  public void testSmaller() {
    TupleMetadata priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(priorSchema);
      assertEquals(1, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(priorSchema));
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(2, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(tableSchema));
    }
  }

  /**
   * Case in which the table schema and prior are disjoint
   * sets. Discard the prior schema.
   */

  @Test
  public void testDisjoint() {
    TupleMetadata priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(priorSchema);
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(2, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(tableSchema));
    }
  }

  /**
   * Column names match, but types differ. Discard the prior schema.
   */

  @Test
  public void testDifferentTypes() {
    TupleMetadata priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(priorSchema);
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(2, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(tableSchema));
    }
  }

  /**
   * The prior and table schemas are identical. Preserve the prior
   * schema (though, the output is no different than if we discarded
   * the prior schema...)
   */

  @Test
  public void testSameSchemas() {
    TupleMetadata priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(priorSchema);
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(1, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(tableSchema));
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(priorSchema));
    }
  }

  /**
   * The prior and table schemas are identical, but the cases of names differ.
   * Preserve the case of the first schema.
   */

  @Test
  public void testDifferentCase() {
    TupleMetadata priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.INT)
        .add("B", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(priorSchema);
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(1, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(priorSchema));
    }
  }

  /**
   * Can't preserve the prior schema if it had required columns
   * where the new schema has no columns.
   */

  @Test
  public void testRequired() {
    TupleMetadata priorSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR)
        .buildSchema();
    TupleMetadata tableSchema = new SchemaBuilder()
        .addNullable("b", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(priorSchema);
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(2, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(tableSchema));
    }
  }

  /**
   * Preserve the prior schema if table is a subset and missing columns
   * are nullable or repeated.
   */

  @Test
  public void testSmoothing() {
    TupleMetadata priorSchema = new SchemaBuilder()
        .addNullable("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addArray("c", MinorType.BIGINT)
        .buildSchema();
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(priorSchema);
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(1, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(priorSchema));
    }
  }

  /**
   * Preserve the prior schema if table is a subset. Map the table
   * columns to the output using the prior schema orderng.
   */

  @Test
  public void testReordering() {
    TupleMetadata priorSchema = new SchemaBuilder()
        .addNullable("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .addArray("c", MinorType.BIGINT)
        .buildSchema();
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("b", MinorType.VARCHAR)
        .addNullable("a", MinorType.INT)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(false);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(priorSchema);
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(1, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(priorSchema));
    }
  }

  /**
   * If using the legacy wildcard expansion, reuse schema if partition paths
   * are the same length.
   */

  @Test
  public void testSamePartitionLength() {
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(true);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    TupleMetadata expectedSchema = projFixture.expandMetadata(tableSchema, 2);
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(tableSchema);
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(expectedSchema));
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(1, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(expectedSchema));
     }
  }

  /**
   * If using the legacy wildcard expansion, reuse schema if the new partition path
   * is shorter than the previous. (Unneded partitions will be set to null by the
   * scan projector.)
   */

  @Test
  public void testShorterPartitionLength() {
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(true);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    TupleMetadata expectedSchema = projFixture.expandMetadata(tableSchema, 2);
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/a.csv"));
      lifecycle.startSchema(tableSchema);
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(expectedSchema));
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(1, lifecycle.schemaVersion());
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(expectedSchema));
     }
  }

  /**
   * If using the legacy wildcard expansion, create a new schema if the new partition path
   * is longer than the previous.
   */

  @Test
  public void testLongerPartitionLength() {
    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options());
    projFixture.scanBuilder.projectAll();
    projFixture.metdataParser.useLegacyWildcardExpansion(true);
    projFixture.metdataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    ProjectionLifecycle lifecycle = ProjectionLifecycle.newLifecycle(projFixture.scanProj, projFixture.metadataProj);

    {
      lifecycle.startFile(new Path("hdfs:///w/x/a.csv"));
      lifecycle.startSchema(tableSchema);
      TupleMetadata expectedSchema = projFixture.expandMetadata(tableSchema, 1);
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(expectedSchema));
    }
    {
      lifecycle.startFile(new Path("hdfs:///w/x/y/b.csv"));
      lifecycle.startSchema(tableSchema);
      assertEquals(2, lifecycle.schemaVersion());
      TupleMetadata expectedSchema = projFixture.expandMetadata(tableSchema, 2);
      assertTrue(ScanTestUtils.schema(lifecycle.tableProjection().output()).isEquivalent(expectedSchema));
     }
  }
}
