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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.SchemaTracker;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.project.ScanProjector;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestScanProjector extends SubOperatorTest {
  private ProjectionFixture buildFixture(String... cols) {
    ProjectionFixture projFixture = new ProjectionFixture()
        .withFileParser(fixture.options())
        .projectedCols(cols);
    projFixture.metadataParser.useLegacyWildcardExpansion(false);
    projFixture.metadataParser.setScanRootDir(new Path("hdfs:///w"));
    projFixture.build();
    return projFixture;
  }

  private ScanProjector buildProjector(String... cols) {
    ProjectionFixture projFixture = buildFixture(cols);
    return new ScanProjector(fixture.allocator(), projFixture.scanProj, projFixture.metadataProj, null);
  }

  private ScanProjector buildProjectorWithNulls(MajorType nullType, String... cols) {
    ProjectionFixture projFixture = buildFixture(cols);
    return new ScanProjector(fixture.allocator(), projFixture.scanProj, projFixture.metadataProj, nullType);
  }

  /**
   * Test the ability of the scan projector to "smooth" out schema changes
   * by reusing the type from a previous reader, if known. That is,
   * given three readers:<br>
   * (a, b)<br>
   * (b)<br>
   * (a, b)<br>
   * Then the type of column a should be preserved for the second reader that
   * does not include a. This works if a is nullable. If so, a's type will
   * be used for the empty column, rather than the usual nullable int.
   * <p>
   * Detailed testing of type matching for "missing" columns is done
   * in {@link #testNullColumnLoader()}.
   * <p>
   * As a side effect, makes sure that two identical tables (in this case,
   * separated by a different table) results in no schema change.
   */

  @Test
  public void testSchemaSmoothing() {

    // SELECT a, b ...

    ScanProjector projector = buildProjector("a", "b");

    // file schema (a, b)

    TupleMetadata twoColSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .buildSchema();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // ... FROM file a.csv

      projector.startFile(new Path("hdfs:///w/x/y/a.csv"));

      // Projection of (a, b) to (a, b)

      ResultSetLoader loader = projector.makeTableLoader(twoColSchema);

      loader.startBatch();
      loader.writer()
          .addRow(10, "fred")
          .addRow(20, "wilma");
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(twoColSchema)
          .addRow(10, "fred")
          .addRow(20, "wilma")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/b.csv"));

      // File schema (a)

      TupleMetadata oneColSchema = new SchemaBuilder()
          .add("a", MinorType.INT)
          .buildSchema();

      // Projection of (a) to (a, b), reusing b from above.

      ResultSetLoader loader = projector.makeTableLoader(oneColSchema);

      loader.startBatch();
      loader.writer()
          .addRow(30)
          .addRow(40);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(twoColSchema)
          .addRow(30, null)
          .addRow(40, null)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // ... FROM file c.csv

      projector.startFile(new Path("hdfs:///w/x/y/c.csv"));

      // Projection of (a, b), to (a, b), reusing b yet again

      ResultSetLoader loader = projector.makeTableLoader(twoColSchema);

      loader.startBatch();
      loader.writer()
          .addRow(50, "dino")
          .addRow(60, "barney");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(twoColSchema)
          .addRow(50, "dino")
          .addRow(60, "barney")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * Verify that different table column orders are projected into the
   * SELECT order, preserving vectors, so no schema change for column
   * reordering.
   */

  @Test
  public void testColumnReordering() {

    ScanProjector projector = buildProjector("a", "b", "c");

    TupleMetadata schema1 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("c", MinorType.BIGINT)
        .buildSchema();
    TupleMetadata schema2 = new SchemaBuilder()
        .add("c", MinorType.BIGINT)
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .buildSchema();
    TupleMetadata schema3 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("c", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .buildSchema();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // ... FROM file a.csv

      projector.startFile(new Path("hdfs:///w/x/y/a.csv"));

      // Projection of (a, b, c) to (a, b, c)

      ResultSetLoader loader = projector.makeTableLoader(schema1);

      loader.startBatch();
      loader.writer()
          .addRow(10, "fred", 110L)
          .addRow(20, "wilma", 110L);
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(schema1)
          .addRow(10, "fred", 110L)
          .addRow(20, "wilma", 110L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/b.csv"));

      // Projection of (c, a, b) to (a, b, c)

      ResultSetLoader loader = projector.makeTableLoader(schema2);

      loader.startBatch();
      loader.writer()
          .addRow(330L, 30, "bambam")
          .addRow(440L, 40, "betty");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(schema1)
          .addRow(30, "bambam", 330L)
          .addRow(40, "betty", 440L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // ... FROM file c.csv

      projector.startFile(new Path("hdfs:///w/x/y/c.csv"));

      // Projection of (a, c, b) to (a, b, c)

      ResultSetLoader loader = projector.makeTableLoader(schema3);

      loader.startBatch();
      loader.writer()
          .addRow(50, 550L, "dino")
          .addRow(60, 660L, "barney");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(schema1)
          .addRow(50, "dino", 550L)
          .addRow(60, "barney", 660L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }

  /**
   * A SELECT * query uses the schema of the table as the output schema.
   * This is trivial when the scanner has one table. But, if two or more
   * tables occur, then things get interesting. The first table sets the
   * schema. The second table then has:
   * <ul>
   * <li>The same schema, trivial case.</li>
   * <li>A subset of the first table. The type of the "missing" column
   * from the first table is used for a null column in the second table.</li>
   * <li>A superset or disjoint set of the first schema. This triggers a hard schema
   * change.</li>
   * </ul>
   * <p>
   * It is an open question whether previous columns should be preserved on
   * a hard reset. For now, the code implements, and this test verifies, that a
   * hard reset clears the "memory" of prior schemas.
   */

  @Test
  public void testWildcardSmoothing() {

    ScanProjector projector = buildProjector(SchemaPath.WILDCARD);

    TupleMetadata firstSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .addNullable("c", MinorType.BIGINT)
        .buildSchema();
    TupleMetadata subsetSchema = new SchemaBuilder()
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("a", MinorType.INT)
        .buildSchema();
    TupleMetadata disjointSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR, 10)
        .add("d", MinorType.VARCHAR)
        .buildSchema();

    SchemaTracker tracker = new SchemaTracker();
    int schemaVersion;
    {
      // First table, establishes the baseline
      // ... FROM file a.csv

      projector.startFile(new Path("hdfs:///w/x/y/a.csv"));

      ResultSetLoader loader = projector.makeTableLoader(firstSchema);

      loader.startBatch();
      loader.writer()
          .addRow(10, "fred", 110L)
          .addRow(20, "wilma", 110L);
      projector.publish();

      tracker.trackSchema(projector.output());
      schemaVersion = tracker.schemaVersion();

      SingleRowSet expected = fixture.rowSetBuilder(firstSchema)
          .addRow(10, "fred", 110L)
          .addRow(20, "wilma", 110L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Second table, same schema, the trivial case
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/b.csv"));

      ResultSetLoader loader = projector.makeTableLoader(firstSchema);

      loader.startBatch();
      loader.writer()
          .addRow(70, "pebbles", 770L)
          .addRow(80, "hoppy", 880L);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(firstSchema)
          .addRow(70, "pebbles", 770L)
          .addRow(80, "hoppy", 880L)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Third table: subset schema of first two
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/c.csv"));

      ResultSetLoader loader = projector.makeTableLoader(subsetSchema);

      loader.startBatch();
      loader.writer()
          .addRow("bambam", 30)
          .addRow("betty", 40);
      projector.publish();

      tracker.trackSchema(projector.output());
      assertEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(firstSchema)
          .addRow(30, "bambam", null)
          .addRow(40, "betty", null)
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }
    {
      // Fourth table: disjoint schema, cases a schema reset
      // ... FROM file b.csv

      projector.startFile(new Path("hdfs:///w/x/y/c.csv"));

      ResultSetLoader loader = projector.makeTableLoader(disjointSchema);

      loader.startBatch();
      loader.writer()
          .addRow(50, "dino", "supporting")
          .addRow(60, "barney", "main");
      projector.publish();

      tracker.trackSchema(projector.output());
      assertNotEquals(schemaVersion, tracker.schemaVersion());

      SingleRowSet expected = fixture.rowSetBuilder(disjointSchema)
          .addRow(50, "dino", "supporting")
          .addRow(60, "barney", "main")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(projector.output()));
    }

    projector.close();
  }
  // TODO: Test schema smoothing with repeated
  // TODO: Test hard schema change
  // TODO: Typed null column tests (resurrect)
  // TODO: Test maps and arrays of maps
}
