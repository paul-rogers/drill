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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.SchemaTracker;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator.ReaderSchemaOrchestrator;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.WildcardSchemaProjection;
import org.apache.drill.exec.physical.impl.scan.project.SchemaSmoother;
import org.apache.drill.exec.physical.impl.scan.project.SchemaSmoother.IncompatibleSchemaException;
import org.apache.drill.exec.physical.impl.scan.project.SchemaSmoother.SmoothingProjection;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests schema smoothing at the schema projection level.
 * This level handles reusing prior types when filling null
 * values. But, because no actual vectors are involved, it
 * does not handle the schema chosen for a table ahead of
 * time, only the schema as it is merged with prior schema to
 * detect missing columns.
 */

public class TestSchemaSmoothing extends SubOperatorTest {
  @Test
  public void testSmoothingProjection() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectAll(), Lists.newArrayList());

    ScanTestUtils.DummySource dummySource = new ScanTestUtils.DummySource();

    // Table 1: (a: nullable bigint, b)

    TupleMetadata schema1 = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR)
        .add("c", MinorType.FLOAT8)
        .buildSchema();
    List<ResolvedColumn> priorSchema = new ArrayList<>();
    {
      WildcardSchemaProjection schemaProj = new WildcardSchemaProjection(
          scanProj, schema1, dummySource,
          new ArrayList<>());
      priorSchema = schemaProj.columns();
    }

    // Table 2: (a: nullable bigint, c), column ommitted, original schema preserved

    TupleMetadata schema2 = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .add("c", MinorType.FLOAT8)
        .buildSchema();
    try {
      SmoothingProjection schemaProj = new SmoothingProjection(
          scanProj, schema2, dummySource, dummySource,
          new ArrayList<>(), priorSchema);
      assertTrue(schema1.isEquivalent(ScanTestUtils.schema(schemaProj.columns())));
    } catch (IncompatibleSchemaException e) {
      fail();
    }

    // Table 3: (a, c, d), column added, must replan schema

    TupleMetadata schema3 = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR)
        .add("c", MinorType.FLOAT8)
        .add("d", MinorType.INT)
        .buildSchema();
    try {
      new SmoothingProjection(
          scanProj, schema3, dummySource, dummySource,
          new ArrayList<>(), priorSchema);
      fail();
    } catch (IncompatibleSchemaException e) {
      // Expected
    }

    // Table 4: (a: double), change type must replan schema

    TupleMetadata schema4 = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .addNullable("b", MinorType.VARCHAR)
        .add("c", MinorType.FLOAT8)
        .buildSchema();
    try {
      new SmoothingProjection(
          scanProj, schema4, dummySource, dummySource,
          new ArrayList<>(), priorSchema);
      fail();
    } catch (IncompatibleSchemaException e) {
      // Expected
    }

//    // Table 5: (a: not-nullable bigint): convert to nullable for consistency
//
//    TupleMetadata schema5 = new SchemaBuilder()
//        .addNullable("a", MinorType.BIGINT)
//        .add("c", MinorType.FLOAT8)
//        .buildSchema();
//    try {
//      SmoothingProjection schemaProj = new SmoothingProjection(
//          scanProj, schema5, dummySource, dummySource,
//          new ArrayList<>(), priorSchema);
//      assertTrue(schema1.isEquivalent(ScanTestUtils.schema(schemaProj.columns())));
//    } catch (IncompatibleSchemaException e) {
//      fail();
//    }

    // Table 6: Drop a non-nullable column, must replan

    TupleMetadata schema6 = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR)
        .buildSchema();
    try {
      new SmoothingProjection(
          scanProj, schema6, dummySource, dummySource,
          new ArrayList<>(), priorSchema);
      fail();
    } catch (IncompatibleSchemaException e) {
      // Expected
    }
  }

  @Test
  public void testSmoothableSchemaBatches() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        ScanTestUtils.projectAll(), Lists.newArrayList());

    ScanTestUtils.DummySource dummySource = new ScanTestUtils.DummySource();
    SchemaSmoother smoother = new SchemaSmoother(scanProj, dummySource, new ArrayList<>());

    // Table 1: (a: bigint, b)

    TupleMetadata schema1 = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR)
        .add("c", MinorType.FLOAT8)
        .buildSchema();
    {
      SchemaLevelProjection schemaProj = smoother.resolve(schema1, dummySource);

      // Just use the original schema.

      assertTrue(schema1.isEquivalent(ScanTestUtils.schema(schemaProj.columns())));
    }

    // Table 2: (a: nullable bigint, c), column ommitted, original schema preserved

    TupleMetadata schema2 = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .add("c", MinorType.FLOAT8)
        .buildSchema();
    {
      SchemaLevelProjection schemaProj = smoother.resolve(schema2, dummySource);
      assertTrue(schema1.isEquivalent(ScanTestUtils.schema(schemaProj.columns())));
    }

    // Table 3: (a, c, d), column added, must replan schema

    TupleMetadata schema3 = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR)
        .add("c", MinorType.FLOAT8)
        .add("d", MinorType.INT)
        .buildSchema();
    {
      SchemaLevelProjection schemaProj = smoother.resolve(schema3, dummySource);
      assertTrue(schema3.isEquivalent(ScanTestUtils.schema(schemaProj.columns())));
    }

    // Table 4: Drop a non-nullable column, must replan

    TupleMetadata schema4 = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR)
        .buildSchema();
    {
      SchemaLevelProjection schemaProj = smoother.resolve(schema4, dummySource);
      assertTrue(schema4.isEquivalent(ScanTestUtils.schema(schemaProj.columns())));
    }

    // Table 5: (a: double), change type must replan schema

    TupleMetadata schema5 = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .addNullable("b", MinorType.VARCHAR)
        .buildSchema();
    {
      SchemaLevelProjection schemaProj = smoother.resolve(schema5, dummySource);
      assertTrue(schema5.isEquivalent(ScanTestUtils.schema(schemaProj.columns())));
    }

//    // Table 6: (a: not-nullable bigint): convert to nullable for consistency
//
//    TupleMetadata schema6 = new SchemaBuilder()
//        .add("a", MinorType.FLOAT8)
//        .add("b", MinorType.VARCHAR)
//        .buildSchema();
//    {
//      SchemaLevelProjection schemaProj = smoother.resolve(schema3, dummySource);
//      assertTrue(schema5.isEquivalent(ScanTestUtils.schema(schemaProj.columns())));
//    }
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
    ScanSchemaOrchestrator projector = new ScanSchemaOrchestrator(fixture.allocator());
    projector.enableSchemaSmoothing(true);
    projector.build(ScanTestUtils.projectAll());

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
      // ... FROM table 1

      ReaderSchemaOrchestrator reader = projector.startReader();
      ResultSetLoader loader = reader.makeTableLoader(firstSchema);

      reader.startBatch();
      loader.writer()
          .addRow(10, "fred", 110L)
          .addRow(20, "wilma", 110L);
      reader.endBatch();

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
      // ... FROM table 2

      ReaderSchemaOrchestrator reader = projector.startReader();
      ResultSetLoader loader = reader.makeTableLoader(firstSchema);

      reader.startBatch();
      loader.writer()
          .addRow(70, "pebbles", 770L)
          .addRow(80, "hoppy", 880L);
      reader.endBatch();

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
      // ... FROM table 3

      ReaderSchemaOrchestrator reader = projector.startReader();
      ResultSetLoader loader = reader.makeTableLoader(subsetSchema);

      reader.startBatch();
      loader.writer()
          .addRow("bambam", 30)
          .addRow("betty", 40);
      reader.endBatch();

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
      // ... FROM table 4

      ReaderSchemaOrchestrator reader = projector.startReader();
      ResultSetLoader loader = reader.makeTableLoader(disjointSchema);

      reader.startBatch();
      loader.writer()
          .addRow(50, "dino", "supporting")
          .addRow(60, "barney", "main");
      reader.endBatch();

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
