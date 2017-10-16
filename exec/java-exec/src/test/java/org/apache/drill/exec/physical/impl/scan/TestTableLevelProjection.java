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
import static org.junit.Assert.assertSame;

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ProjectionFixture;
import org.apache.drill.exec.physical.impl.scan.project.ReaderLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.RowBatchMerger.VectorSource;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection.ExplicitTableProjection;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.TableLevelProjection.WildcardTableProjection;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestTableLevelProjection extends SubOperatorTest {

  public static class DummySource implements VectorSource {

    @Override
    public ValueVector getVector(int fromIndex) {
      // Not a real source!
      throw new UnsupportedOperationException();
    }

    @Override
    public BatchSchema getSchema() {
      // Not a real source!
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void testWildcard() {
    ProjectionFixture projFixture = new ProjectionFixture()
        .projectAll();
    projFixture.build();

    ReaderLevelProjection fileProj = projFixture.resolveReader();
    assertEquals(1, fileProj.output().size());

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("c", MinorType.INT)
        .addArray("d", MinorType.FLOAT8)
        .buildSchema();

    DummySource fileSource = new DummySource();
    TableLevelProjection tableProj = new WildcardTableProjection(fileProj, tableSchema, fileSource);
    assertEquals(3, tableProj.columns().size());

    List<ResolvedColumn> columns = tableProj.columns();
    assertEquals("a", columns.get(0).name());
    assertEquals(0, columns.get(0).projection().sourceIndex());
    assertEquals(0, columns.get(0).projection().destIndex());
    assertSame(fileSource, columns.get(0).projection().source());
    assertEquals("c", columns.get(1).name());
    assertEquals(1, columns.get(1).projection().sourceIndex());
    assertEquals(1, columns.get(1).projection().destIndex());
    assertSame(fileSource, columns.get(1).projection().source());
    assertEquals("d", columns.get(2).name());
    assertEquals(2, columns.get(2).projection().sourceIndex());
    assertEquals(2, columns.get(2).projection().destIndex());
    assertSame(fileSource, columns.get(2).projection().source());
  }
  /**
   * Test SELECT list with columns defined in a order and with
   * name case different than the early-schema table.
   */

  @Test
  public void testFullList() {

    // Simulate SELECT c, b, a ...

    ProjectionFixture projFixture = new ProjectionFixture()
        .projectedCols("c", "b", "a");
    projFixture.build();

    ReaderLevelProjection fileProj = projFixture.resolveReader();
    assertEquals(3, fileProj.output().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    DummySource fileSource = new DummySource();
    DummySource nullSource = new DummySource();
    TableLevelProjection tableProj = new ExplicitTableProjection(fileProj, tableSchema, fileSource, nullSource);
    assertEquals(3, tableProj.columns().size());

    List<ResolvedColumn> columns = tableProj.columns();
    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).projection().sourceIndex());
    assertEquals(0, columns.get(0).projection().destIndex());
    assertSame(fileSource, columns.get(0).projection().source());

    assertEquals("b", columns.get(1).name());
    assertEquals(1, columns.get(1).projection().sourceIndex());
    assertEquals(1, columns.get(1).projection().destIndex());
    assertSame(fileSource, columns.get(1).projection().source());

    assertEquals("a", columns.get(2).name());
    assertEquals(0, columns.get(2).projection().sourceIndex());
    assertEquals(2, columns.get(2).projection().destIndex());
    assertSame(fileSource, columns.get(2).projection().source());
  }

  /**
   * Test SELECT list with columns missing from the table schema.
   */

  @Test
  public void testMissing() {

    // Simulate SELECT c, v, b, w ...

    ProjectionFixture projFixture = new ProjectionFixture()
        .projectedCols("c", "v", "b", "w");
    projFixture.build();

    ReaderLevelProjection fileProj = projFixture.resolveReader();
    assertEquals(4, fileProj.output().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    DummySource fileSource = new DummySource();
    DummySource nullSource = new DummySource();
    TableLevelProjection tableProj = new ExplicitTableProjection(fileProj, tableSchema, fileSource, nullSource);
    assertEquals(4, tableProj.columns().size());

    @SuppressWarnings("unused")
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .addNullable("v", MinorType.NULL)
        .add("b", MinorType.VARCHAR)
        .addNullable("w", MinorType.NULL)
        .buildSchema();

    List<ResolvedColumn> columns = tableProj.columns();
    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).projection().sourceIndex());
    assertEquals(0, columns.get(0).projection().destIndex());
    assertSame(fileSource, columns.get(0).projection().source());

    assertEquals("v", columns.get(1).name());
    assertEquals(0, columns.get(1).projection().sourceIndex());
    assertEquals(1, columns.get(1).projection().destIndex());
    assertSame(nullSource, columns.get(1).projection().source());

    assertEquals("b", columns.get(2).name());
    assertEquals(1, columns.get(2).projection().sourceIndex());
    assertEquals(2, columns.get(2).projection().destIndex());
    assertSame(fileSource, columns.get(2).projection().source());

    assertEquals("w", columns.get(3).name());
    assertEquals(1, columns.get(3).projection().sourceIndex());
    assertEquals(3, columns.get(3).projection().destIndex());
    assertSame(nullSource, columns.get(3).projection().source());
  }

  @Test
  public void testSubset() {

    // Simulate SELECT c, a ...

    ProjectionFixture projFixture = new ProjectionFixture()
        .projectedCols("c", "a");
    projFixture.build();

    ReaderLevelProjection fileProj = projFixture.resolveReader();
    assertEquals(2, fileProj.output().size());

    // Simulate a data source, with early schema, of (a, b, c)

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("B", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    DummySource fileSource = new DummySource();
    DummySource nullSource = new DummySource();
    TableLevelProjection tableProj = new ExplicitTableProjection(fileProj, tableSchema, fileSource, nullSource);
    assertEquals(2, tableProj.columns().size());

    @SuppressWarnings("unused")
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("c", MinorType.VARCHAR)
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    List<ResolvedColumn> columns = tableProj.columns();
    assertEquals("c", columns.get(0).name());
    assertEquals(2, columns.get(0).projection().sourceIndex());
    assertEquals(0, columns.get(0).projection().destIndex());
    assertSame(fileSource, columns.get(0).projection().source());

    assertEquals("a", columns.get(1).name());
    assertEquals(0, columns.get(1).projection().sourceIndex());
    assertEquals(1, columns.get(1).projection().destIndex());
    assertSame(fileSource, columns.get(1).projection().source());
  }

  // TODO: Custom columns array type
}
