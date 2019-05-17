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
package org.apache.drill.exec.physical.impl.scan.project.projSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ProjectionSet.ColumnReadProjection;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.physical.rowSet.project.ImpliedTupleRequest;
import org.apache.drill.exec.physical.rowSet.project.ProjectionType;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.physical.rowSet.project.RequestedTupleImpl;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.ConvertStringToInt;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions;
import org.junit.Test;

public class TestProjectionCols {

  @Test
  public void testUnprojected() {

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();

    ColumnMetadata readColSchema = readSchema.metadata("a");
    ColumnReadProjection col = new UnprojectedReadColumn(readColSchema);
    assertFalse(col.isProjected());
    assertSame(readColSchema, col.readSchema());
    assertSame(readColSchema, col.providedSchema());
    assertNull(col.conversionFactory());
    assertSame(ImpliedTupleRequest.NO_MEMBERS, col.mapProjection());
    assertNull(col.projectionType());
  }

  @Test
  public void testWildcard() {

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();

    ColumnMetadata readColSchema = readSchema.metadata("a");
    ColumnReadProjection col = new ProjectedReadColumn(readColSchema);
    assertTrue(col.isProjected());
    assertSame(readColSchema, col.readSchema());
    assertSame(readColSchema, col.providedSchema());
    assertNull(col.conversionFactory());

    // We don't really care if this is a map or not.
    // If it is, project everything. If not, don't
    // ask the question.

    assertSame(ImpliedTupleRequest.ALL_MEMBERS, col.mapProjection());
    assertNull(col.projectionType());
  }

  @Test
  public void testExplicitScalarProjection() {

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();

    RequestedTuple proj = RequestedTupleImpl.parse(RowSetTestUtils.projectList("a"));
    RequestedColumn projCol = proj.get("a");

    ColumnMetadata readColSchema = readSchema.metadata("a");
    ColumnReadProjection col = new ProjectedReadColumn(readColSchema, projCol);
    assertTrue(col.isProjected());
    assertSame(readColSchema, col.readSchema());
    assertSame(readColSchema, col.providedSchema());
    assertNull(col.conversionFactory());
    assertSame(ImpliedTupleRequest.ALL_MEMBERS, col.mapProjection());
    assertEquals(ProjectionType.GENERAL, col.projectionType());
  }

  @Test
  public void testExplicitMapProjectAll() {

    TupleMetadata readSchema = new SchemaBuilder()
        .addMap("m")
          .add("a", MinorType.INT)
          .resumeSchema()
        .buildSchema();

    RequestedTuple proj = RequestedTupleImpl.parse(RowSetTestUtils.projectList("m"));
    RequestedColumn projCol = proj.get("m");

    ColumnMetadata readColSchema = readSchema.metadata("m");
    ColumnReadProjection col = new ProjectedReadColumn(readColSchema, projCol);
    assertTrue(col.isProjected());
    assertSame(readColSchema, col.readSchema());
    assertSame(readColSchema, col.providedSchema());
    assertNull(col.conversionFactory());
    assertSame(ImpliedTupleRequest.ALL_MEMBERS, col.mapProjection());
    assertEquals(ProjectionType.GENERAL, col.projectionType());
  }

  @Test
  public void testExplicitMapProjectOne() {

    TupleMetadata readSchema = new SchemaBuilder()
        .addMap("m")
          .add("a", MinorType.INT)
          .resumeSchema()
        .buildSchema();

    RequestedTuple proj = RequestedTupleImpl.parse(RowSetTestUtils.projectList("m.a"));
    RequestedColumn projCol = proj.get("m");

    ColumnMetadata readColSchema = readSchema.metadata("m");
    ColumnReadProjection col = new ProjectedReadColumn(readColSchema, projCol);
    assertTrue(col.isProjected());
    assertSame(readColSchema, col.readSchema());
    assertSame(readColSchema, col.providedSchema());
    assertNull(col.conversionFactory());
    assertSame(projCol.mapProjection(), col.mapProjection());
    assertEquals(ProjectionType.TUPLE, col.projectionType());
  }

  @Test
  public void testExplicitSchemaProjection() {

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();

    RequestedTuple proj = RequestedTupleImpl.parse(RowSetTestUtils.projectList("a"));
    RequestedColumn projCol = proj.get("a");

    ColumnMetadata readColSchema = readSchema.metadata("a");
    ColumnMetadata outputColSchema = outputSchema.metadata("a");
    ColumnConversionFactory conv = StandardConversions.factory(ConvertStringToInt.class);
    ColumnReadProjection col =
        new ProjectedReadColumn(readColSchema, projCol, outputColSchema, conv);
    assertTrue(col.isProjected());
    assertSame(readColSchema, col.readSchema());
    assertSame(outputColSchema, col.providedSchema());
    assertSame(conv, col.conversionFactory());
    assertSame(ImpliedTupleRequest.ALL_MEMBERS, col.mapProjection());
    assertEquals(ProjectionType.GENERAL, col.projectionType());
  }

  @Test
  public void testExpandedSchemaProjection() {

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();

    ColumnMetadata readColSchema = readSchema.metadata("a");
    ColumnMetadata outputColSchema = outputSchema.metadata("a");
    ColumnConversionFactory conv = StandardConversions.factory(ConvertStringToInt.class);
    ColumnReadProjection col =
        new ProjectedReadColumn(readColSchema, null, outputColSchema, conv);
    assertTrue(col.isProjected());
    assertSame(readColSchema, col.readSchema());
    assertSame(outputColSchema, col.providedSchema());
    assertSame(conv, col.conversionFactory());
    assertSame(ImpliedTupleRequest.ALL_MEMBERS, col.mapProjection());
    assertNull(col.projectionType());
  }
}
