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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.projSet.TypeConverter.CustomTypeTransform;
import org.apache.drill.exec.physical.rowSet.ProjectionSet;
import org.apache.drill.exec.physical.rowSet.ProjectionSet.ColumnReadProjection;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.ConvertStringToInt;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions;
import org.junit.Test;

public class TestProjectionSet {

  /**
   * Empty projection, no schema
   */

  @Test
  public void testEmptyProjection() {
    ProjectionSet projSet = ProjectionSetFactory.projectNone();

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    ColumnMetadata readColSchema = readSchema.metadata("a");
    ColumnReadProjection col = projSet.readProjection(readColSchema);
    assertFalse(col.isProjected());
  }

  /**
   * Wildcard projection, no schema
   */

  @Test
  public void testWildcardProjection() {
    ProjectionSet projSet = ProjectionSetFactory.projectAll();

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    ColumnMetadata readColSchema = readSchema.metadata("a");
    ColumnReadProjection col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());
  }

  /**
   * Wildcard projection, with schema. Some columns marked
   * as special; not expanded by wildcard.
   */

  @Test
  public void testWildcardAndSchemaProjection() {
    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.INT)
        .add("d", MinorType.INT)
        .buildSchema();
    readSchema.metadata("b").setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);

    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("c", MinorType.INT)
        .add("d", MinorType.INT)
        .buildSchema();
    outputSchema.metadata("c").setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);

    TypeConverter converter = TypeConverter.builder()
        .providedSchema(outputSchema)
        .build();

    ProjectionSet projSet = new WildcardProjectionSet(converter);

    ColumnReadProjection col = projSet.readProjection(readSchema.metadata("a"));
    assertTrue(col.isProjected());
    assertSame(outputSchema.metadata("a"), col.outputSchema());
    assertNotNull(col.conversionFactory());

    // Column b marked as special by reader

    col = projSet.readProjection(readSchema.metadata("b"));
    assertFalse(col.isProjected());
    assertSame(readSchema.metadata("b"), col.outputSchema());
    assertNull(col.conversionFactory());

    // Column c marked as special by provided schema

    col = projSet.readProjection(readSchema.metadata("c"));
    assertFalse(col.isProjected());
    assertSame(readSchema.metadata("c"), col.outputSchema());
    assertNull(col.conversionFactory());

    // Column d needs no conversion

    col = projSet.readProjection(readSchema.metadata("d"));
    assertTrue(col.isProjected());
    assertSame(outputSchema.metadata("d"), col.outputSchema());
    assertNull(col.conversionFactory());
  }

  /**
   * Wildcard and strict schema
   */

  @Test
  public void testWildcardAndStrictSchemaProjection() {
    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
         .buildSchema();

    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    outputSchema.setBooleanProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP, true);

    TypeConverter converter = TypeConverter.builder()
        .providedSchema(outputSchema)
        .build();

    ProjectionSet projSet = new WildcardProjectionSet(converter);

    ColumnReadProjection col = projSet.readProjection(readSchema.metadata("a"));
    assertTrue(col.isProjected());
    assertSame(outputSchema.metadata("a"), col.outputSchema());
    assertNotNull(col.conversionFactory());

    // Column b not in provided schema

    col = projSet.readProjection(readSchema.metadata("b"));
    assertFalse(col.isProjected());
    assertSame(readSchema.metadata("b"), col.outputSchema());
    assertNull(col.conversionFactory());
  }

  /**
   * Explicit projection of three forms: wildcard, explcit, none.
   * Wildcard and none already tested above, here we test the
   * builder. No schema.
   */

  @Test
  public void testExplicitProjection() {
    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    ColumnMetadata readColSchema = readSchema.metadata("a");

    // Project all

    ProjectionSet projSet = ProjectionSetFactory.build(null);

    ColumnReadProjection col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());

    // Project none

    projSet = ProjectionSetFactory.build(new ArrayList<>());

    col = projSet.readProjection(readColSchema);
    assertFalse(col.isProjected());

    // Project some

    projSet = ProjectionSetFactory.build(
        RowSetTestUtils.projectList("a"));

    col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());
    assertFalse(projSet.readProjection(readSchema.metadata("b")).isProjected());
  }

  /**
   * Explicit projection of three forms: wildcard, explcit, none.
   * Wildcard and none already tested above, here we test the
   * builder. With schema.
   */

  @Test
  public void testExplicitSchemaProjection() {
    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
         .buildSchema();

    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    outputSchema.setBooleanProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP, true);

    TypeConverter converter = TypeConverter.builder()
        .providedSchema(outputSchema)
        .build();

    ColumnMetadata readColSchema = readSchema.metadata("a");

    // Project all

    ProjectionSet projSet = new ProjectionSetBuilder()
        .transform(converter)
        .build();

    ColumnReadProjection col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());
    assertSame(outputSchema.metadata("a"), col.outputSchema());
    assertNotNull(col.conversionFactory());

    // Project none

    projSet = new ProjectionSetBuilder()
        .transform(converter)
        .projectionList(new ArrayList<>())
        .build();

    col = projSet.readProjection(readColSchema);
    assertFalse(col.isProjected());

    // Project some]

   projSet = new ProjectionSetBuilder()
        .transform(converter)
        .projectionList(RowSetTestUtils.projectList("a"))
        .build();

    col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());
    assertSame(outputSchema.metadata("a"), col.outputSchema());
    assertNotNull(col.conversionFactory());

    assertFalse(projSet.readProjection(readSchema.metadata("b")).isProjected());
  }

  /**
   * Wildcard projection, no schema, custom column transform.
   */

  @Test
  public void testTransformConversion() {
    ColumnConversionFactory conv = StandardConversions.factory(ConvertStringToInt.class);
    CustomTypeTransform customTransform = ProjectionSetFactory.simpleTransform(conv);
    TypeConverter typeConverter = TypeConverter.builder()
        .transform(customTransform)
        .build();

    ProjectionSet projSet = new WildcardProjectionSet(typeConverter);

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    ColumnMetadata readColSchema = readSchema.metadata("a");
    ColumnReadProjection col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());
    assertSame(conv, col.conversionFactory());
  }
}
