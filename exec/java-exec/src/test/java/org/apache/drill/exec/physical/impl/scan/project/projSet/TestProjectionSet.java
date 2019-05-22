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
        .addMap("m")
          .add("b", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();

    ColumnReadProjection aCol = projSet.readProjection(readSchema.metadata("a"));
    assertFalse(aCol.isProjected());

    ColumnReadProjection mCol = projSet.readProjection(readSchema.metadata("m"));
    assertFalse(mCol.isProjected());

    ColumnReadProjection bCol = mCol.mapProjection().readProjection(
        readSchema.metadata("m").mapSchema().metadata("b"));
    assertFalse(bCol.isProjected());
  }

  /**
   * Wildcard projection, no schema
   */

  @Test
  public void testWildcardProjection() {
    ProjectionSet projSet = ProjectionSetFactory.projectAll();

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addMap("m")
          .add("b", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();

    ColumnReadProjection aCol = projSet.readProjection(readSchema.metadata("a"));
    assertTrue(aCol.isProjected());

    ColumnReadProjection mCol = projSet.readProjection(readSchema.metadata("m"));
    assertTrue(mCol.isProjected());

    ColumnReadProjection bCol = mCol.mapProjection().readProjection(
        readSchema.metadata("m").mapSchema().metadata("b"));
    assertTrue(bCol.isProjected());
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
        .addMap("m")
          .add("e", MinorType.VARCHAR)
          .add("f", MinorType.VARCHAR)
          .add("g", MinorType.VARCHAR)
          .add("h", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    readSchema.metadata("b").setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
    TupleMetadata mReadSchema = readSchema.metadata("m").mapSchema();
    mReadSchema.metadata("f").setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);

    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("c", MinorType.INT)
        .add("d", MinorType.INT)
        .addMap("m")
          .add("e", MinorType.INT)
          .add("f", MinorType.VARCHAR)
          .add("g", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    outputSchema.metadata("c").setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
    TupleMetadata mOutputSchema = outputSchema.metadata("m").mapSchema();
    mOutputSchema.metadata("g").setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);

    TypeConverter converter = TypeConverter.builder()
        .providedSchema(outputSchema)
        .build();

    ProjectionSet projSet = new WildcardProjectionSet(converter);

    ColumnReadProjection aCol = projSet.readProjection(readSchema.metadata("a"));
    assertTrue(aCol.isProjected());
    assertSame(outputSchema.metadata("a"), aCol.providedSchema());
    assertNotNull(aCol.conversionFactory());

    // Column b marked as special by reader

    ColumnReadProjection bCol = projSet.readProjection(readSchema.metadata("b"));
    assertFalse(bCol.isProjected());
    assertSame(readSchema.metadata("b"), bCol.providedSchema());
    assertNull(bCol.conversionFactory());

    // Column c marked as special by provided schema

    ColumnReadProjection cCol = projSet.readProjection(readSchema.metadata("c"));
    assertFalse(cCol.isProjected());
    assertSame(readSchema.metadata("c"), cCol.providedSchema());
    assertNull(cCol.conversionFactory());

    // Column d needs no conversion

    ColumnReadProjection dCol = projSet.readProjection(readSchema.metadata("d"));
    assertTrue(dCol.isProjected());
    assertSame(outputSchema.metadata("d"), dCol.providedSchema());
    assertNull(dCol.conversionFactory());

    // Column m is a map

    ColumnReadProjection mCol = projSet.readProjection(readSchema.metadata("m"));
    assertTrue(mCol.isProjected());
    assertSame(outputSchema.metadata("m"), mCol.providedSchema());
    assertNull(mCol.conversionFactory());
    ProjectionSet mProj = mCol.mapProjection();

    // Column m.e requires conversion

    ColumnReadProjection eCol = mProj.readProjection(mReadSchema.metadata("e"));
    assertTrue(eCol.isProjected());
    assertSame(mReadSchema.metadata("e"), eCol.readSchema());
    assertSame(mOutputSchema.metadata("e"), eCol.providedSchema());
    assertNotNull(eCol.conversionFactory());

    // Column m.f marked as special by reader

    ColumnReadProjection fCol = mProj.readProjection(mReadSchema.metadata("f"));
    assertFalse(fCol.isProjected());

    // Column m.g marked as special by provided schema

    ColumnReadProjection gCol = mProj.readProjection(mReadSchema.metadata("g"));
    assertFalse(gCol.isProjected());

    // Column m.h needs no conversion

    ColumnReadProjection hCol = mProj.readProjection(mReadSchema.metadata("h"));
    assertTrue(hCol.isProjected());
    assertSame(mReadSchema.metadata("h"), hCol.providedSchema());
    assertNull(hCol.conversionFactory());
  }

  /**
   * Wildcard and strict schema
   */

  @Test
  public void testWildcardAndStrictSchemaProjection() {
    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .addMap("m")
          .add("c", MinorType.INT)
          .add("d", MinorType.VARCHAR)
          .resumeSchema()
        .addMap("m2")
          .add("e", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    TupleMetadata mReadSchema = readSchema.metadata("m").mapSchema();
    TupleMetadata m2ReadSchema = readSchema.metadata("m2").mapSchema();

    TupleMetadata outputSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .add("c", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    outputSchema.setBooleanProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP, true);
    TupleMetadata mOutputSchema = outputSchema.metadata("m").mapSchema();

    TypeConverter converter = TypeConverter.builder()
        .providedSchema(outputSchema)
        .build();

    ProjectionSet projSet = new WildcardProjectionSet(converter);

    ColumnReadProjection aCol = projSet.readProjection(readSchema.metadata("a"));
    assertTrue(aCol.isProjected());
    assertSame(outputSchema.metadata("a"), aCol.providedSchema());
    assertNotNull(aCol.conversionFactory());

    // Column b not in provided schema

    ColumnReadProjection bCol = projSet.readProjection(readSchema.metadata("b"));
    assertFalse(bCol.isProjected());
    assertSame(readSchema.metadata("b"), bCol.providedSchema());
    assertNull(bCol.conversionFactory());

    // Column m is a map in provided schema

    ColumnReadProjection mCol = projSet.readProjection(readSchema.metadata("m"));
    assertTrue(mCol.isProjected());
    assertSame(outputSchema.metadata("m"), mCol.providedSchema());
    assertNull(mCol.conversionFactory());
    ProjectionSet mProj = mCol.mapProjection();

    // Column m.c is in the provided schema

    ColumnReadProjection cCol = mProj.readProjection(mReadSchema.metadata("c"));
    assertTrue(cCol.isProjected());
    assertSame(mOutputSchema.metadata("c"), cCol.providedSchema());
    assertNull(cCol.conversionFactory());

    // Column m.d is not in the provided schema

    ColumnReadProjection dCol = mProj.readProjection(mReadSchema.metadata("d"));
    assertFalse(dCol.isProjected());

    // Column m2, a map, is not in the provided schema

    ColumnReadProjection m2Col = projSet.readProjection(mReadSchema.metadata("d"));
    assertFalse(m2Col.isProjected());
    ProjectionSet m2Proj = mCol.mapProjection();

    // Since m2 is not in the provided schema, its members are not projected.

    ColumnReadProjection eCol = m2Proj.readProjection(m2ReadSchema.metadata("e"));
    assertFalse(eCol.isProjected());
  }

  /**
   * Test explicit projection without a provided schema.
   * Also, sanity test of the builder for the project all,
   * project none cases.
   */

  @Test
  public void testExplicitProjection() {
    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .addMap("m1")
          .add("c", MinorType.INT)
          .add("d", MinorType.VARCHAR)
          .resumeSchema()
        .addMap("m2")
          .add("e", MinorType.INT)
          .resumeSchema()
        .addMap("m3")
          .add("f", MinorType.INT)
          .resumeSchema()
        .buildSchema();

    ColumnMetadata aSchema = readSchema.metadata("a");
    ColumnMetadata m1Schema = readSchema.metadata("m1");
    ColumnMetadata m2Schema = readSchema.metadata("m2");
    ColumnMetadata m3Schema = readSchema.metadata("m3");
    TupleMetadata m1ReadSchema = m1Schema.mapSchema();
    TupleMetadata m2ReadSchema = m2Schema.mapSchema();
    TupleMetadata m3ReadSchema = m3Schema.mapSchema();

    // Project all

    ProjectionSet projSet = ProjectionSetFactory.build(null);

    assertTrue(projSet.readProjection(aSchema).isProjected());
    assertTrue(projSet.readProjection(m1Schema).isProjected());

    // Project none

    projSet = ProjectionSetFactory.build(new ArrayList<>());

    assertFalse(projSet.readProjection(aSchema).isProjected());
    assertFalse(projSet.readProjection(m1Schema).isProjected());

    // Project some

    projSet = ProjectionSetFactory.build(
        RowSetTestUtils.projectList("a", "m1.c", "m2"));

    assertTrue(projSet.readProjection(aSchema).isProjected());
    assertFalse(projSet.readProjection(readSchema.metadata("b")).isProjected());

    ColumnReadProjection m1Col = projSet.readProjection(m1Schema);
    assertTrue(m1Col.isProjected());
    assertTrue(m1Col.mapProjection().readProjection(m1ReadSchema.metadata("c")).isProjected());
    assertFalse(m1Col.mapProjection().readProjection(m1ReadSchema.metadata("d")).isProjected());

    ColumnReadProjection m2Col = projSet.readProjection(m2Schema);
    assertTrue(m2Col.isProjected());
    assertTrue(m2Col.mapProjection().readProjection(m2ReadSchema.metadata("e")).isProjected());

    ColumnReadProjection m3Col = projSet.readProjection(m3Schema);
    assertFalse(m3Col.isProjected());
    assertFalse(m3Col.mapProjection().readProjection(m3ReadSchema.metadata("f")).isProjected());
  }

  /**
   * Explicit projection with implied wildcard projection of the map.
   * That is, SELECT m is logically equivalent to SELECT m.*
   * and is subject to the strict schema projection rule.
   */

  @Test
  public void testImpliedWildcardWithStrictSchema() {
    TupleMetadata readSchema = new SchemaBuilder()
        .addMap("m")
          .add("a", MinorType.INT)
          .add("b", MinorType.INT)
          .resumeSchema()
        .buildSchema();

    ColumnMetadata mSchema = readSchema.metadata("m");
    TupleMetadata mReadSchema = mSchema.mapSchema();

    TupleMetadata outputSchema = new SchemaBuilder()
        .addMap("m")
          .add("a", MinorType.INT)
          .resumeSchema()
        .buildSchema();

    outputSchema.setBooleanProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP, true);

    ProjectionSet projSet = new ProjectionSetBuilder()
        .typeConverter(TypeConverter.builder()
            .providedSchema(outputSchema)
            .build())
        .projectionList(RowSetTestUtils.projectList("m"))
        .build();

    ColumnReadProjection mCol = projSet.readProjection(mSchema);
    assertTrue(mCol.isProjected());
    ProjectionSet mProj = mCol.mapProjection();
    assertTrue(mProj.readProjection(mReadSchema.metadata("a")).isProjected());
    assertFalse(mProj.readProjection(mReadSchema.metadata("b")).isProjected());
  }

  /**
   * Explicit projection of three forms: wildcard, explicit, none.
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
        .typeConverter(converter)
        .build();

    ColumnReadProjection col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());
    assertSame(outputSchema.metadata("a"), col.providedSchema());
    assertNotNull(col.conversionFactory());

    // Project none

    projSet = new ProjectionSetBuilder()
        .typeConverter(converter)
        .projectionList(new ArrayList<>())
        .build();

    col = projSet.readProjection(readColSchema);
    assertFalse(col.isProjected());

    // Project some]

   projSet = new ProjectionSetBuilder()
        .typeConverter(converter)
        .projectionList(RowSetTestUtils.projectList("a"))
        .build();

    col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());
    assertSame(outputSchema.metadata("a"), col.providedSchema());
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
