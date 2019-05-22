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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ProjectionSet;
import org.apache.drill.exec.physical.rowSet.ProjectionSet.ColumnReadProjection;
import org.apache.drill.exec.physical.rowSet.ProjectionSet.CustomTypeTransform;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.ConvertStringToInt;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions;
import org.junit.Test;

public class TestProjectionSet {

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

  @Test
  public void testExplicitProjection() {
    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .buildSchema();

    ColumnMetadata readColSchema = readSchema.metadata("a");

    ProjectionSet projSet = ProjectionSetFactory.build(null);

    ColumnReadProjection col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());

    projSet = ProjectionSetFactory.build(new ArrayList<>());

    col = projSet.readProjection(readColSchema);
    assertFalse(col.isProjected());

    projSet = ProjectionSetFactory.build(
        RowSetTestUtils.projectList("a"));

    col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());
    assertFalse(projSet.readProjection(readSchema.metadata("b")).isProjected());
  }

  @Test
  public void testTransformProjection() {
    ColumnConversionFactory conv = StandardConversions.factory(ConvertStringToInt.class);
    CustomTypeTransform customTransform = ProjectionSetFactory.simpleTransform(conv);

    ProjectionSet projSet = new WildcardAndTransformProjectionSet(customTransform);

    TupleMetadata readSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    ColumnMetadata readColSchema = readSchema.metadata("a");
    ColumnReadProjection col = projSet.readProjection(readColSchema);
    assertTrue(col.isProjected());
    assertSame(conv, col.conversionFactory());
  }
}
