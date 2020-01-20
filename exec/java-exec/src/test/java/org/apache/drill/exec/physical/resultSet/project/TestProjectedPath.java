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
package org.apache.drill.exec.physical.resultSet.project;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.junit.Test;

/**
 * Projection creates a pattern which we match against a particular type
 * to see if the projection path is consistent with the type. Tests here
 * verify the consistency checks.
 */
public class TestProjectedPath {

  // INT is a proxy for all scalar columns.
  private static final ColumnMetadata INT_COLUMN = intSchema().metadata("a");
  private static final ColumnMetadata INT_ARRAY_COLUMN = intArraySchema().metadata("a");
  private static final ColumnMetadata MAP_COLUMN = mapSchema().metadata("a");
  private static final ColumnMetadata MAP_ARRAY_COLUMN = mapArraySchema().metadata("a");
  private static final ColumnMetadata DICT_COLUMN = dictSchema().metadata("a");
  private static final ColumnMetadata DICT_ARRAY_COLUMN = dictArraySchema().metadata("a");
  private static final ColumnMetadata UNION_COLUMN = unionSchema().metadata("a");
  private static final ColumnMetadata LIST_COLUMN = listSchema().metadata("a");

  private static TupleMetadata intSchema() {
    return new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();
  }

  private static TupleMetadata intArraySchema() {
    return new SchemaBuilder()
        .addArray("a", MinorType.INT)
        .build();
  }

  private static TupleMetadata mapSchema() {
    return new SchemaBuilder()
        .addMap("a")
          .add("i", MinorType.INT)
          .addMap("m")
            .add("mi", MinorType.INT)
            .resumeMap()
          .resumeSchema()
        .build();
  }

  private static TupleMetadata mapArraySchema() {
    return new SchemaBuilder()
        .addMapArray("a")
          .add("i", MinorType.INT)
          .addMap("m")
            .add("mi", MinorType.INT)
            .resumeMap()
          .resumeSchema()
        .build();
  }

  private static TupleMetadata dictSchema() {
    return new SchemaBuilder()
        .addDict("a", MinorType.VARCHAR)
          .value(MinorType.INT)
          .resumeSchema()
        .build();
  }

  private static TupleMetadata dictArraySchema() {
    return new SchemaBuilder()
        .addDictArray("a", MinorType.VARCHAR)
          .value(MinorType.INT)
          .resumeSchema()
        .build();
  }

  private static TupleMetadata unionSchema() {
    return new SchemaBuilder()
        .addUnion("a")
          .addType(MinorType.INT)
          .resumeSchema()
        .build();
  }

  private static TupleMetadata listSchema() {
    return new SchemaBuilder()
        .addList("a")
          .addType(MinorType.INT)
          .resumeSchema()
        .build();
  }

  private void assertConsistent(RequestedTuple projSet, ColumnMetadata col) {
    assertTrue(projSet.isConsistentWith(col));
    assertTrue(projSet.isConsistentWith(col.name(), col.majorType()));
  }

  private void assertNotConsistent(RequestedTuple projSet, ColumnMetadata col) {
    assertFalse(projSet.isConsistentWith(col));
    assertFalse(projSet.isConsistentWith(col.name(), col.majorType()));
  }

  private void assertAllConsistent(RequestedTuple projSet) {
    assertConsistent(projSet, INT_COLUMN);
    assertConsistent(projSet, INT_ARRAY_COLUMN);
    assertConsistent(projSet, MAP_COLUMN);
    assertConsistent(projSet, MAP_ARRAY_COLUMN);
    assertConsistent(projSet, DICT_COLUMN);
    assertConsistent(projSet, DICT_ARRAY_COLUMN);
    assertConsistent(projSet, UNION_COLUMN);
    assertConsistent(projSet, LIST_COLUMN);
  }

  @Test
  public void testSimplePath() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a"));

    assertAllConsistent(projSet);

    // No constraints on an unprojected column.

    assertTrue(projSet.isConsistentWith("b", INT_COLUMN.majorType()));
  }

  @Test
  public void testProjectAll() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectAll());

    // No constraints on wildcard projection
    assertAllConsistent(projSet);
  }

  @Test
  public void testProjectNone() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectNone());

    // No constraints on empty projection
    assertAllConsistent(projSet);
  }

  @Test
  public void test1DArray() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[0]"));

    assertNotConsistent(projSet, INT_COLUMN);
    assertConsistent(projSet, INT_ARRAY_COLUMN);
    assertNotConsistent(projSet, MAP_COLUMN);
    assertConsistent(projSet, MAP_ARRAY_COLUMN);
    assertNotConsistent(projSet, DICT_COLUMN);
    assertConsistent(projSet, DICT_ARRAY_COLUMN);
    assertConsistent(projSet, UNION_COLUMN);
    assertConsistent(projSet, LIST_COLUMN);
  }

  @Test
  public void test2DArray() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[0][1]"));

    assertNotConsistent(projSet, INT_COLUMN);
    assertNotConsistent(projSet, INT_ARRAY_COLUMN);
    assertNotConsistent(projSet, MAP_COLUMN);
    assertNotConsistent(projSet, MAP_ARRAY_COLUMN);
    assertNotConsistent(projSet, DICT_COLUMN);
    assertNotConsistent(projSet, DICT_ARRAY_COLUMN);
    assertConsistent(projSet, UNION_COLUMN);
    assertConsistent(projSet, LIST_COLUMN);
  }

  @Test
  public void test3DArray() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[0][1][2]"));

    assertNotConsistent(projSet, INT_COLUMN);
    assertNotConsistent(projSet, INT_ARRAY_COLUMN);
    assertNotConsistent(projSet, MAP_COLUMN);
    assertNotConsistent(projSet, MAP_ARRAY_COLUMN);
    assertNotConsistent(projSet, DICT_COLUMN);
    assertNotConsistent(projSet, DICT_ARRAY_COLUMN);
    assertConsistent(projSet, UNION_COLUMN);
    assertConsistent(projSet, LIST_COLUMN);
  }

  @Test
  public void testMap() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a.b"));

    assertNotConsistent(projSet, INT_COLUMN);
    assertNotConsistent(projSet, INT_ARRAY_COLUMN);
    assertConsistent(projSet, MAP_COLUMN);
    assertConsistent(projSet, MAP_ARRAY_COLUMN);
    assertNotConsistent(projSet, DICT_COLUMN);
    assertNotConsistent(projSet, DICT_ARRAY_COLUMN);

    // A UNION could contain a map, which would allow the
    // a.b path to be valid.
    assertConsistent(projSet, UNION_COLUMN);
    // A LIST could be a list of MAPs, so a.b could mean
    // to pick out the b column in all array entries.
    assertConsistent(projSet, LIST_COLUMN);
  }

  @Test
  public void testMapArray() {
    RequestedTuple projSet = Projections.parse(
        RowSetTestUtils.projectList("a[0].b"));

    assertNotConsistent(projSet, INT_COLUMN);
    assertNotConsistent(projSet, INT_ARRAY_COLUMN);
    assertNotConsistent(projSet, MAP_COLUMN);
    assertConsistent(projSet, MAP_ARRAY_COLUMN);
    assertNotConsistent(projSet, DICT_COLUMN);
    assertNotConsistent(projSet, DICT_ARRAY_COLUMN);

    // A UNION could contain a repeated map, which would allow the
    // a.b path to be valid.
    assertConsistent(projSet, UNION_COLUMN);
    // A LIST could contain MAPs.
    assertConsistent(projSet, LIST_COLUMN);
  }

  // TODO: Test DICT projection when supported.

}
