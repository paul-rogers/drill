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
package org.apache.drill.test.rowSet.test;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RowSetTest {

  private static OperatorFixture fixture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    fixture = OperatorFixture.standardFixture();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
  }

  @Test
  public void testSchema() {
    RowSetSchema schema = RowSetSchema.builder()
        .add("c", MinorType.INT)
        .add("a", MinorType.INT, DataMode.REPEATED)
        .addNullable("b", MinorType.VARCHAR)
        .build();

    assertEquals(3, schema.count());

    assertEquals("c", schema.get(0).getName());
    assertEquals(MinorType.INT, schema.get(0).getType().getMinorType());
    assertEquals(DataMode.REQUIRED, schema.get(0).getDataMode());
    assertEquals(DataMode.REQUIRED, schema.get(0).getType().getMode());
    assertTrue(! schema.get(0).isNullable());

    assertEquals("a", schema.get(1).getName());
    assertEquals(MinorType.INT, schema.get(1).getType().getMinorType());
    assertEquals(DataMode.REPEATED, schema.get(1).getDataMode());
    assertEquals(DataMode.REPEATED, schema.get(1).getType().getMode());
    assertTrue(! schema.get(1).isNullable());

    assertEquals("b", schema.get(2).getName());
    assertEquals(MinorType.VARCHAR, schema.get(2).getType().getMinorType());
    assertEquals(DataMode.OPTIONAL, schema.get(2).getDataMode());
    assertEquals(DataMode.OPTIONAL, schema.get(2).getType().getMode());
    assertTrue(schema.get(2).isNullable());

    BatchSchema batchSchema = schema.toBatchSchema(SelectionVectorMode.NONE);
    assertEquals("c", batchSchema.getColumn(0).getName());
    assertEquals("a", batchSchema.getColumn(1).getName());
    assertEquals("b", batchSchema.getColumn(2).getName());
  }

  @Test
  public void testMapSchema() {
    RowSetSchema schema = RowSetSchema.builder()
        .add("c", MinorType.INT)
        .addMap("a")
          .addNullable("b", MinorType.VARCHAR)
          .add("d", MinorType.INT)
          .addMap("e")
            .add("f", MinorType.VARCHAR)
            .buildMap()
          .add("g", MinorType.INT)
          .buildMap()
        .add("h", MinorType.BIGINT)
        .build();

    assertEquals(3, schema.count());
    assertEquals("c", schema.get(0).getName());
    assertNull(schema.getColumn(0).getMembers());
    assertEquals("a", schema.get(1).getName());
    assertEquals("h", schema.get(2).getName());

    RowSetSchema mapA = schema.getColumn(1).getMembers();
    assertEquals(4, mapA.count());
    assertEquals("b", mapA.get(0).getName());
    assertEquals("d", mapA.get(1).getName());
    assertEquals("e", mapA.get(2).getName());
    assertEquals("g", mapA.get(3).getName());

    RowSetSchema mapF = mapA.getColumn(2).getMembers();
    assertEquals(1, mapF.count());
    assertEquals("f", mapF.get(0).getName());
  }

  @Test
  public void testScalarReaderWriter() {
    testTinyIntRW();
    testSmallIntRW();
    testIntRW();
    testLongRW();
    testFloatRW();
    testDoubleRW();
  }

  private void testTinyIntRW() {
    RowSetSchema schema = RowSetSchema.builder()
        .add("col", MinorType.TINYINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(schema)
        .add(0)
        .add(Byte.MAX_VALUE)
        .add(Byte.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getInt());
    assertTrue(reader.next());
    assertEquals(Byte.MAX_VALUE, reader.column(0).getInt());
    assertTrue(reader.next());
    assertEquals(Byte.MIN_VALUE, reader.column(0).getInt());
    rs.clear();
  }

  private void testSmallIntRW() {
    RowSetSchema schema = RowSetSchema.builder()
        .add("col", MinorType.SMALLINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(schema)
        .add(0)
        .add(Short.MAX_VALUE)
        .add(Short.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getInt());
    assertTrue(reader.next());
    assertEquals(Short.MAX_VALUE, reader.column(0).getInt());
    assertTrue(reader.next());
    assertEquals(Short.MIN_VALUE, reader.column(0).getInt());
    rs.clear();
  }

  private void testIntRW() {
    RowSetSchema schema = RowSetSchema.builder()
        .add("col", MinorType.INT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(schema)
        .add(0)
        .add(Integer.MAX_VALUE)
        .add(Integer.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getInt());
    assertTrue(reader.next());
    assertEquals(Integer.MAX_VALUE, reader.column(0).getInt());
    assertTrue(reader.next());
    assertEquals(Integer.MIN_VALUE, reader.column(0).getInt());
    rs.clear();
  }

  private void testLongRW() {
    RowSetSchema schema = RowSetSchema.builder()
        .add("col", MinorType.BIGINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(schema)
        .add(0L)
        .add(Long.MAX_VALUE)
        .add(Long.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getLong());
    assertTrue(reader.next());
    assertEquals(Long.MAX_VALUE, reader.column(0).getLong());
    assertTrue(reader.next());
    assertEquals(Long.MIN_VALUE, reader.column(0).getLong());
    rs.clear();
  }

  private void testFloatRW() {
    RowSetSchema schema = RowSetSchema.builder()
        .add("col", MinorType.FLOAT4)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(schema)
        .add(0F)
        .add(Float.MAX_VALUE)
        .add(Float.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getDouble(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Float.MAX_VALUE, reader.column(0).getDouble(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Float.MIN_VALUE, reader.column(0).getDouble(), 0.000001);
    rs.clear();
  }

  private void testDoubleRW() {
    RowSetSchema schema = RowSetSchema.builder()
        .add("col", MinorType.FLOAT8)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(schema)
        .add(0D)
        .add(Double.MAX_VALUE)
        .add(Double.MIN_VALUE)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.column(0).getDouble(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Double.MAX_VALUE, reader.column(0).getDouble(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Double.MIN_VALUE, reader.column(0).getDouble(), 0.000001);
    rs.clear();
  }

}
