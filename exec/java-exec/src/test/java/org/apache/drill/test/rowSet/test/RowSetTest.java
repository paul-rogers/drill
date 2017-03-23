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
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.TupleSchema.RowSetSchema;
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
    BatchSchema batchSchema = RowSetSchema.builder()
        .add("c", MinorType.INT)
        .add("a", MinorType.INT, DataMode.REPEATED)
        .addNullable("b", MinorType.VARCHAR)
        .build();

    assertEquals("c", batchSchema.getColumn(0).getName());
    assertEquals("a", batchSchema.getColumn(1).getName());
    assertEquals("b", batchSchema.getColumn(2).getName());

    RowSetSchema schema = new RowSetSchema(batchSchema);
    assertEquals(3, schema.count());

    crossCheck(schema, 0, "c", MinorType.INT);
    assertEquals(DataMode.REQUIRED, schema.get(0).getDataMode());
    assertEquals(DataMode.REQUIRED, schema.get(0).getType().getMode());
    assertTrue(! schema.get(0).isNullable());

    crossCheck(schema, 1, "a", MinorType.INT);
    assertEquals(DataMode.REPEATED, schema.get(1).getDataMode());
    assertEquals(DataMode.REPEATED, schema.get(1).getType().getMode());
    assertTrue(! schema.get(1).isNullable());

    crossCheck(schema, 2, "b", MinorType.INT);
    assertEquals(MinorType.VARCHAR, schema.get(2).getType().getMinorType());
    assertEquals(DataMode.OPTIONAL, schema.get(2).getDataMode());
    assertEquals(DataMode.OPTIONAL, schema.get(2).getType().getMode());
    assertTrue(schema.get(2).isNullable());
  }

  public void crossCheck(RowSetSchema schema, int index, String name, MinorType type) {
    assertEquals(name, schema.getColumn(index).fullName);
    assertEquals(index, schema.getIndex(name));
    assertEquals(schema.get(index), schema.get(name));
    assertEquals(type, schema.get(index).getType().getMinorType());
  }

  @Test
  public void testMapSchema() {
    BatchSchema batchSchema = RowSetSchema.builder()
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

    RowSetSchema schema = new RowSetSchema(batchSchema);
    assertEquals(8, schema.count());
    crossCheck(schema, 0, "c", MinorType.INT);
    crossCheck(schema, 1, "a", MinorType.MAP);
    assertNotNull(schema.getColumn(1).mapSchema);
    assertEquals(4, schema.getColumn(1).mapSchema.count());
    crossCheck(schema, 2, "a.b", MinorType.VARCHAR);
    crossCheck(schema, 3, "a.d", MinorType.INT);
    crossCheck(schema, 4, "a.e", MinorType.MAP);
    assertNotNull(schema.getColumn(1).mapSchema.getColumn(2).mapSchema);
    assertEquals(1, schema.getColumn(1).mapSchema.getColumn(2).mapSchema.count());
    crossCheck(schema, 5, "a.e.f", MinorType.VARCHAR);
    crossCheck(schema, 6, "a.g", MinorType.INT);
    crossCheck(schema, 7, "h", MinorType.BIGINT);
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
    BatchSchema schema = RowSetSchema.builder()
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
    BatchSchema schema = RowSetSchema.builder()
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
    BatchSchema schema = RowSetSchema.builder()
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
    BatchSchema schema = RowSetSchema.builder()
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
    BatchSchema schema = RowSetSchema.builder()
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
    BatchSchema schema = RowSetSchema.builder()
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

  @Test
  public void testMap() {
    BatchSchema schema = RowSetSchema.builder()
        .add("a", MinorType.INT)
        .addMap("b")
          .add("c", MinorType.INT)
          .add("d", MinorType.INT)
          .buildMap()
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(schema)
        .add(10, 20, 30)
        .add(40, 50, 60)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(10, reader.column(0).getInt());
    assertEquals(20, reader.column(2).getInt());
    assertEquals(30, reader.column(3).getInt());
    assertEquals(10, reader.column("a").getInt());
    assertEquals(30, reader.column("a.d").getInt());
    assertTrue(reader.next());
    assertEquals(50, reader.column(0).getInt());
    assertEquals(60, reader.column(2).getInt());
    assertEquals(60, reader.column(3).getInt());
    rs.clear();
  }

}
