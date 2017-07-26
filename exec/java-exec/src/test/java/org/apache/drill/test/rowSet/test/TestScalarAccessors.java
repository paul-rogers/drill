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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.joda.time.Period;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.junit.Test;

public class TestScalarAccessors extends SubOperatorTest {

  /**
   * Verify that simple scalar (non-repeated) column readers
   * and writers work as expected. This is for tiny ints.
   */

  @Test
  public void testTinyIntRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.TINYINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0)
        .add(Byte.MAX_VALUE)
        .add(Byte.MIN_VALUE)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.INTEGER, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getInt());

    assertTrue(reader.next());
    assertEquals(Byte.MAX_VALUE, colReader.getInt());
    assertEquals((int) Byte.MAX_VALUE, colReader.getObject());
    assertEquals(Byte.toString(Byte.MAX_VALUE), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(Byte.MIN_VALUE, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableTinyInt() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.TINYINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(10)
        .addSingleCol(null)
        .add(30)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(10, colReader.getInt());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());
    // Data value is undefined, may be garbage

    assertTrue(reader.next());
    assertEquals(30, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testSmallIntRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.SMALLINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0)
        .add(Short.MAX_VALUE)
        .add(Short.MIN_VALUE)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.INTEGER, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getInt());

    assertTrue(reader.next());
    assertEquals(Short.MAX_VALUE, colReader.getInt());
    assertEquals((int) Short.MAX_VALUE, colReader.getObject());
    assertEquals(Short.toString(Short.MAX_VALUE), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(Short.MIN_VALUE, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableSmallInt() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.SMALLINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(10)
        .addSingleCol(null)
        .add(30)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(10, colReader.getInt());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());
    // Data value is undefined, may be garbage

    assertTrue(reader.next());
    assertEquals(30, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.INT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0)
        .add(Integer.MAX_VALUE)
        .add(Integer.MIN_VALUE)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.INTEGER, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, reader.scalar(0).getInt());

    assertTrue(reader.next());
    assertEquals(Integer.MAX_VALUE, colReader.getInt());
    assertEquals(Integer.MAX_VALUE, colReader.getObject());
    assertEquals(Integer.toString(Integer.MAX_VALUE), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(Integer.MIN_VALUE, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableInt() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.INT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(10)
        .addSingleCol(null)
        .add(30)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(10, colReader.getInt());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());
    // Data value is undefined, may be garbage

    assertTrue(reader.next());
    assertEquals(30, colReader.getInt());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testLongRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.BIGINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0L)
        .add(Long.MAX_VALUE)
        .add(Long.MIN_VALUE)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.LONG, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getLong());

    assertTrue(reader.next());
    assertEquals(Long.MAX_VALUE, colReader.getLong());
    assertEquals(Long.MAX_VALUE, colReader.getObject());
    assertEquals(Long.toString(Long.MAX_VALUE), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(Long.MIN_VALUE, colReader.getLong());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableLong() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.BIGINT)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(10L)
        .addSingleCol(null)
        .add(30L)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(10, colReader.getLong());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());
    // Data value is undefined, may be garbage

    assertTrue(reader.next());
    assertEquals(30, colReader.getLong());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testFloatRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.FLOAT4)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0F)
        .add(Float.MAX_VALUE)
        .add(Float.MIN_VALUE)
        .add(100F)
        .build();
    assertEquals(4, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.DOUBLE, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertEquals(Float.MAX_VALUE, colReader.getDouble(), 0.000001);
    assertEquals((double) Float.MAX_VALUE, (double) colReader.getObject(), 0.000001);

    assertTrue(reader.next());
    assertEquals(Float.MIN_VALUE, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertEquals(100, colReader.getDouble(), 0.000001);
    assertEquals("100.0", colReader.getAsString());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableFloat() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.FLOAT4)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(10F)
        .addSingleCol(null)
        .add(30F)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(10, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());
    // Data value is undefined, may be garbage

    assertTrue(reader.next());
    assertEquals(30, colReader.getDouble(), 0.000001);

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testDoubleRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.FLOAT8)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(0D)
        .add(Double.MAX_VALUE)
        .add(Double.MIN_VALUE)
        .add(100D)
        .build();
    assertEquals(4, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.DOUBLE, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(0, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertEquals(Double.MAX_VALUE, colReader.getDouble(), 0.000001);
    assertEquals(Double.MAX_VALUE, (double) colReader.getObject(), 0.000001);

    assertTrue(reader.next());
    assertEquals(Double.MIN_VALUE, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertEquals(100, colReader.getDouble(), 0.000001);
    assertEquals("100.0", colReader.getAsString());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableDouble() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.FLOAT4)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(10D)
        .addSingleCol(null)
        .add(30D)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(10, colReader.getDouble(), 0.000001);

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());
    // Data value is undefined, may be garbage

    assertTrue(reader.next());
    assertEquals(30, colReader.getDouble(), 0.000001);

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testStringRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.VARCHAR)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add("")
        .add("abcd")
        .build();
    assertEquals(2, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.STRING, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals("", colReader.getString());

    assertTrue(reader.next());
    assertEquals("abcd", colReader.getString());
    assertEquals("abcd", colReader.getObject());
    assertEquals("\"abcd\"", colReader.getAsString());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableString() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.VARCHAR)
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add("")
        .addSingleCol(null)
        .add("abcd")
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals("", colReader.getString());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());

    assertTrue(reader.next());
    assertEquals("abcd", colReader.getString());
    assertEquals("abcd", colReader.getObject());
    assertEquals("\"abcd\"", colReader.getAsString());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntervalYearRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.INTERVALYEAR)
        .build();

    Period p1 = Period.years(0);
    Period p2 = Period.years(2).plusMonths(3);
    Period p3 = Period.years(1234).plusMonths(11);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(p1)
        .add(p2)
        .add(p3)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(p1, colReader.getPeriod());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod());
    assertEquals(p2, colReader.getObject());
    assertEquals(p2.toString(), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p3, colReader.getPeriod());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableIntervalYear() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.INTERVALYEAR)
        .build();

    Period p1 = Period.years(0);
    Period p2 = Period.years(2).plusMonths(3);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(p1)
        .addSingleCol(null)
        .add(p2)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(p1, colReader.getPeriod());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getPeriod());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testIntervalDayRW() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("col", MinorType.INTERVALDAY)
        .build();

    Period p1 = Period.days(0);
    Period p2 = Period.days(3).plusHours(4).plusMinutes(5).plusSeconds(23);
    Period p3 = Period.days(999).plusHours(23).plusMinutes(59).plusSeconds(59);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(p1)
        .add(p2)
        .add(p3)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    // The normalizedStandard() call is a hack. See DRILL-5689.
    assertEquals(p1, colReader.getPeriod().normalizedStandard());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod().normalizedStandard());
    assertEquals(p2, ((Period) colReader.getObject()).normalizedStandard());
    assertEquals(p2.toString(), colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p3, colReader.getPeriod().normalizedStandard());

    assertFalse(reader.next());
    rs.clear();
  }

  @Test
  public void testNullableIntervalDay() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("col", MinorType.INTERVALDAY)
        .build();

    Period p1 = Period.years(0);
    Period p2 = Period.days(3).plusHours(4).plusMinutes(5).plusSeconds(23);

    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(p1)
        .addSingleCol(null)
        .add(p2)
        .build();
    assertEquals(3, rs.rowCount());

    RowSetReader reader = rs.reader();
    ScalarReader colReader = reader.scalar(0);
    assertEquals(ValueType.PERIOD, colReader.valueType());

    assertTrue(reader.next());
    assertFalse(colReader.isNull());
    assertEquals(p1, colReader.getPeriod().normalizedStandard());

    assertTrue(reader.next());
    assertTrue(colReader.isNull());
    assertNull(colReader.getPeriod());
    assertNull(colReader.getObject());
    assertEquals("null", colReader.getAsString());

    assertTrue(reader.next());
    assertEquals(p2, colReader.getPeriod().normalizedStandard());

    assertFalse(reader.next());
    rs.clear();
  }
}
