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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetWriter;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.bouncycastle.util.Arrays;
import org.junit.Test;

import com.google.common.base.Splitter;

/**
 * Test row sets. Since row sets are a thin wrapper around vectors,
 * readers and writers, this is also a test of those constructs.
 * <p>
 * Tests basic protocol of the writers: <pre><code>
 * row : tuple
 * tuple : column *
 * column : scalar obj | array obj | tuple obj
 * scalar obj : scalar
 * arary obj : array writer
 * array writer : element
 * element : column
 * tuple obj : tuple</code></pre>
 */

public class RowSetTest extends SubOperatorTest {

  /**
   * Test the simplest constructs: a row with top-level scalar
   * columns.
   * <p>
   * The focus here is the structure of the readers and writers, along
   * with the row set loader and verifier that use those constructs.
   * That is, while this test uses the int vector, this test is not
   * focused on that vector.
   *
   * @throws VectorOverflowException should never occur
   */

  @Test
  public void testScalarStructure() throws VectorOverflowException {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer();

    // Required Int
    // Verify the invariants of the "full" and "simple" access paths

    assertEquals(ObjectType.SCALAR, writer.column("a").type());
    assertSame(writer.column("a"), writer.column(0));
    assertSame(writer.scalar("a"), writer.scalar(0));
    assertSame(writer.column("a").scalar(), writer.scalar("a"));
    assertSame(writer.column(0).scalar(), writer.scalar(0));
    assertEquals(ValueType.INTEGER, writer.scalar(0).valueType());

    // Test the various ways to get at the scalar writer.

    writer.column("a").scalar().setInt(10);
    writer.save();
    writer.scalar("a").setInt(20);
    writer.save();
    writer.column(0).scalar().setInt(30);
    writer.save();
    writer.scalar(0).setInt(40);
    writer.save();

    // Sanity checks

    try {
      writer.column(0).array();
      fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      writer.column(0).tuple();
      fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }

    // Finish the row set and get a reader.

    SingleRowSet actual = writer.done();
    RowSetReader reader = actual.reader();

    // Verify invariants

    assertEquals(ObjectType.SCALAR, reader.column(0).type());
    assertSame(reader.column("a"), reader.column(0));
    assertSame(reader.scalar("a"), reader.scalar(0));
    assertSame(reader.column("a").scalar(), reader.scalar("a"));
    assertSame(reader.column(0).scalar(), reader.scalar(0));
    assertEquals(ValueType.INTEGER, reader.scalar(0).valueType());

    // Test various accessors: full and simple

    assertTrue(reader.next());
    assertEquals(10, reader.column("a").scalar().getInt());
    assertTrue(reader.next());
    assertEquals(20, reader.scalar("a").getInt());
    assertTrue(reader.next());
    assertEquals(30, reader.column(0).scalar().getInt());
    assertTrue(reader.next());
    assertEquals(40, reader.scalar(0).getInt());
    assertFalse(reader.next());

    // Test the above again via the writer and reader
    // utility classes.

    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .add(10)
        .add(20)
        .add(30)
        .add(40)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  /**
   * Test a record with a top level array. The focus here is on the
   * scalar array structure.
   *
   * @throws VectorOverflowException should never occur
   */

  @Test
  public void testScalarArrayStructure() throws VectorOverflowException {
    TupleMetadata schema = new SchemaBuilder()
        .addArray("a", MinorType.INT)
        .buildSchema();
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer();

    // Repeated Int
    // Verify the invariants of the "full" and "simple" access paths

    assertEquals(ObjectType.ARRAY, writer.column("a").type());

    assertSame(writer.column("a"), writer.column(0));
    assertSame(writer.array("a"), writer.array(0));
    assertSame(writer.column("a").array(), writer.array("a"));
    assertSame(writer.column(0).array(), writer.array(0));

    assertEquals(ObjectType.SCALAR, writer.column("a").array().entry().type());
    assertEquals(ObjectType.SCALAR, writer.column("a").array().entryType());
    assertSame(writer.array(0).entry().scalar(), writer.array(0).scalar());
    assertEquals(ValueType.INTEGER, writer.array(0).scalar().valueType());

    // Write some data

    ScalarWriter intWriter = writer.array("a").scalar();
    intWriter.setInt(10);
    intWriter.setInt(11);
    writer.save();
    intWriter.setInt(20);
    intWriter.setInt(21);
    intWriter.setInt(22);
    writer.save();
    intWriter.setInt(30);
    writer.save();
    intWriter.setInt(40);
    intWriter.setInt(41);
    writer.save();

    // Sanity checks

    try {
      writer.column(0).scalar();
      fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      writer.column(0).tuple();
      fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }

    // Finish the row set and get a reader.

    SingleRowSet actual = writer.done();
    RowSetReader reader = actual.reader();

    // Verify the invariants of the "full" and "simple" access paths

    assertEquals(ObjectType.ARRAY, writer.column("a").type());

    assertSame(reader.column("a"), reader.column(0));
    assertSame(reader.array("a"), reader.array(0));
    assertSame(reader.column("a").array(), reader.array("a"));
    assertSame(reader.column(0).array(), reader.array(0));

    assertEquals(ObjectType.SCALAR, reader.column("a").array().entryType());
    assertEquals(ValueType.INTEGER, reader.array(0).elements().valueType());

    // Read and verify the rows

    ScalarElementReader intReader = reader.array(0).elements();
    assertTrue(reader.next());
    assertEquals(2, intReader.size());
    assertEquals(10, intReader.getInt(0));
    assertEquals(11, intReader.getInt(1));
    assertTrue(reader.next());
    assertEquals(3, intReader.size());
    assertEquals(20, intReader.getInt(0));
    assertEquals(21, intReader.getInt(1));
    assertEquals(22, intReader.getInt(2));
    assertTrue(reader.next());
    assertEquals(1, intReader.size());
    assertEquals(30, intReader.getInt(0));
    assertTrue(reader.next());
    assertEquals(2, intReader.size());
    assertEquals(40, intReader.getInt(0));
    assertEquals(41, intReader.getInt(1));
    assertFalse(reader.next());

    // Test the above again via the writer and reader
    // utility classes.

    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addSingleCol(new int[] {10, 11})
        .addSingleCol(new int[] {20, 21, 22})
        .addSingleCol(new int[] {30})
        .addSingleCol(new int[] {40, 41})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  /**
   * Test a simple map structure at the top level of a row.
   *
   * @throws VectorOverflowException should never occur
   */

  @Test
  public void testMapStructure() throws VectorOverflowException {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("m")
          .addArray("b", MinorType.INT)
          .buildMap()
        .buildSchema();
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer();

    // Map and Int
    // Test Invariants

    assertEquals(ObjectType.SCALAR, writer.column("a").type());
    assertEquals(ObjectType.SCALAR, writer.column(0).type());
    assertEquals(ObjectType.TUPLE, writer.column("m").type());
    assertEquals(ObjectType.TUPLE, writer.column(1).type());
    assertSame(writer.column(1).tuple(), writer.tuple(1));

    TupleWriter mapWriter = writer.column(1).tuple();
    assertEquals(ObjectType.SCALAR, mapWriter.column("b").array().entry().type());
    assertEquals(ObjectType.SCALAR, mapWriter.column("b").array().entryType());

    ScalarWriter aWriter = writer.column("a").scalar();
    ScalarWriter bWriter = writer.column("m").tuple().column("b").array().entry().scalar();
    assertSame(bWriter, writer.tuple(1).array(0).scalar());
    assertEquals(ValueType.INTEGER, bWriter.valueType());

    // Write data

    aWriter.setInt(10);
    bWriter.setInt(11);
    bWriter.setInt(12);
    writer.save();
    aWriter.setInt(20);
    bWriter.setInt(21);
    bWriter.setInt(22);
    writer.save();
    aWriter.setInt(30);
    bWriter.setInt(31);
    bWriter.setInt(32);
    writer.save();

    // Sanity checks

    try {
      writer.column(1).scalar();
      fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      writer.column(1).array();
      fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }

    // Finish the row set and get a reader.

    SingleRowSet actual = writer.done();
    RowSetReader reader = actual.reader();

    assertEquals(ObjectType.SCALAR, reader.column("a").type());
    assertEquals(ObjectType.SCALAR, reader.column(0).type());
    assertEquals(ObjectType.TUPLE, reader.column("m").type());
    assertEquals(ObjectType.TUPLE, reader.column(1).type());
    assertSame(reader.column(1).tuple(), reader.tuple(1));

    ScalarReader aReader = reader.column(0).scalar();
    TupleReader mReader = reader.column(1).tuple();
    assertEquals(ObjectType.SCALAR, mReader.column("b").array().entryType());
    ScalarElementReader bReader = mReader.column(0).elements();
    assertEquals(ValueType.INTEGER, bReader.valueType());

    assertTrue(reader.next());
    assertEquals(10, aReader.getInt());
    assertEquals(11, bReader.getInt(0));
    assertEquals(12, bReader.getInt(1));
    assertTrue(reader.next());
    assertEquals(20, aReader.getInt());
    assertEquals(21, bReader.getInt(0));
    assertEquals(22, bReader.getInt(1));
    assertTrue(reader.next());
    assertEquals(30, aReader.getInt());
    assertEquals(31, bReader.getInt(0));
    assertEquals(32, bReader.getInt(1));
    assertFalse(reader.next());

    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .add(10, new int[] {11, 12})
        .add(20, new int[] {21, 22})
        .add(30, new int[] {31, 32})
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(actual);
  }

  @Test
  public void testStructure() {
    BatchSchema schema = new SchemaBuilder()
        .add("scalar", MinorType.INT)
        .addArray("array", MinorType.INT)
        .addMap("map")
          .add("c1", MinorType.INT)
          .addArray("c2", MinorType.INT)
          .buildMap()
        .addMapArray("mapList")
          .add("d1", MinorType.INT)
          .addArray("d2", MinorType.INT)
          .buildMap()
        .build();

    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter resultWriter = rowSet.writer();

    ArrayWriter batchWriter = resultWriter.rows();
    batchWriter.next();

    TupleWriter rowWriter = batchWriter.tuple();

    // Required Int

    rowWriter.scalar("scalar").setInt(10);

    // Repeated Int

    ArrayWriter arrayWriter = rowWriter.column("array").array();
    ScalarWriter bValue = arrayWriter.element().scalar();
    bArray.next(); // Optional
    bValue.setInt(10);
    bArray.save(); // Optional
    bArray.next(); // Optional
    bValue.setInt(20);
    bArray.save(); // Optional

    // Repeated int, abbreviated

    bValue = rowWriter.array("b").scalar();
    bValue.setInt(30);
    bValue.setInt(40);

    // Map

    TupleWriter mapWriter = rowWriter.column("map").map();
    mapWriter.column("c1").scalar().setInt(100);
    mapWriter.column("c2").array().setArray(new int[] {100, 200});

    // Repeated map

    ArrayWriter mapListWriter = rowWriter.column("d").array();
    TupleWriter mapElementWriter = mapListWriter.element().tuple();
    mapListWriter.next();
    mapElementWriter.column("d1").scalar().setInt(300);
    mapElementWriter.column("d1").array().setArray(new int[] {111, 211});
    mapListWriter.next();
    mapElementWriter.column("d1").scalar().setInt(400);
    mapElementWriter.column("d1").array().setArray(new int[] {121, 221});

    // List of repeated map

    ArrayWriter eOuter = rowWriter.array("e");
    ArrayWriter eInner = eOuter.array();
    TupleWriter eMap = eInner.tuple();
    eOuter.next();
    eInner.next();
    eMap.scalar("e1").setInt(400);


    batchWriter.save();

    resultWriter.done();
    // Get row set, or whatever
  }

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
    assertTrue(reader.next());
    assertEquals(0, reader.scalar(0).getInt());
    assertTrue(reader.next());
    assertEquals(Byte.MAX_VALUE, reader.scalar(0).getInt());
    assertEquals((int) Byte.MAX_VALUE, reader.scalar(0).getObject());
    assertTrue(reader.next());
    assertEquals(Byte.MIN_VALUE, reader.scalar(0).getInt());
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
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.scalar(0).getInt());
    assertTrue(reader.next());
    assertEquals(Short.MAX_VALUE, reader.scalar(0).getInt());
    assertEquals((int) Short.MAX_VALUE, reader.scalar(0).getObject());
    assertTrue(reader.next());
    assertEquals(Short.MIN_VALUE, reader.scalar(0).getInt());
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
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.scalar(0).getInt());
    assertTrue(reader.next());
    assertEquals(Integer.MAX_VALUE, reader.scalar(0).getInt());
    assertEquals(Integer.MAX_VALUE, reader.scalar(0).getObject());
    assertTrue(reader.next());
    assertEquals(Integer.MIN_VALUE, reader.scalar(0).getInt());
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
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.scalar(0).getLong());
    assertTrue(reader.next());
    assertEquals(Long.MAX_VALUE, reader.scalar(0).getLong());
    assertEquals(Long.MAX_VALUE, reader.scalar(0).getObject());
    assertTrue(reader.next());
    assertEquals(Long.MIN_VALUE, reader.scalar(0).getLong());
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
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.scalar(0).getDouble(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Float.MAX_VALUE, reader.scalar(0).getDouble(), 0.000001);
    assertEquals((double) Float.MAX_VALUE, (double) reader.scalar(0).getObject(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Float.MIN_VALUE, reader.scalar(0).getDouble(), 0.000001);
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
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(0, reader.scalar(0).getDouble(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Double.MAX_VALUE, reader.scalar(0).getDouble(), 0.000001);
    assertEquals(Double.MAX_VALUE, (double) reader.scalar(0).getObject(), 0.000001);
    assertTrue(reader.next());
    assertEquals(Double.MIN_VALUE, reader.scalar(0).getDouble(), 0.000001);
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
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals("", reader.scalar(0).getString());
    assertTrue(reader.next());
    assertEquals("abcd", reader.scalar(0).getString());
    assertEquals("abcd", reader.scalar(0).getObject());
    assertFalse(reader.next());
    rs.clear();
  }

  /**
   * Test writing to and reading from a row set with nested maps.
   * Map fields are flattened into a logical schema.
   */

  @Test
  // TODO: This relies on a flat view which is no longer valid.
  public void testMap() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMap("b")
          .add("c", MinorType.INT)
          .add("d", MinorType.INT)
          .buildMap()
        .build();
    SingleRowSet rs = fixture.rowSetBuilder(batchSchema)
        .add(10, 20, 30)
        .add(40, 50, 60)
        .build();
    RowSetReader reader = rs.reader();
    assertTrue(reader.next());
    assertEquals(10, reader.scalar(0).getInt());
    assertEquals(20, reader.scalar(1).getInt());
    assertEquals(30, reader.scalar(2).getInt());
    assertEquals(10, reader.scalar("a").getInt());
    assertEquals(30, reader.scalar("b.d").getInt());
    assertTrue(reader.next());
    assertEquals(40, reader.scalar(0).getInt());
    assertEquals(50, reader.scalar(1).getInt());
    assertEquals(60, reader.scalar(2).getInt());
    assertFalse(reader.next());
    rs.clear();
  }

  /**
   * Test an array of ints (as an example fixed-width type)
   * at the top level of a schema.
   */

  @Test
  public void TestTopFixedWidthArray() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .addArray("a", MinorType.INT)
        .build();

    ExtendableRowSet rs1 = fixture.rowSet(batchSchema);
    RowSetWriter writer = rs1.writer();
    try {
      writer.scalar(0).setInt(10);
      ScalarWriter array = writer.array(1).scalar();
      array.setInt(100);
      array.setInt(110);
      writer.save();
      writer.scalar(0).setInt(20);
      array.setInt(200);
      array.setInt(120);
      array.setInt(220);
      writer.save();
      writer.scalar(0).setInt(30);
      writer.save();
    } catch (VectorOverflowException e) {
      fail("Should not overflow vector");
    }
    SingleRowSet result = writer.done();

    RowSetReader reader = result.reader();
    assertTrue(reader.next());
    assertEquals(10, reader.scalar(0).getInt());
    ScalarElementReader arrayReader = reader.array(1).elements();
    assertEquals(2, arrayReader.size());
    assertEquals(100, arrayReader.getInt(0));
    assertEquals(110, arrayReader.getInt(1));
    assertTrue(reader.next());
    assertEquals(20, reader.scalar(0).getInt());
    assertEquals(3, arrayReader.size());
    assertEquals(200, arrayReader.getInt(0));
    assertEquals(120, arrayReader.getInt(1));
    assertEquals(220, arrayReader.getInt(2));
    assertTrue(reader.next());
    assertEquals(30, reader.scalar(0).getInt());
    assertEquals(0, arrayReader.size());
    assertFalse(reader.next());

    SingleRowSet rs2 = fixture.rowSetBuilder(batchSchema)
      .add(10, new int[] {100, 110})
      .add(20, new int[] {200, 120, 220})
      .add(30, null)
      .build();

    new RowSetComparison(rs1)
      .verifyAndClearAll(rs2);
  }

  /**
   * Test filling a row set up to the maximum number of rows.
   * Values are small enough to prevent filling to the
   * maximum buffer size.
   */

  @Test
  public void testRowBounds() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .build();

    ExtendableRowSet rs = fixture.rowSet(batchSchema);
    RowSetWriter writer = rs.writer();
    int count = 0;
    boolean lastSave = true;
    while (! writer.isFull()) {
      assertTrue(lastSave);
      try {
        writer.scalar(0).setInt(count++);
      } catch (VectorOverflowException e) {
        fail("Int vector should not overflow");
      }
      lastSave = writer.save();
    }
    assertFalse(lastSave);
    writer.done();

    assertEquals(ValueVector.MAX_ROW_COUNT, count);
    // The writer index points past the writable area.
    // But, this is fine, the valid() method says we can't
    // write at this location.
    assertEquals(ValueVector.MAX_ROW_COUNT, writer.rowIndex());
    assertEquals(ValueVector.MAX_ROW_COUNT, rs.rowCount());
    rs.clear();
  }

  /**
   * Test filling a row set up to the maximum vector size.
   * Values in the first column are small enough to prevent filling to the
   * maximum buffer size, but values in the second column
   * will reach maximum buffer size before maximum row size.
   * The result should be the number of rows that fit, with the
   * partial last row not counting. (A complete application would
   * reload the partial row into a new row set.)
   */

  @Test
  public void testbufferBounds() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .build();

    String varCharValue;
    try {
      byte rawValue[] = new byte[512];
      Arrays.fill(rawValue, (byte) 'X');
      varCharValue = new String(rawValue, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }

    ExtendableRowSet rs = fixture.rowSet(batchSchema);
    RowSetWriter writer = rs.writer();
    int count = 0;
    for (;;) {
      try {
        writer.scalar(0).setInt(count++);
      } catch (VectorOverflowException e) {
        fail("Int vector should not overflow");
      }
      try {
        writer.scalar(1).setString(varCharValue);
      } catch (VectorOverflowException e) {
        break;
      }
      assertTrue(writer.save());
    }
    writer.done();

    assertTrue(count < ValueVector.MAX_ROW_COUNT);
    assertEquals(count, writer.rowIndex());
    assertEquals(count, rs.rowCount());
    rs.clear();
  }
}
