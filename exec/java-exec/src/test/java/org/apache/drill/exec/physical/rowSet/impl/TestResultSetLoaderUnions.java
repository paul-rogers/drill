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
package org.apache.drill.exec.physical.rowSet.impl;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetReader;
import org.junit.Test;

import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;

import com.google.common.base.Charsets;

public class TestResultSetLoaderUnions extends SubOperatorTest {

  @Test
  public void testUnionBasics() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addUnion("u")
          .addType(MinorType.VARCHAR)
          .addMap()
            .addNullable("a", MinorType.INT)
            .addNullable("b", MinorType.VARCHAR)
            .buildNested()
          .build()
        .buildSchema();

    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader writer = rsLoader.writer();

    // Sanity check of writer structure

    ObjectWriter wo = writer.column(1);
    assertEquals(ObjectType.VARIANT, wo.type());
    VariantWriter vw = wo.variant();

    assertTrue(vw.hasType(MinorType.VARCHAR));
    ObjectWriter strObj = vw.member(MinorType.VARCHAR);
    ScalarWriter strWriter = strObj.scalar();
    assertSame(strWriter, vw.scalar(MinorType.VARCHAR));

    assertTrue(vw.hasType(MinorType.MAP));
    ObjectWriter mapObj = vw.member(MinorType.MAP);
    TupleWriter mWriter = mapObj.tuple();
    assertSame(mWriter, vw.tuple());

    // Write values

    rsLoader.startBatch();
    writer
      .addRow(1, "first")
      .addRow(2, mapValue(20, "fred"))
      .addRow(3, null)
      .addRow(4, mapValue(40, null))
      .addRow(5, "last")
      ;

    // Verify the values.
    // (Relies on the row set level union tests having passed.)

    SingleRowSet expected = fixture.rowSetBuilder(schema)
      .addRow(1, "first")
      .addRow(2, mapValue(20, "fred"))
      .addRow(3, null)
      .addRow(4, mapValue(40, null))
      .addRow(5, "last")
      .build();

    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(rsLoader.harvest()));
  }

  @Test
  public void testUnionAddTypes() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    RowSetLoader writer = rsLoader.writer();

    rsLoader.startBatch();

    // First row, (1, "first"), create types as we go.

    writer.start();
    writer.addColumn(SchemaBuilder.columnSchema("id", MinorType.INT, DataMode.REQUIRED));
    writer.scalar("id").setInt(1);
    writer.addColumn(SchemaBuilder.columnSchema("u", MinorType.UNION, DataMode.OPTIONAL));
    VariantWriter variant = writer.column("u").variant();
    variant.member(MinorType.VARCHAR).scalar().setString("first");
    writer.save();

    // Second row, (2, {20, "fred"}), create types as we go.

    writer.start();
    writer.scalar("id").setInt(2);
    TupleWriter innerMap = variant.member(MinorType.MAP).tuple();
    innerMap.addColumn(SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.OPTIONAL));
    innerMap.scalar("a").setInt(20);
    innerMap.addColumn(SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.OPTIONAL));
    innerMap.scalar("b").setString("fred");
    writer.save();

    // Write remaining rows using convenient methods, using
    // schema defined above.

    writer
      .addRow(3, null)
      .addRow(4, mapValue(40, null))
      .addRow(5, "last")
      ;

    // Verify the values.
    // (Relies on the row set level union tests having passed.)

    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addUnion("u")
          .addType(MinorType.VARCHAR)
          .addMap()
            .addNullable("a", MinorType.INT)
            .addNullable("b", MinorType.VARCHAR)
            .buildNested()
          .build()
        .buildSchema();

    SingleRowSet expected = fixture.rowSetBuilder(schema)
      .addRow(1, "first")
      .addRow(2, mapValue(20, "fred"))
      .addRow(3, null)
      .addRow(4, mapValue(40, null))
      .addRow(5, "last")
      .build();

    new RowSetComparison(expected)
      .verifyAndClearAll(fixture.wrap(rsLoader.harvest()));
  }

  @Test
  public void testUnionOverflow() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addUnion("u")
          .addType(MinorType.INT)
          .addType(MinorType.VARCHAR)
          .build()
        .buildSchema();

    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader writer = rsLoader.writer();

    // Fill the batch with enough data to cause overflow.
    // Fill even rows with a Varchar, odd rows with an int.
    // Data must be large enough to cause overflow before 32K rows
    // (the half that get strings.
    // 16 MB / 32 K = 512 bytes
    // Make a bit bigger to overflow early.

    final int strLength = 600;
    byte value[] = new byte[strLength - 6];
    Arrays.fill(value, (byte) 'X');
    String strValue = new String(value, Charsets.UTF_8);
    int count = 0;

    rsLoader.startBatch();
    while (! writer.isFull()) {
      if (count % 2 == 0) {
        writer.addRow(count, String.format("%s%06d", strValue, count));
      } else {
        writer.addRow(count, count * 10);
      }
      count++;
    }

    // Number of rows should be driven by vector size.
    // Our row count should include the overflow row

    int expectedCount = ValueVector.MAX_BUFFER_SIZE / strLength * 2;
    assertEquals(expectedCount + 1, count);

    // Loader's row count should include only "visible" rows

    assertEquals(expectedCount, writer.rowCount());

    // Total count should include invisible and look-ahead rows.

    assertEquals(expectedCount + 1, rsLoader.totalRowCount());

    // Result should exclude the overflow row

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(expectedCount, result.rowCount());

    // Verify the data.

    RowSetReader reader = result.reader();
    int readCount = 0;
    while (reader.next()) {
      assertEquals(readCount, reader.scalar(0).getInt());
      if (readCount % 2 == 0) {
        assertEquals(String.format("%s%06d", strValue, readCount),
            reader.variant(1).scalar().getString());
      } else {
        assertEquals(readCount * 10, reader.variant(1).scalar().getInt());
      }
      readCount++;
    }
    assertEquals(readCount, result.rowCount());
    result.clear();

    // Write a few more rows to verify the overflow row.

    rsLoader.startBatch();
    for (int i = 0; i < 1000; i++) {
      if (count % 2 == 0) {
        writer.addRow(count, String.format("%s%06d", strValue, count));
      } else {
        writer.addRow(count, count * 10);
      }
      count++;
    }

    result = fixture.wrap(rsLoader.harvest());
    assertEquals(1001, result.rowCount());

    int startCount = readCount;
    reader = result.reader();
    while (reader.next()) {
      assertEquals(readCount, reader.scalar(0).getInt());
      if (readCount % 2 == 0) {
        assertEquals(String.format("%s%06d", strValue, readCount),
            reader.variant(1).scalar().getString());
      } else {
        assertEquals(readCount * 10, reader.variant(1).scalar().getInt());
      }
      readCount++;
    }
    assertEquals(readCount - startCount, result.rowCount());
    result.clear();
  }

}
