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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.test.SubOperatorTest;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.Test;

public class TestResultSetLoaderUnions extends SubOperatorTest {

  @Test
  public void testBasics() {
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

    ObjectWriter wo = writer.column(0);
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
  public void testAddTypes() {
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
  public void testOverflow() {
    fail("Not yet implemented");
  }

}
