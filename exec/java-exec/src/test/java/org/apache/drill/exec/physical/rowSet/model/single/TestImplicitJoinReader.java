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
package org.apache.drill.exec.physical.rowSet.model.single;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetPrinter;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;

import com.google.common.collect.Lists;

import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

/**
 * Tests for a prototype version of an implicit join reader. Suppose
 * we have a table of customers, and for each we have an array or orders.
 * The normal readers can iterate over the rows (the customers.) The implicit
 * join reader can iterate over the orders as if they were the top-level table.
 * Each order is then implicitly joined to its enclosing customer. The result
 * is a reader that provides access to both the customer and order columns
 * as if the tables had been joined.
 * <p>
 * The join is specified by providing an access path from the row down through
 * a collection of one or more maps or map arrays. The row is given a made-up
 * name. For our example, the access path is <tt>["customer", "orders"]</tt>.
 * <p>
 * The reader is structured as a set of maps: one for each entry in the access
 * path. The first map column represents the top-level row, minus the orders
 * map. The second level is the orders map, minus its child, if any. And so on.
 * The maps are needed because the maps may have common column names, say an
 * "id" field for both customers and orders. Keeping the fields in maps keeps
 * things tidy and avoids name conflicts.
 * <p>
 * This works because of the structure of Drill value vectors. Don't try
 * this with row-structured data!
 * <p>
 * This version does not handle hyper-vectors.
 */

public class TestImplicitJoinReader extends SubOperatorTest {

  @Test
  public void basicTest() {
    TupleMetadata schema = new SchemaBuilder()
        .add("custId", MinorType.INT)
        .add("name", MinorType.VARCHAR)
        .addMapArray("order")
          .add("orderId", MinorType.INT)
          .add("amount", MinorType.FLOAT8)
          .buildMap()
        .buildSchema();

    SingleRowSet rowSet = new RowSetBuilder(fixture.allocator(), schema)
        .addRow(1, "fred", mapArray(
            mapValue(10, 100.0),
            mapValue(11, 110.0),
            mapValue(12, 120.0)))
        .addRow(2, "barney", mapArray())
        .addRow(3, "wilma", mapArray(
            mapValue(20, 200.0),
            mapValue(21, 210.0)))
        .build();

    {
      RowSetReader reader = rowSet.reader();
      printSchema(reader);
      while (reader.next()) {
        printRow(reader.logicalIndex(), reader);
      }
    }

    {
      ImplicitJoinReader reader =
          new JoinReaderBuilder(
              rowSet.container(),
              Lists.newArrayList("customer", "order"))
          .build();
      printSchema(reader);
      while (reader.next()) {
        printRow(reader.index(), reader);
      }
    }

    rowSet.clear();
  }

  @Test
  public void demoTest() {
    TupleMetadata schema = new SchemaBuilder()
        .add("custId", MinorType.INT)
        .add("name", MinorType.VARCHAR)
        .addMapArray("order")
          .add("orderId", MinorType.INT)
          .add("amount", MinorType.FLOAT8)
          .buildMap()
        .buildSchema();

    SingleRowSet rowSet = new RowSetBuilder(fixture.allocator(), schema)
        .addRow(1, "fred", mapArray(
            mapValue(1001, 123.45),
            mapValue(1007, 234.65)))
        .addRow(2, "barney", mapArray())
        .addRow(3, "wilma", mapArray(
            mapValue(1007, 321.65)))
        .build();

    {
      RowSetReader reader = rowSet.reader();
      printSchema(reader);
      while (reader.next()) {
        printRow(reader.logicalIndex(), reader);
      }
    }

    {
      ImplicitJoinReader reader =
          new JoinReaderBuilder(
              rowSet.container(),
              Lists.newArrayList("customer", "order"))
          .build();
      printSchema(reader);
      while (reader.next()) {
        printRow(reader.index(), reader);
      }
    }

    {
      ImplicitJoinReader reader =
          new JoinReaderBuilder(
              rowSet.container(),
              Lists.newArrayList("customer", "order"))
          .build();
      printSchema(reader);
      while (reader.next()) {

        // This is a lame form of testing -- but this is a prototype.
        // A better evolution would be for the reader to access tables
        // as maps, so reader.tuple("customer") for the top-level row,
        // and so on.

        printCol(reader.table("customer").scalar("custId").getInt());
        printCol(reader.table("customer").scalar("name").getString());
        printCol(reader.table("order").scalar("orderId").getInt());
        printCol(reader.table("order").scalar("amount").getDouble());
        endRow();
      }
    }

    {
      ImplicitJoinReader reader =
          new JoinReaderBuilder(
              rowSet.container(),
              Lists.newArrayList("customer", "order"))
          .build();
      printSchema(reader);
      while (reader.next()) {
        printCol(reader.table(0).scalar(0).getInt());
        printCol(reader.table(0).scalar(1).getString());
        printCol(reader.table(1).scalar(0).getInt());
        printCol(reader.table(1).scalar(1).getDouble());
        endRow();
      }
    }

    {
      ImplicitJoinReader reader =
          new JoinReaderBuilder(
              rowSet.container(),
              Lists.newArrayList("customer", "order"))
          .build();
      printSchema(reader);

      // Cache all the readers

      TupleReader customer = reader.table("customer");
      ScalarReader custId = customer.scalar("custId");
      ScalarReader name = customer.scalar("name");
      TupleReader order = reader.table("order");
      ScalarReader orderId = order.scalar("orderId");
      ScalarReader amount = order.scalar("amount");

      while (reader.next()) {
        printCol(custId.getInt());
        printCol(name.getString());
        printCol(orderId.getInt());
        printCol(amount.getDouble());
        endRow();
      }
    }

    rowSet.clear();
  }


  int colCount;

  public void printCol(Object value) {
    if (colCount > 0) {
      System.out.print(", ");
    }
    colCount++;
    System.out.print(value);
  }

  public void endRow() {
    colCount = 0;
    System.out.println();
  }

  private void printSchema(ImplicitJoinReader reader) {
    for (int i = 0; i < reader.tableCount(); i++) {
      if (i > 0) {
        System.out.print(", ");
      }
      System.out.print(reader.tableDef(i).name());
      System.out.print("(");
      RowSetPrinter.printTupleSchema(System.out, reader.tableDef(i).tuple().schema());
      System.out.print(")");
    }
    System.out.println();
  }

  private void printSchema(RowSetReader reader) {
    RowSetPrinter.printTupleSchema(System.out, reader.schema());
    System.out.println();
  }

  private void printRow(int rowNo, TupleReader reader) {
    System.out.format("%4d", rowNo);
    printTuple(reader);
    System.out.println();
  }

  private void printRow(int rowNo, ImplicitJoinReader reader) {
    System.out.format("%4d", rowNo);
    for (int i = 0; i < reader.tableCount(); i++) {
      printTuple(reader.tableDef(i).tuple());
    }
    System.out.println();
  }

  private void printTuple(TupleReader reader) {
    for (int i = 0; i < reader.columnCount(); i++) {
      System.out.print(", ");
      System.out.print(reader.column(i).getAsString());
    }
  }

}
