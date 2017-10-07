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

import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.TupleReader;

/**
 * Provides an implicit join reader. An implicit join occurs when a data set contains
 * map arrays and we wish to iterate over the records implied by one of the arrays.
 * In this case, we have a top-level table which is the overall result set. We can
 * then identify a path of tuples and/or tuple arrays to identify the iteration
 * target, which must be a map array. The path identifies a set of levels.
 * <p>
 * Consider an example: a result set consisting of customer records. Each contains
 * order records, each of which contains items. For example:
 * <table>
 * <tr><th colspan=2>Customer</th>
 *     <th colspan=2>Order</th>
 *     <th colspan=2>Item</th></tr>
 * <tr><th>Cust ID</th><th>Cust Name</th>
 *     <th>Order ID</th><th>Status</th>
 *     <th>Descrip</th><th>Price</th></th>
 * <tr><td rowspan=4>1</td><td rowspan=4>Fred</td>
 *     <td rowspan=2>1001</td><td rowspan=2>Filled</td>
 *     <td>Drill</td><td>30.00</td></tr>
 * <tr><td>Hammer</td><td>5.00</td></tr>
 * <tr><td rowspan=2>1007</td><td rowspan=2>Backorder</td>
 *     <td>Auger</td><td>6.00</td></tr>
 * <tr><td>Planer</td><td>54.00</td></tr>
 * <tr><td>2</td><td>Barney</td>
 *     <td></td><td></td><td></td></tr>
 * </table>
 *
 * In the above, Fred has two orders, each of which has two items. The table
 * format hints that these form a tree. Customer is at level 0, Order at level 1,
 * and Item at level 2. Note that Barney is a customer, but has not placed any
 * orders, and so there are no nested documents (the Order array for Barney
 * is empty.)
 * <p>
 * If we iterated over the table itself, we'd iterate over the two customers:
 * Fred and Barney. If we iterate over orders, we'd get two orders. If we iterate
 * over items, we'd get four items.
 * <p>
 * As with any join, we can ask what to do with empty entries. An "inner" implicit
 * join skips outer records if there are no inner records (just as an inner SQL join
 * omits records from one side that don't match records on the other side.) But, if
 * we do an "outer" join, we will include outer records even if there are no inner
 * records, with the inner values being null. (Note that the inner values will be
 * null even if the actual columns are defined as non-nullable.)
 *
 * <h4>Top-Level Tables</h4>
 *
 * In normal usage, each Drill batch represents a single table. In the example above,
 * a table of customers, which contains an array of orders. When using the implicit
 * join view, we must think of multiple top-level tables. In the above example we
 * have a <tt>`customer`</tt> table and an <tt>`orders`</tt> table.
 * <p>
 * The table concept is essential as the same column name may be used in both.
 * For example, we might have used <tt>`customer.id`</tt> and
 * <tt>`orders.id`</tt> above rather than <tt>Cust ID</tt> and <tt>Order ID</tt>.
 * When the data is in its original form, the customer <tt>id</tt> appears in the
 * top-level row (tuple), while the order <tt>id</tt> field appears in the
 * orders map (tuple). By preserving this table concept in the implicit join
 * reader, we keep the tuple name spaces distinct, avoiding naming conflicts.
 * <p>
 * Each table contains all the fields from the original, unjoined structure
 * <i>except</i> those that have been promoted to top level in the joined
 * result. For example, in the joined result, the <tt>`customer`</tt> table will
 * contain the <tt>Cust ID</tt> and <tt>Cust Name</tt> fields, but not the
 * <tt>`orders`</tt> tuple array since that has been promoted to top level in
 * the joined result.
 *
 * <h4>Nested Tuples and Tuple Arrays</h4>
 *
 * To expand on the above concept a bit, a Drill table can be used in an implicit
 * join for any path through the table that starts at the top level and names
 * tuples (AKA "maps") or tuple arrays (AKA "repeated maps.").
 * <p>
 * A single tuple is projected as a top-level table. A tuple array is also
 * projected as a top-level table; the array portion is automatically iterated
 * over to implement the join. For example, in the above example, the
 * <tt>`orders`</tt> array gives rise to the <tt>`orders`</tt> table in the join;
 * the join reader will iterate over each element in the array to produce the
 * joined result.
 * <p>
 * Note that a record may contain multiple nested documents (nested tuples.)
 * For example, the customer record might contain not just the list of orders,
 * but also the list of support calls. In this case, the join reader can expand
 * only one of these paths; the other path is left as a map or repeated map.
 * For example, in the above join, if the customer record also contains support
 * calls, then the list of support calls would appear on every joined record.
 * This may even occasionally be useful.
 *
 * <h4>Example</h4>
 *
 * Here is a simple example:<pre><code>
 * ImplicitJoinReader reader =
 *     new JoinReaderBuilder(
 *         myContainer,
 *         Lists.newArrayList("customer", "order"))
 *     .build();
 * printSchema(reader);
 * while (reader.next()) {
 *   printCol(reader.table("customer").scalar("custId").getInt());
 *   printCol(reader.table("customer").scalar("name").getString());
 *   printCol(reader.table("order").scalar("orderId").getInt());
 *   printCol(reader.table("order").scalar("amount").getDouble());
 *   endRow();
 * }
 * </code></pre>
 * <tt>printCol()</tt> and <tt>endRow()</tt> are just formatting functions.
 * The above produces output like the following:<pre><code>
 * customer(custId, name), order(orderId, amount)
 * 1, fred, 1001, 123.45
 * 1, fred, 1007, 234.65
 * 3, wilma, 1007, 321.65
 * </code></pre>
 */

public interface ImplicitJoinReader {

  /**
   * Metadata information available about each implicit table in
   * the joined results.
   */

  public interface ImplicitTable {
    String name();
    ObjectReader reader();
    TupleReader tuple();
    int size();
  }

  /**
   * Advance to the next row in the joined table. The reader follows the
   * JDBC convention of starting before the first row.
   *
   * @return <tt>true</tt> if another row is available, <tt>false</tt>
   * if EOF is reached
   */

  boolean next();
  int index();

  /**
   * Return the number of tables (levels) in the iterator.
   *
   * @return the table count which will be at least 2 for this
   * reader
   */

  int tableCount();

  /**
   * Return the name of the table at the given level. The name is a constant for
   * the top level, and comes from the name of a map for inner tables.
   *
   * @param level a table level
   * @return name of the table at that level
   */

  int tableIndex(String name);

  /**
   * Return a tuple reader to access the columns within the table
   * identified by index.
   *
   * @param table identified by index. Table 0 is the top-level
   * table, table 1 the first table in the join path, etc.
   * @return level tuple reader that accesses each field in the
   * join table
   */

  TupleReader table(int level);

  /**
   * Return a tuple reader to access the columns within the table
   * identified by name.
   *
   * @param name name of the target table, using the names provided
   * when building the join, including the implicit name for the
   * top-level table
   * @return level tuple reader that accesses each field in the
   * join table
   */

  TupleReader table(String name);
  ImplicitJoinReader.ImplicitTable tableDef(int level);
  ImplicitJoinReader.ImplicitTable tableDef(String name);
}
