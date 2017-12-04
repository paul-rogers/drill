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

import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleNameSpace;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.ArrayReaderImpl;

/**
 * Implementation for the implicit join reader.
 * <p>
 * They key concept here is that we leverage the mechanisms available in
 * the regular, non-join reader, but wire them up in a way that gives us
 * the desired join behavior.
 *
 * <h4>Tables and Tuples</h4>
 *
 * In a normal reader, nested maps are represented in a JSON-like structure
 * that consists of a top-level tuple reader which contains readers for each
 * column. Map columns uses a tuple reader, map arrays use an array reader
 * which contains a tuple reader.
 * <p>
 * Here, the tuple readers along the join path are promoted to top level
 * and are accessible via the <tt>table()</tt> methods. The readers do
 * exactly the same work as in the simple reader, but they are exposed
 * differently.
 *
 * <h4>Column Indexes</h4>
 *
 * In the normal readers, internal "index" classes keep track of the value
 * vector location to read for each record. A top-level index counts off
 * the rows. Nested indexes count off array elements for arrays. In the
 * normal reader, an internal process keeps the indexes in sync as the
 * client steps through rows, and steps though elements within arrays.
 * <p>
 * Here, we essentially reverse the direction of index flow. Instead of
 * the top-level index driving the (start) array position, we iterate over
 * the innermost index, and increment the outer index each time we hit
 * the end of an array. Again, we use exactly the same index classes,
 * we just wire them up differently.
 */

public class ImplicitJoinReaderImpl implements ImplicitJoinReader {

  /**
   * Implementation of an implicit projection table. Provides the information
   * defined by the public interface, but also tracks internal state needed to
   * iterate over the join rows.
   */

  protected static class ImplicitTableImpl implements ImplicitTable {
    protected final int level;
    protected final String name;
    protected ImplicitTableImpl child;
    protected AbstractObjectReader reader;
    protected TupleMetadata schema;
    protected int rowCount;
    protected TupleReader tupleReader;
    protected ImplicitTableIterator iter;

    public ImplicitTableImpl(int level, String name) {
      this.level = level;
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public ObjectReader reader() {
      return reader;
    }

    @Override
    public int size() {
      return rowCount;
    }

    @Override
    public TupleReader tuple() {
      return tupleReader;
    }
  }

  /**
   * The implementation must iterate over tables. The join handles three kinds
   * of tables (top-level, single maps, map arrays). This interface provides a
   * common mechanism for iteration, with implementations handling the differences
   * for each kind of table.
   */

  private interface ImplicitTableIterator {
    ColumnReaderIndex index();

    /**
     * Set up internal reader state for the current row.
     */

    void reposition();

    /**
     * Move to the next position in the (constrained) row set. For top level,
     * this is a row within the batch. For single maps, it iterates over the
     * one and only tuple. For map arrays, it iterates over the elements within
     * a single array instance. Each iterator starts positioned before the
     * first item.
     *
     * @return <true> if another item is available, <false> if not
     */

    boolean next();
  }

  /**
   * Iterator for the root table: the top-level rows in the record batch.
   */

  private static class RootTableIterator implements ImplicitTableIterator {

    private final ImplicitTableImpl table;
    private final ColumnReaderIndex rootIndex;

    public RootTableIterator(ImplicitJoinReaderImpl.ImplicitTableImpl table) {
      this.table = table;
      rootIndex = new DirectRowIndex(table.rowCount);
      table.reader.events().bindIndex(rootIndex);
    }

    @Override
    public boolean next() {
      if (rootIndex.next()) {
        reposition();
        return true;
      }
      return false;
    }

    @Override
    public void reposition() {
      table.reader.events().reposition();
    }

    @Override
    public ColumnReaderIndex index() {
      return rootIndex;
    }
  }

  /**
   * Virtual index that steps over the one-and-only entry in a single nested map.
   * Allows maps to appear on the join path, though there is little value in joining
   * a non-repeated map itself, the repeated map may occur inside a non-repeated
   * map, so we just handle that case.
   */

  public static class TupleTableIterator implements ImplicitTableIterator {

    private final ImplicitTableImpl table;
    private int index;

    public TupleTableIterator(ImplicitTableImpl table, ColumnReaderIndex parentIndex) {
      this.table = table;
      table.reader.events().bindIndex(parentIndex);
    }

    @Override
    public boolean next() {
      return ++index < 1;
    }

    @Override
    public void reposition() {
      table.reader.events().reposition();
      index = -1;
    }

    @Override
    public ColumnReaderIndex index() {
      return null;
    }
  }

  /**
   * Iterator over a repeated map represented as an array reader that contains
   * a tuple reader.
   */

  public static class TupleArrayIterator implements ImplicitTableIterator {

    private final ImplicitTableImpl table;
    protected final ColumnReaderIndex index;
    private final ArrayReader arrayReader;

    public TupleArrayIterator(ImplicitTableImpl table, ColumnReaderIndex parentIndex) {
      this.table = table;
      table.reader.events().bindIndex(parentIndex);
      index = ((ArrayReaderImpl) table.reader.array()).elementIndex();
      arrayReader = table.reader.array();
    }

    @Override
    public boolean next() {
      return arrayReader.next();
    }

    @Override
    public void reposition() {
      table.reader.events().reposition();
    }

    @Override
    public ColumnReaderIndex index() {
      return index;
    }
  }

  private enum ReaderState { START, RUN, EOF }

  private final ImplicitJoinReaderImpl.ImplicitTableImpl tables[];
  private final TupleNameSpace<ImplicitJoinReaderImpl.ImplicitTableImpl> nameSpace = new TupleNameSpace<>();
  private ImplicitJoinReaderImpl.ReaderState state = ReaderState.START;

  public ImplicitJoinReaderImpl(ImplicitJoinReaderImpl.ImplicitTableImpl tables[]) {
    this.tables = tables;
    ColumnReaderIndex parentIndex = null;
    for (int i = 0; i < tables.length; i++) {
      ImplicitJoinReaderImpl.ImplicitTableImpl table = tables[i];
      nameSpace.add(table.name(), table);

      // Create a table iterator as needed for each kind of table.

      if (i == 0) {
        table.tupleReader = table.reader.tuple();
        table.iter = new RootTableIterator(table);
        parentIndex = table.iter.index();
      } else  if (table.reader.type() == ObjectType.ARRAY) {
        table.tupleReader = table.reader.array().tuple();
        table.iter = new TupleArrayIterator(table, parentIndex);
        parentIndex = table.iter.index();
      } else {
        table.tupleReader = table.reader.tuple();
        table.iter = new TupleTableIterator(table, parentIndex);
      }
    }
  }

  @Override
  public boolean next() {
    switch (state) {
    case START:
      state = ReaderState.RUN;
      return moveNext(0);

    case RUN:
      return moveNext(tables.length -1);

    case EOF:
      return false;

    default:
      throw new IllegalStateException("State: " + state);
    }
  }

  /**
   * Move to the next row in the joined result set. The code is short, but
   * the algorithm is subtle.
   * <p>
   * Iteration starts at some level: the top for the first row, the bottom
   * for all other rows. Suppose we start at the bottom.
   * <p>
   * The code moves the iterator for the current level forward. If there is
   * another row, then the lower level (if any) is repositioned at that new
   * location, and the lower level iterator is advanced, as just described.
   * <p>
   * However, if any iterator reaches the end (perhaps because we are iterating
   * over an empty array, or have reached the end of the array), then we have
   * to move up a level and advance the iterator for the next level up, as
   * described above.
   * <p>
   * If we end up running out of levels (the top-level iterator reaches the
   * end), then no more rows are available and the method returns false.
   * <p>
   * However, if we reach the bottom level, and the iterator had more values,
   * then we have successfully found another joined row, and we return true.
   * <p>
   * This algorithm implements an inner join (skip over outer records if no
   * inner records exist.) A variation can be created that implements an outer
   * join, but an enhancement would be needed to indicate that inner rows are
   * null (at the table level, or at the level of each column.) That is left
   * as an exercise for later.
   *
   * @param level the level at which to start the iteration: 0 is the top level
   * (overall record batch), and (table count - 1) is the bottom most level (the
   * innermost detail record)
   *
   * @return <tt>true</tt> if another row is available, <tt>false</tt>
   * otherwise
   */

  private boolean moveNext(int level) {
    while (level >= 0) {
      if (tables[level].iter.next()) {
        if (++level == tables.length) {
          return true;
        }
        tables[level].iter.reposition();
      } else {
        level--;
      }
    }
    state = ReaderState.EOF;
    return false;
  }

  @Override
  public int index() {
    return 0;
    // TODO
    //      return tables[tables.length - 1].index.batchIndex();
  }

  @Override
  public int tableCount() {
    return tables.length;
  }

  @Override
  public int tableIndex(String name) {
    return nameSpace.indexOf(name);
  }

  @Override
  public ImplicitTable tableDef(int level) {
    return nameSpace.get(level);
  }

  @Override
  public ImplicitTable tableDef(String name) {
    return nameSpace.get(name);
  }

  @Override
  public TupleReader table(int level) {
    return tableDef(level).tuple();
  }

  @Override
  public TupleReader table(String name) {
    return tableDef(name).tuple();
  }
}
