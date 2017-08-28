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
package org.apache.drill.exec.vector.accessor.writer;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

/**
 * Writer for a Drill Map type. Maps are actually tuples, just like rows.
 */

public abstract class MapWriter extends AbstractTupleWriter {

  /**
   * Wrap the outer index to avoid incrementing the array index
   * on the call to <tt>nextElement().</tt> For maps, the increment
   * is done at the map level, not the column level.
   */

  private static class MemberWriterIndex implements ColumnWriterIndex {
    private ColumnWriterIndex baseIndex;

    private MemberWriterIndex(ColumnWriterIndex baseIndex) {
      this.baseIndex = baseIndex;
    }

    @Override public int rowStartIndex() { return baseIndex.rowStartIndex(); }
    @Override public int vectorIndex() { return baseIndex.vectorIndex(); }
    @Override public void nextElement() { }
    @Override public void rollover() { }
    @Override public ColumnWriterIndex outerIndex() {
      return baseIndex.outerIndex();
    }

    @Override
    public String toString() {
      return new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" baseIndex = ")
        .append(baseIndex.toString())
        .append("]")
        .toString();
    }
  }

  /**
   * Writer for a single (non-array) map. Clients don't really "write" maps;
   * rather, this writer is a holder for the columns within the map, and those
   * columns are what is written.
   */

  private static class SingleMapWriter extends MapWriter {
    private final MapVector mapVector;

    private SingleMapWriter(ColumnMetadata schema, MapVector vector, List<AbstractObjectWriter> writers) {
      super(schema, writers);
      mapVector = vector;
    }

    @Override
    public void endWrite() {
      super.endWrite();

      // Special form of set value count: used only for
      // this class to avoid setting the value count of children.
      // Setting these counts was already done. Doing it again
      // will corrupt nullable vectors because the writers don't
      // set the "lastSet" field of nullable vector accessors,
      // and the initial value of -1 will cause all values to
      // be overwritten.

      mapVector.setMapValueCount(vectorIndex.vectorIndex());
    }
  }

  /**
   * Writer for a an array of maps. A single array index coordinates writes
   * to the constituent member vectors so that, say, the values for (row 10,
   * element 5) all occur to the same position in the columns within the map.
   * Since the map is an array, it has an associated offset vector, which the
   * parent array writer is responsible for maintaining.
   */

  private static class ArrayMapWriter extends MapWriter {
    @SuppressWarnings("unused")
    private final RepeatedMapVector mapVector;

    private ArrayMapWriter(ColumnMetadata schema, RepeatedMapVector vector, List<AbstractObjectWriter> writers) {
      super(schema, writers);
      mapVector = vector;
    }

    @Override
    public void bindIndex(ColumnWriterIndex index) {

      // This is a repeated map, so the provided index is an array element
      // index. Convert this to an index that will not increment the element
      // index on each write so that a map with three members, say, won't
      // increment the index for each member. Rather, the index must be
      // incremented at the array level.

      bindIndex(index, new MemberWriterIndex(index));
    }

    @Override
    public void endWrite() {
      super.endWrite();

      // Do not call setValueCount on the map vector.
      // Doing so will zero-fill the composite vectors because
      // the internal map state does not track the writer state.
      // Instead, the code in this structure has set the value
      // count for each composite vector individually.

//      mapVector.getMutator().setValueCount(vectorIndex.outerIndex().vectorIndex());
    }
  }

  protected final ColumnMetadata mapColumnSchema;

  private MapWriter(ColumnMetadata schema, List<AbstractObjectWriter> writers) {
    super(schema.mapSchema(), writers);
    mapColumnSchema = schema;
  }

  public static TupleObjectWriter buildMap(ColumnMetadata schema, MapVector vector,
                                        List<AbstractObjectWriter> writers) {
    return new TupleObjectWriter(schema, new SingleMapWriter(schema, vector, writers));
  }

  public static TupleObjectWriter buildMapArray(ColumnMetadata schema, RepeatedMapVector vector,
                                        List<AbstractObjectWriter> writers) {
    return new TupleObjectWriter(schema, new ArrayMapWriter(schema, vector, writers));
  }

  public static TupleObjectWriter buildMapArray(ColumnMetadata schema, RepeatedMapVector vector) {
    assert schema.mapSchema().size() == 0;
    return buildMapArray(schema, vector, new ArrayList<AbstractObjectWriter>());
  }

  public static TupleObjectWriter build(ColumnMetadata schema, AbstractMapVector vector,
                                        List<AbstractObjectWriter> writers) {
    if (schema.isArray()) {
      return buildMapArray(schema, (RepeatedMapVector) vector, writers);
    } else {
      return buildMap(schema, (MapVector) vector, writers);
    }
  }

  public static TupleObjectWriter build(ColumnMetadata schema, AbstractMapVector vector) {
    assert schema.mapSchema().size() == 0;
    return build(schema, vector, new ArrayList<AbstractObjectWriter>());
  }
}
