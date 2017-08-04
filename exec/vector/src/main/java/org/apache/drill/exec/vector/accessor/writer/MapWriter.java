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

import java.util.List;

import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter.ArrayElementWriterIndex;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

/**
 * Writer for a Drill Map type. Maps are actually tuples, just like rows.
 */

public abstract class MapWriter extends AbstractTupleWriter {

  private static class MemberWriterIndex implements ColumnWriterIndex {
    private ColumnWriterIndex baseIndex;

    private MemberWriterIndex(ColumnWriterIndex baseIndex) {
      this.baseIndex = baseIndex;
    }

    @Override public int vectorIndex() { return baseIndex.vectorIndex(); }
    @Override public void overflowed() { baseIndex.overflowed(); }
    @Override public boolean legal() { return baseIndex.legal(); }
    @Override public void nextElement() { }
  }

  private static class SingleMapWriter extends MapWriter {
    private MapVector mapVector;

    private SingleMapWriter(ColumnMetadata schema, List<AbstractObjectWriter> writers) {
      super(schema, writers);
    }

    @Override
    public void bindVector(ValueVector vector) {
      mapVector = (MapVector) vector;
    }

    @Override
    public void endWrite() {
      super.endWrite();
      mapVector.getMutator().setValueCount(vectorIndex.vectorIndex());
    }

    @Override
    public void bindIndex(ColumnWriterIndex index) {
      bindIndex(index, index);
    }


    @Override
    public void reset(int index) {
      // TODO Auto-generated method stub

    }
  }

  private static class ArrayMapWriter extends MapWriter {
    private RepeatedMapVector mapVector;

    private ArrayMapWriter(ColumnMetadata schema, List<AbstractObjectWriter> writers) {
      super(schema, writers);
    }

    @Override
    public void bindVector(ValueVector vector) {
      mapVector = (RepeatedMapVector) vector;
    }

    @Override
    public void bindIndex(ColumnWriterIndex index) {

      // This is a repeated map, then the provided index is an array element
      // index. Convert this to an index that will not increment the element
      // index on each write so that a map with three members, say, won't
      // increment the index for each member. Rather, the index must be
      // incremented at the array level.

      final ColumnWriterIndex childIndex = new MemberWriterIndex(index);
      bindIndex(index, childIndex);
    }

    @Override
    public void endWrite() {
      super.endWrite();

      // A bit of a hack. This writer sees the element index. But,
      // the vector wants the base element count, provided by the
      // parent index.

      ColumnWriterIndex baseIndex = ((ArrayElementWriterIndex) vectorIndex).baseIndex();
      mapVector.getMutator().setValueCount(baseIndex.vectorIndex());
    }
  }

  protected final ColumnMetadata mapColumnSchema;

  private MapWriter(ColumnMetadata schema, List<AbstractObjectWriter> writers) {
    super(schema.mapSchema(), writers);
    mapColumnSchema = schema;
  }

  public static TupleObjectWriter buildSingleMap(ColumnMetadata schema,
                                        List<AbstractObjectWriter> writers) {
    return new TupleObjectWriter(new SingleMapWriter(schema, writers));
  }

  public static TupleObjectWriter buildMapArray(ColumnMetadata schema,
                                        List<AbstractObjectWriter> writers) {
    return new TupleObjectWriter(new ArrayMapWriter(schema, writers));
  }

  protected void bindIndex(ColumnWriterIndex index, ColumnWriterIndex childIndex) {
    vectorIndex = index;

    for (int i = 0; i < writers.size(); i++) {
      writers.get(i).bindIndex(childIndex);
    }
  }

  @Override
  public void reset(int index) {
    assert false;
  }
}
