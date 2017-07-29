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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;

/**
 * Writer for a Drill Map type. Maps are actually tuples, just like rows.
 */

public class MapWriter extends AbstractTupleWriter {

  public static class MemberWriterIndex implements ColumnWriterIndex {
    private ColumnWriterIndex baseIndex;

    private MemberWriterIndex(ColumnWriterIndex baseIndex) {
      this.baseIndex = baseIndex;
    }

    @Override public int vectorIndex() { return baseIndex.vectorIndex(); }
    @Override public void overflowed() { baseIndex.overflowed(); }
    @Override public boolean legal() { return baseIndex.legal(); }
    @Override public void nextElement() { }
  }

  protected final ColumnMetadata mapColumnSchema;

  private MapWriter(ColumnMetadata schema, AbstractObjectWriter[] writers) {
    super(schema.mapSchema(), writers);
    mapColumnSchema = schema;
  }

  public static TupleObjectWriter build(ColumnMetadata schema, AbstractObjectWriter[] writers) {
    return new TupleObjectWriter(new MapWriter(schema, writers));
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    vectorIndex = index;

    // If this is a repeated map, then the provided index is an array element
    // index. Convert this to an index that will not increment the element
    // index on each write so that a map with three members, say, won't
    // increment the index for each member. Rather, the index must be
    // incremented at the array level.

    final ColumnWriterIndex childIndex;
    if (mapColumnSchema.mode() == DataMode.REPEATED) {
      childIndex = new MemberWriterIndex(index);
    } else {
      childIndex = index;
    }
    for (int i = 0; i < writers.length; i++) {
      writers[i].bindIndex(childIndex);
    }
  }
}
