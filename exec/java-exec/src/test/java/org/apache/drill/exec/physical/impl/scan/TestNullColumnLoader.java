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
package org.apache.drill.exec.physical.impl.scan;

import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.NullColumnLoader;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.NullProjectedColumn;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.NullResultVectorCacheImpl;
import org.apache.drill.exec.physical.rowSet.impl.ResultVectorCacheImpl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestNullColumnLoader extends SubOperatorTest {

  private NullProjectedColumn makeNullCol(String name, MajorType nullType) {

    // For this test, we don't need the projection, so just
    // set it to null.

    return new NullProjectedColumn(name, nullType, null);
  }

  private NullProjectedColumn makeNullCol(String name) {
    return makeNullCol(name, null);
  }

  /**
   * Test the simplest case: default null type, nothing in the vector
   * cache. Specify no column type, the special NULL type, or a
   * predefined type. Output types should be set accordingly.
   */

  @Test
  public void testBasics() {

    List<NullProjectedColumn> defns = new ArrayList<>();
    defns.add(makeNullCol("unspecified", null));
    defns.add(makeNullCol("nullType", MajorType.newBuilder()
        .setMinorType(MinorType.NULL)
        .setMode(DataMode.OPTIONAL)
        .build()));
    defns.add(makeNullCol("specifiedOpt", MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build()));
    defns.add(makeNullCol("specifiedReq", MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.REQUIRED)
        .build()));
    defns.add(makeNullCol("specifiedArray", MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.REPEATED)
        .build()));

    ResultVectorCache cache = new NullResultVectorCacheImpl(fixture.allocator());
    NullColumnLoader staticLoader = new NullColumnLoader(cache, defns, null);

    // Create a batch

    VectorContainer output = staticLoader.load(2);

    // Verify values and types

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("unspecified", NullColumnLoader.DEFAULT_NULL_TYPE)
        .add("nullType", NullColumnLoader.DEFAULT_NULL_TYPE)
        .addNullable("specifiedOpt", MinorType.VARCHAR)
        .addNullable("specifiedReq", MinorType.VARCHAR)
        .addArray("specifiedArray", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(null, null, null, null, new String[] {})
        .addRow(null, null, null, null, new String[] {})
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(output));
    staticLoader.close();
  }

  @Test
  public void testCustomNullType() {

    List<NullProjectedColumn> defns = new ArrayList<>();
    defns.add(makeNullCol("unspecified", null));
    defns.add(makeNullCol("nullType", MajorType.newBuilder()
        .setMinorType(MinorType.NULL)
        .setMode(DataMode.OPTIONAL)
        .build()));
    defns.add(makeNullCol("nullTypeReq", MajorType.newBuilder()
        .setMinorType(MinorType.NULL)
        .setMode(DataMode.REQUIRED)
        .build()));

    // Null type array does not make sense, so is not tested.

    ResultVectorCache cache = new NullResultVectorCacheImpl(fixture.allocator());
    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();
    NullColumnLoader staticLoader = new NullColumnLoader(cache, defns, nullType);

    // Create a batch

    VectorContainer output = staticLoader.load(2);

    // Verify values and types

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("unspecified", nullType)
        .add("nullType", nullType)
        .add("nullTypeReq", nullType)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(null, null, null)
        .addRow(null, null, null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(output));
    staticLoader.close();
  }

  @SuppressWarnings("resource")
  @Test
  public void testCachedTypes() {

    List<NullProjectedColumn> defns = new ArrayList<>();
    defns.add(makeNullCol("req"));
    defns.add(makeNullCol("opt"));
    defns.add(makeNullCol("rep"));
    defns.add(makeNullCol("unk"));

    // Populate the cache with a column of each mode.

    ResultVectorCacheImpl cache = new ResultVectorCacheImpl(fixture.allocator());
    cache.addOrGet(SchemaBuilder.columnSchema("req", MinorType.FLOAT8, DataMode.REQUIRED));
    ValueVector opt = cache.addOrGet(SchemaBuilder.columnSchema("opt", MinorType.FLOAT8, DataMode.OPTIONAL));
    ValueVector rep = cache.addOrGet(SchemaBuilder.columnSchema("rep", MinorType.FLOAT8, DataMode.REPEATED));

    // Use nullable Varchar for unknown null columns.

    MajorType nullType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .build();
    NullColumnLoader staticLoader = new NullColumnLoader(cache, defns, nullType);

    // Create a batch

    VectorContainer output = staticLoader.load(2);

    // Verify vectors are reused

    assertSame(opt, output.getValueVector(1).getValueVector());
    assertSame(rep, output.getValueVector(2).getValueVector());

    // Verify values and types

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("req", MinorType.FLOAT8)
        .addNullable("opt", MinorType.FLOAT8)
        .addArray("rep", MinorType.FLOAT8)
        .addNullable("unk", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(null, null, new int[] { }, null)
        .addRow(null, null, new int[] { }, null)
        .build();

    new RowSetComparison(expected)
        .verifyAndClearAll(fixture.wrap(output));
    staticLoader.close();
  }
}
