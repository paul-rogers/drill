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
package org.apache.drill.exec.physical.impl.xsort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.xsort.managed.OperatorCodeGenerator;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.TestRowSet;
import org.apache.drill.test.TestRowSet.RecordSetReader;
import org.apache.drill.test.TestRowSet.RecordSetWriter;
import org.apache.drill.test.TestSchema;
import org.junit.Test;

import com.google.common.base.Charsets;

import jersey.repackaged.com.google.common.collect.Lists;

public class TestSortExp {

  @Test
  public void testFieldReference() {
    // Misnomer: the reference must be unquoted.
    FieldReference expr = FieldReference.getWithQuotedRef("foo");
    assertEquals(Types.LATE_BIND_TYPE, expr.getMajorType());
    assertTrue(expr.isSimplePath());
    assertEquals("foo", expr.getRootSegment().getPath());
    assertEquals("`foo`", expr.toExpr());
  }

  @Test
  public void testOrdering() {
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(null));
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_ASC));
    assertEquals(Direction.DESCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_DESC));
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_ASCENDING));
    assertEquals(Direction.DESCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_DESCENDING));
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_ASC.toLowerCase()));
    assertEquals(Direction.DESCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_DESC.toLowerCase()));
    assertEquals(Direction.ASCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_ASCENDING.toLowerCase()));
    assertEquals(Direction.DESCENDING, Ordering.getOrderingSpecFromString(Ordering.ORDER_DESCENDING.toLowerCase()));
    try {
      Ordering.getOrderingSpecFromString("");
      fail();
    } catch(DrillRuntimeException e) { }
    try {
      Ordering.getOrderingSpecFromString("foo");
      fail();
    } catch(DrillRuntimeException e) { }

    assertEquals(NullDirection.UNSPECIFIED, Ordering.getNullOrderingFromString(null));
    assertEquals(NullDirection.FIRST, Ordering.getNullOrderingFromString(Ordering.NULLS_FIRST));
    assertEquals(NullDirection.LAST, Ordering.getNullOrderingFromString(Ordering.NULLS_LAST));
    assertEquals(NullDirection.UNSPECIFIED, Ordering.getNullOrderingFromString(Ordering.NULLS_UNSPECIFIED));
    assertEquals(NullDirection.FIRST, Ordering.getNullOrderingFromString(Ordering.NULLS_FIRST.toLowerCase()));
    assertEquals(NullDirection.LAST, Ordering.getNullOrderingFromString(Ordering.NULLS_LAST.toLowerCase()));
    assertEquals(NullDirection.UNSPECIFIED, Ordering.getNullOrderingFromString(Ordering.NULLS_UNSPECIFIED.toLowerCase()));
    try {
      Ordering.getNullOrderingFromString("");
      fail();
    } catch(DrillRuntimeException e) { }
    try {
      Ordering.getNullOrderingFromString("foo");
      fail();
    } catch(DrillRuntimeException e) { }

    FieldReference expr = FieldReference.getWithQuotedRef("foo");

    // Test all getters

    Ordering ordering = new Ordering((String) null, expr, (String) null);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());
    assertSame(expr, ordering.getExpr());
    assertTrue(ordering.nullsSortHigh());

    // Test all ordering strings

    ordering = new Ordering((String) Ordering.ORDER_ASC, expr, (String) null);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    ordering = new Ordering((String) Ordering.ORDER_ASC.toLowerCase(), expr, (String) null);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    ordering = new Ordering((String) Ordering.ORDER_ASCENDING, expr, (String) null);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    ordering = new Ordering((String) Ordering.ORDER_DESC, expr, (String) null);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    ordering = new Ordering((String) Ordering.ORDER_DESCENDING, expr, (String) null);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());

    // Test all null ordering strings

    ordering = new Ordering((String) null, expr, Ordering.NULLS_FIRST);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.FIRST, ordering.getNullDirection());
    assertFalse(ordering.nullsSortHigh());

    ordering = new Ordering((String) null, expr, Ordering.NULLS_FIRST);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.FIRST, ordering.getNullDirection());
    assertFalse(ordering.nullsSortHigh());

    ordering = new Ordering((String) null, expr, Ordering.NULLS_LAST);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.LAST, ordering.getNullDirection());
    assertTrue(ordering.nullsSortHigh());

    ordering = new Ordering((String) null, expr, Ordering.NULLS_UNSPECIFIED);
    assertEquals(Direction.ASCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());
    assertTrue(ordering.nullsSortHigh());

    // Unspecified order is always nulls high

    ordering = new Ordering(Ordering.ORDER_DESC, expr, Ordering.NULLS_UNSPECIFIED);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.UNSPECIFIED, ordering.getNullDirection());
    assertTrue(ordering.nullsSortHigh());

    // Null sort direction reverses with a Desc sort.

    ordering = new Ordering(Ordering.ORDER_DESC, expr, Ordering.NULLS_FIRST);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.FIRST, ordering.getNullDirection());
    assertTrue(ordering.nullsSortHigh());

    ordering = new Ordering(Ordering.ORDER_DESC, expr, Ordering.NULLS_LAST);
    assertEquals(Direction.DESCENDING, ordering.getDirection());
    assertEquals(NullDirection.LAST, ordering.getNullDirection());
    assertFalse(ordering.nullsSortHigh());
  }

  @Test
  public void testSortSpec() {
    FieldReference expr = FieldReference.getWithQuotedRef("foo");
    Ordering ordering = new Ordering(Ordering.ORDER_ASC, expr, Ordering.NULLS_FIRST);

    // Basics

    ExternalSort popConfig = new ExternalSort(null, Lists.newArrayList(ordering), false);
    assertSame(ordering, popConfig.getOrderings().get(0));
    assertFalse(popConfig.getReverse());
    assertEquals(SelectionVectorMode.FOUR_BYTE, popConfig.getSVMode());
    assertEquals(CoreOperatorType.EXTERNAL_SORT_VALUE, popConfig.getOperatorType());
    assertEquals(ExternalSort.DEFAULT_INIT_ALLOCATION, popConfig.getInitialAllocation());
    assertEquals(AbstractBase.DEFAULT_MAX_ALLOCATION, popConfig.getMaxAllocation());
    assertTrue(popConfig.isExecutable());

    // Non-default settings

    popConfig = new ExternalSort(null, Lists.newArrayList(ordering), true);
    assertTrue(popConfig.getReverse());
    long maxAlloc = 50_000_000;
    popConfig.setMaxAllocation(maxAlloc);
    assertEquals(ExternalSort.DEFAULT_INIT_ALLOCATION, popConfig.getInitialAllocation());
    assertEquals(maxAlloc, popConfig.getMaxAllocation());
  }

  public static final long ONE_MEG = 1024 * 1024;

  @Test
  public void testSorter() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.builder().build()) {

      FieldReference expr = FieldReference.getWithQuotedRef("key");
      Ordering ordering = new Ordering(Ordering.ORDER_ASC, expr, Ordering.NULLS_LAST);
      Sort popConfig = new Sort(null, Lists.newArrayList(ordering), false);

      OperatorCodeGenerator opCodeGen = new OperatorCodeGenerator(fixture.codeGenContext(), popConfig);

      TestSchema schema = TestSchema.builder()
          .add("key", MinorType.INT)
          .add("value", MinorType.VARCHAR)
          .build();

      TestRowSet recSet = new TestRowSet(fixture.allocator(), schema);
      SingleBatchSorter sorter = opCodeGen.getSorter(recSet.getVectorAccessible());

      // Sort empty row set.

      recSet.allocate(10);
      recSet.setRowCount(0);
      recSet.makeSv2();
      sorter.setup(null, recSet.getSv2(), recSet.getVectorAccessible());
      sorter.sort(recSet.getSv2());
      assertEquals(0, recSet.rowCount());

      // Sort with one row

      recSet.clear();
      recSet.allocate(10);
      RecordSetWriter writer = recSet.writer();
      writer.column(0).setInt(0);
      writer.column(1).setString("0");
      writer.advance();
      writer.done();

      assertEquals(1, recSet.rowCount());

      recSet.makeSv2();
      sorter.setup(null, recSet.getSv2(), recSet.getVectorAccessible());
      sorter.sort(recSet.getSv2());

      RecordSetReader reader = recSet.reader();
      assertTrue(reader.valid());
      assertEquals(0, reader.column(0).getInt());
      assertEquals("0", reader.column(1).getString());
      assertFalse(reader.advance());
      assertFalse(reader.valid());
      recSet.clear();
    }
  }

}
