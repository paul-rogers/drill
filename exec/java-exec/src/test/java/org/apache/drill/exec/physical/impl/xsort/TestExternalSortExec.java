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

import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.xsort.managed.OperatorCodeGenerator;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.RowSetBuilder;
import org.apache.drill.test.RowSetComparison;
import org.apache.drill.test.TestRowSet;
import org.apache.drill.test.TestRowSet.RowSetReader;
import org.apache.drill.test.TestRowSet.RowSetWriter;
import org.apache.drill.test.TestSchema;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.junit.Test;

import jersey.repackaged.com.google.common.collect.Lists;

public class TestExternalSortExec extends DrillTest {

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
  public void testSorterBasics() throws Exception {
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
      recSet.clear();

      // Sort with one row

      recSet = new RowSetBuilder(fixture.allocator(), schema)
          .add(0, "0")
          .withSv2()
          .build();

      assertEquals(1, recSet.rowCount());

      sorter.setup(null, recSet.getSv2(), recSet.getVectorAccessible());
      sorter.sort(recSet.getSv2());

      TestRowSet expected = new RowSetBuilder(fixture.allocator(), schema)
          .add(0, "0")
          .build();
      new RowSetComparison(expected)
          .verifyAndClear(recSet);

      // Paranoia: sort with two rows.

      recSet = new RowSetBuilder(fixture.allocator(), schema)
          .add(1, "1")
          .add(0, "0")
          .withSv2()
          .build();

      assertEquals(2, recSet.rowCount());

      sorter.setup(null, recSet.getSv2(), recSet.getVectorAccessible());
      sorter.sort(recSet.getSv2());

      expected = new RowSetBuilder(fixture.allocator(), schema)
          .add(0, "0")
          .add(1, "1")
          .build();
      new RowSetComparison(expected)
          .verifyAndClear(recSet);
    }
  }

  public static void setFromInt(ColumnWriter writer, int value) {
    switch (writer.getType()) {
    case BYTES:
      writer.setBytes(Integer.toHexString(value).getBytes());
      break;
    case DOUBLE:
      writer.setDouble(value);
      break;
    case INTEGER:
      writer.setInt(value);
      break;
    case LONG:
      writer.setLong(value);
      break;
    case STRING:
      writer.setString(Integer.toString(value));
      break;
    case DECIMAL:
      writer.setDecimal(BigDecimal.valueOf(value));
      break;
    default:
      throw new IllegalStateException("Unknown writer type: " + writer.getType());
    }
  }

  private abstract static class SortTester {

    private OperatorFixture fixture;
    private OperatorCodeGenerator opCodeGen;

    public SortTester(OperatorFixture fixture,
        OperatorCodeGenerator opCodeGen) {
      this.fixture = fixture;
      this.opCodeGen = opCodeGen;
    }

    public void test(MinorType type) throws SchemaChangeException {
      DataItem data[] = makeDataArray(20);
      TestSchema schema = makeSchema(type, false);
      TestRowSet input = makeDataSet(fixture.allocator(), schema, data);
      input.makeSv2();
      SingleBatchSorter sorter = opCodeGen.createNewSorter(input.getVectorAccessible());
      sorter.setup(null, input.getSv2(), input.getVectorAccessible());
      sorter.sort(input.getSv2());
      verify(data, input);
    }

     public TestSchema makeSchema(MinorType type, boolean nullable) {
      return TestSchema.builder()
          .add("key", type, nullable ? DataMode.OPTIONAL : DataMode.REQUIRED)
          .add("value", MinorType.VARCHAR)
          .build();
    }

    public static class DataItem {
      public final int key;
      public final int value;
      public final boolean isNull;

      public DataItem(int key, int value, boolean isNull) {
        this.key = key;
        this.value = value;
        this.isNull = isNull;
      }

      @Override
      public String toString() {
        return "(" + key + ", \"" + value + "\", " +
               (isNull ? "null" : "set") + ")";
      }
    }

    public DataItem[] makeDataArray(int size) {
      DataItem values[] = new DataItem[size];
      int key = 11;
      int delta = 3;
      for (int i = 0; i < size; i++) {
        values[i] = new DataItem(key, i, key % 5 == 0);
        key = (key + delta) % size;
      }
      return values;
    }

    public TestRowSet makeDataSet(BufferAllocator allocator, TestSchema schema, DataItem values[]) {
      boolean nullable = schema.get(0).getDataMode() == DataMode.OPTIONAL;
      TestRowSet rowSet = new TestRowSet(allocator, schema);
      RowSetWriter writer = rowSet.writer(values.length);
      for (int i = 0; i < values.length; i++) {
        DataItem item = values[i];
        if (nullable && item.isNull) {
          writer.column(0).setNull();
        } else {
          setFromInt(writer.column(0), item.key);
        }
        writer.column(1).setString(Integer.toString(item.value));
        writer.advance();
      }
      writer.done();
      return rowSet;
    }

    private void verify(DataItem[] data, TestRowSet actual) {
      DataItem expected[] = Arrays.copyOf(data, data.length);
      doSort(expected);
      TestRowSet expectedRows = makeDataSet(actual.getAllocator(), actual.schema(), expected);
      System.out.println("Expected:");
      expectedRows.print();
      System.out.println("Actual:");
      actual.print();
      new RowSetComparison(expectedRows)
          .verifyAndClear(actual);
    }

    protected abstract void doSort(DataItem[] expected);
  }

  private static class TestSorterNumericAscHigh extends SortTester {

    public TestSorterNumericAscHigh(OperatorFixture fixture,
        OperatorCodeGenerator opCodeGen) {
      super(fixture, opCodeGen);
    }

    @Override
    protected void doSort(DataItem[] expected) {
      Arrays.sort(expected, new Comparator<DataItem>(){
        @Override
        public int compare(DataItem o1, DataItem o2) {
          return Integer.compare(o1.key, o2.key);
        }
      });
    }
  }

  private static class TestSorterStringAscHigh extends SortTester {

    public TestSorterStringAscHigh(OperatorFixture fixture,
        OperatorCodeGenerator opCodeGen) {
      super(fixture, opCodeGen);
    }

    @Override
    protected void doSort(DataItem[] expected) {
      Arrays.sort(expected, new Comparator<DataItem>(){
        @Override
        public int compare(DataItem o1, DataItem o2) {
          return Integer.toString(o1.key).compareTo(Integer.toString(o2.key));
        }
      });
    }
  }

  private static class TestSorterBinaryAscHigh extends SortTester {

    public TestSorterBinaryAscHigh(OperatorFixture fixture,
        OperatorCodeGenerator opCodeGen) {
      super(fixture, opCodeGen);
    }

    @Override
    protected void doSort(DataItem[] expected) {
      Arrays.sort(expected, new Comparator<DataItem>(){
        @Override
        public int compare(DataItem o1, DataItem o2) {
          return Integer.toHexString(o1.key).compareTo(Integer.toHexString(o2.key));
        }
      });
    }
  }

  @Test
  public void testSorterTypes() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.builder().build()) {

      FieldReference expr = FieldReference.getWithQuotedRef("key");
      Ordering ordering = new Ordering(Ordering.ORDER_ASC, expr, Ordering.NULLS_LAST);
      Sort popConfig = new Sort(null, Lists.newArrayList(ordering), false);

      OperatorCodeGenerator opCodeGen = new OperatorCodeGenerator(fixture.codeGenContext(), popConfig);

      TestSorterNumericAscHigh tester1 = new TestSorterNumericAscHigh(fixture, opCodeGen);
//      tester1.test(MinorType.TINYINT); DRILL-5329
//      tester1.test(MinorType.UINT1); DRILL-5329
//      tester1.test(MinorType.SMALLINT); DRILL-5329
//      tester1.test(MinorType.UINT2); DRILL-5329
      tester1.test(MinorType.INT);
//      tester1.test(MinorType.UINT4); DRILL-5329
      tester1.test(MinorType.BIGINT);
//      tester1.test(MinorType.UINT8); DRILL-5329
      tester1.test(MinorType.FLOAT4);
      tester1.test(MinorType.FLOAT8);
      tester1.test(MinorType.DECIMAL9);
      tester1.test(MinorType.DECIMAL18);
//      tester1.test(MinorType.DECIMAL28SPARSE); DRILL-5329
//      tester1.test(MinorType.DECIMAL38SPARSE); DRILL-5329
//    tester1.test(MinorType.DECIMAL28DENSE); No writer
//    tester1.test(MinorType.DECIMAL38DENSE); No writer

      TestSorterStringAscHigh tester2 = new TestSorterStringAscHigh(fixture, opCodeGen);
      tester2.test(MinorType.VARCHAR);
//      tester2.test(MinorType.VAR16CHAR); DRILL-5329

      TestSorterBinaryAscHigh tester3 = new TestSorterBinaryAscHigh(fixture, opCodeGen);
      tester3.test(MinorType.VARBINARY);
    }
  }
}
