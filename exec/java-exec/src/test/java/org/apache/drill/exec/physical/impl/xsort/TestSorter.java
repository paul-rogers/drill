package org.apache.drill.exec.physical.impl.xsort;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.xsort.managed.OperatorCodeGenerator;
import org.apache.drill.exec.vector.accessor.AccessorUtilities;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.TestRowSet;
import org.apache.drill.test.rowSet.TestSchema;
import org.apache.drill.test.rowSet.TestRowSet.RowSetWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestSorter extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setup() {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    fixture.close();
  }

  public void runSimpleTest(TestRowSet rowSet, TestRowSet expected) throws Exception {
    FieldReference expr = FieldReference.getWithQuotedRef("key");
    Ordering ordering = new Ordering(Ordering.ORDER_ASC, expr, Ordering.NULLS_LAST);
    Sort popConfig = new Sort(null, Lists.newArrayList(ordering), false);
    OperatorCodeGenerator opCodeGen = new OperatorCodeGenerator(fixture.codeGenContext(), popConfig);

    SingleBatchSorter sorter = opCodeGen.getSorter(rowSet.getVectorAccessible());
    sorter.setup(null, rowSet.getSv2(), rowSet.getVectorAccessible());
    sorter.sort(rowSet.getSv2());

    new RowSetComparison(expected)
        .verifyAndClear(rowSet);
  }

  public static TestSchema makeSchema(MinorType type, boolean nullable) {
    return TestSchema.builder()
        .add("key", type, nullable ? DataMode.OPTIONAL : DataMode.REQUIRED)
        .add("value", MinorType.VARCHAR)
        .build();
  }

  public static TestSchema nonNullSchema() {
    return makeSchema(MinorType.INT, false);
  }

  public static TestSchema nullSchema() {
    return makeSchema(MinorType.INT, true);
  }

  // Test degenerate case: no rows

  @Test
  public void testEmptyRowSet() throws Exception {
    TestSchema schema = nonNullSchema();
    TestRowSet rowSet = new RowSetBuilder(fixture.allocator(), schema)
        .withSv2()
        .build();
    TestRowSet expected = new RowSetBuilder(fixture.allocator(), schema)
        .build();
    runSimpleTest(rowSet, expected);
  }

  // Sanity test: single row

  @Test
  public void testSingleRow() throws Exception {
    TestSchema schema = nonNullSchema();
    TestRowSet rowSet = new RowSetBuilder(fixture.allocator(), schema)
          .add(0, "0")
          .withSv2()
          .build();

    TestRowSet expected = new RowSetBuilder(fixture.allocator(), schema)
        .add(0, "0")
        .build();
    runSimpleTest(rowSet, expected);
  }

  // Paranoia: sort with two rows.

  @Test
  public void testTwoRows() throws Exception {
    TestSchema schema = nonNullSchema();
    TestRowSet rowSet = new RowSetBuilder(fixture.allocator(), schema)
          .add(1, "1")
          .add(0, "0")
          .withSv2()
          .build();

    TestRowSet expected = new RowSetBuilder(fixture.allocator(), schema)
        .add(0, "0")
        .add(1, "1")
        .build();
    runSimpleTest(rowSet, expected);
  }

  private abstract static class SortTester {

    private OperatorFixture fixture;
    private OperatorCodeGenerator opCodeGen;
    protected boolean nullable;
    protected Ordering ordering;
    protected DataItem data[];

    public SortTester(OperatorFixture fixture, String sortOrder, String nullOrder) {
      this.fixture = fixture;
      FieldReference expr = FieldReference.getWithQuotedRef("key");
      ordering = new Ordering(sortOrder, expr, nullOrder);
      Sort popConfig = new Sort(null, Lists.newArrayList(ordering), false);

      opCodeGen = new OperatorCodeGenerator(fixture.codeGenContext(), popConfig);
    }

    public void test(MinorType type) throws SchemaChangeException {
      data = makeDataArray(20);
      TestSchema schema = makeSchema(type, nullable);
      TestRowSet input = makeDataSet(fixture.allocator(), schema, data);
      input.makeSv2();
      SingleBatchSorter sorter = opCodeGen.createNewSorter(input.getVectorAccessible());
      sorter.setup(null, input.getSv2(), input.getVectorAccessible());
      sorter.sort(input.getSv2());
      verify(input);
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

    public TestRowSet makeDataSet(BufferAllocator allocator, TestSchema schema, DataItem[] items) {
      TestRowSet rowSet = new TestRowSet(allocator, schema);
      RowSetWriter writer = rowSet.writer(items.length);
      for (int i = 0; i < items.length; i++) {
        DataItem item = items[i];
        if (nullable && item.isNull) {
          writer.column(0).setNull();
        } else {
          AccessorUtilities.setFromInt(writer.column(0), item.key);
        }
        writer.column(1).setString(Integer.toString(item.value));
        writer.advance();
      }
      writer.done();
      return rowSet;
    }

    private void verify(TestRowSet actual) {
      DataItem expected[] = Arrays.copyOf(data, data.length);
      doSort(expected);
      TestRowSet expectedRows = makeDataSet(actual.getAllocator(), actual.schema(), expected);
      System.out.println("Expected:");
      expectedRows.print();
      System.out.println("Actual:");
      actual.print();
      doVerify(expected, expectedRows, actual);
    }

    protected void doVerify(DataItem[] expected, TestRowSet expectedRows, TestRowSet actual) {
      new RowSetComparison(expectedRows)
            .verifyAndClear(actual);
    }

    protected abstract void doSort(DataItem[] expected);
  }

  private static class TestSorterNumeric extends SortTester {

    private final int sign;

    public TestSorterNumeric(OperatorFixture fixture, boolean asc) {
      super(fixture,
            asc ? Ordering.ORDER_ASC : Ordering.ORDER_DESC,
            Ordering.NULLS_UNSPECIFIED);
      sign = asc ? 1 : -1;
    }

    @Override
    protected void doSort(DataItem[] expected) {
      Arrays.sort(expected, new Comparator<DataItem>(){
        @Override
        public int compare(DataItem o1, DataItem o2) {
          return sign * Integer.compare(o1.key, o2.key);
        }
      });
    }
  }

  private static class TestSorterNullableNumeric extends SortTester {

    private final int sign;
    private final int nullSign;

    public TestSorterNullableNumeric(OperatorFixture fixture, boolean asc, boolean nullsLast) {
      super(fixture,
          asc ? Ordering.ORDER_ASC : Ordering.ORDER_DESC,
          nullsLast ? Ordering.NULLS_LAST : Ordering.NULLS_FIRST);
      sign = asc ? 1 : -1;
      nullSign = nullsLast ? 1 : -1;
      nullable = true;
    }

    @Override
    protected void doSort(DataItem[] expected) {
      Arrays.sort(expected, new Comparator<DataItem>(){
        @Override
        public int compare(DataItem o1, DataItem o2) {
          if (o1.isNull  &&  o2.isNull) { return 0; }
          if (o1.isNull) { return nullSign; }
          if (o2.isNull) { return -nullSign; }
          return sign * Integer.compare(o1.key, o2.key);
        }
      });
    }

    @Override
    protected void doVerify(DataItem[] expected, TestRowSet expectedRows, TestRowSet actual) {
      int nullCount = 0;
      for (DataItem item : expected) {
        if (item.isNull) { nullCount++; }
      }
      int length = expected.length - nullCount;
      int offset = (nullSign == 1) ? 0 : nullCount;
      new RowSetComparison(expectedRows)
            .offset(offset)
            .span(length)
            .verify(actual);
      offset = length - offset;
      new RowSetComparison(expectedRows)
            .offset(offset)
            .span(nullCount)
            .withMask(true, false)
            .verifyAndClear(actual);
    }
  }

  private static class TestSorterStringAsc extends SortTester {

    public TestSorterStringAsc(OperatorFixture fixture) {
      super(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED);
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

  private static class TestSorterBinaryAsc extends SortTester {

    public TestSorterBinaryAsc(OperatorFixture fixture) {
      super(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED);
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
  public void testNumericTypes() throws Exception {
    TestSorterNumeric tester1 = new TestSorterNumeric(fixture, true);
//      tester1.test(MinorType.TINYINT); DRILL-5329
//      tester1.test(MinorType.UINT1); DRILL-5329
//      tester1.test(MinorType. ); DRILL-5329
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
    tester1.test(MinorType.DATE);
    tester1.test(MinorType.TIME);
    tester1.test(MinorType.TIMESTAMP);
//      tester1.test(MinorType.INTERVAL); No writer
    tester1.test(MinorType.INTERVALYEAR);
//      tester1.test(MinorType.INTERVALDAY); No writer
  }

  @Test
  public void testVarCharTypes() throws Exception {
    TestSorterStringAsc tester = new TestSorterStringAsc(fixture);
    tester.test(MinorType.VARCHAR);
//      tester.test(MinorType.VAR16CHAR); DRILL-5329
  }

  @Test
  public void testVarBinary() throws Exception {
    TestSorterBinaryAsc tester = new TestSorterBinaryAsc(fixture);
    tester.test(MinorType.VARBINARY);
  }

  @Test
  public void testDesc() throws Exception {
    TestSorterNumeric tester = new TestSorterNumeric(fixture, false);
    tester.test(MinorType.INT);
  }

  /**
   * Verify that nulls sort in the requested position: high or low.
   * Earlier tests verify that "unspecified" maps to high or low
   * depending on sort order.
   */

  @Test
  public void testNullable() throws Exception {
    TestSorterNullableNumeric tester = new TestSorterNullableNumeric(fixture, true, true);
    tester.test(MinorType.INT);
    tester = new TestSorterNullableNumeric(fixture, true, false);
    tester.test(MinorType.INT);
    tester = new TestSorterNullableNumeric(fixture, false, true);
    tester.test(MinorType.INT);
    tester = new TestSorterNullableNumeric(fixture, false, false);
    tester.test(MinorType.INT);
  }
}
