package org.apache.drill.exec.physical.rowSet.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

/**
 * Test map array support in the result set loader.
 * <p>
 * The tests here should be considered in the "extra for experts"
 * category: run and/or debug these tests only after the scalar
 * tests work. Maps, and especially repeated maps, are very complex
 * constructs not to be tackled lightly.
 */

public class TestResultSetLoaderMapArray extends SubOperatorTest {

  @Test
  public void testBasics() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMapArray("m")
          .add("c", MinorType.INT)
          .add("d", MinorType.VARCHAR)
          .buildMap()
        .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new ResultSetLoaderImpl.OptionBuilder()
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Verify structure and schema

    TupleMetadata actualSchema = rootWriter.schema();
    assertEquals(2, actualSchema.size());
    assertTrue(actualSchema.metadata(1).isArray());
    assertTrue(actualSchema.metadata(1).isMap());
    assertEquals(2, actualSchema.metadata("m").mapSchema().size());
    assertEquals(2, actualSchema.column("m").getChildren().size());

    // Write a couple of rows with arrays.

    rsLoader.startBatch();
    rootWriter
      .addRow(10, new Object[] {
          new Object[] {110, "d1.1"},
          new Object[] {120, "d2.2"}})
      .addRow(20, new Object[] {})
      .addRow(30, new Object[] {
          new Object[] {310, "d3.1"},
          new Object[] {320, "d3.2"},
          new Object[] {330, "d3.3"}})
      ;

    // Verify the first batch

    RowSet actual = fixture.wrap(rsLoader.harvest());
    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow(10, new Object[] {
            new Object[] {110, "d1.1"},
            new Object[] {120, "d2.2"}})
        .addRow(20, new Object[] {})
        .addRow(30, new Object[] {
            new Object[] {310, "d3.1"},
            new Object[] {320, "d3.2"},
            new Object[] {330, "d3.3"}})
        .build();
    new RowSetComparison(expected).verifyAndClearAll(actual);

    // In the second, create a row, then add a map member.
    // Should be back-filled to empty for the first row.

    rsLoader.startBatch();
    rootWriter
      .addRow(40, new Object[] {
          new Object[] {410, "d4.1"},
          new Object[] {420, "d4.2"}});

    TupleWriter mapWriter = rootWriter.array("m").tuple();
    mapWriter.addColumn(SchemaBuilder.columnSchema("e", MinorType.VARCHAR, DataMode.OPTIONAL));

    rootWriter
      .addRow(50, new Object[] {
          new Object[] {510, "d5.1", "e5.1"},
          new Object[] {520, "d5.2", null}})
      .addRow(60, new Object[] {
          new Object[] {610, "d6.1", "e6.1"},
          new Object[] {620, "d6.2", null},
          new Object[] {630, "d6.3", "e6.3"}})
      ;

    // Verify the second batch

    actual = fixture.wrap(rsLoader.harvest());
    expected = fixture.rowSetBuilder(schema)
        .addRow(40, new Object[] {
            new Object[] {410, "d4.1", null},
            new Object[] {420, "d4.2", null}})
        .addRow(50, new Object[] {
            new Object[] {510, "d5.1", "e5.1"},
            new Object[] {520, "d5.2", null}})
        .addRow(60, new Object[] {
            new Object[] {610, "d6.1", "e6.1"},
            new Object[] {620, "d6.2", null},
            new Object[] {630, "d6.3", "e6.3"}})
        .build();
    new RowSetComparison(expected).verifyAndClearAll(actual);

    rsLoader.close();
  }

  /**
   * Version of the {#link TestResultSetLoaderProtocol#testOverwriteRow()} test
   * that uses nested columns inside an array of maps. Here we must call
   * <tt>start()</tt> to reset the array back to the initial start position after
   * each "discard."
   */

  @Test
  public void testOverwriteRow() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addMapArray("m")
          .add("b", MinorType.INT)
          .add("c", MinorType.VARCHAR)
        .buildMap()
      .buildSchema();
    ResultSetLoaderImpl.ResultSetOptions options = new ResultSetLoaderImpl.OptionBuilder()
        .setSchema(schema)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    RowSetLoader rootWriter = rsLoader.writer();

    // Can't use the shortcut to populate rows when doing overwrites.

    ScalarWriter aWriter = rootWriter.scalar("a");
    ArrayWriter maWriter = rootWriter.array("m");
    TupleWriter mWriter = maWriter.tuple();
    ScalarWriter bWriter = mWriter.scalar("b");
    ScalarWriter cWriter = mWriter.scalar("c");

    // Write 100,000 rows, overwriting 99% of them. This will cause vector
    // overflow and data corruption if overwrite does not work; but will happily
    // produce the correct result if everything works as it should.

    byte value[] = new byte[512];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsLoader.startBatch();
    while (count < 10_000) {
      rootWriter.start();
      count++;
      aWriter.setInt(count);
      for (int i = 0; i < 10; i++) {
        bWriter.setInt(count * 10 + i);
        cWriter.setBytes(value, value.length);
        maWriter.save();
      }
      if (count % 100 == 0) {
        rootWriter.save();
      }
    }

    // Verify using a reader.

    RowSet result = fixture.wrap(rsLoader.harvest());
    assertEquals(count / 100, result.rowCount());
    RowSetReader reader = result.reader();
    ArrayReader maReader = reader.array("m");
    TupleReader mReader = maReader.tuple();
    int rowId = 1;
    while (reader.next()) {
      assertEquals(rowId * 100, reader.scalar("a").getInt());
      assertEquals(10, maReader.size());
      for (int i = 0; i < 10; i++) {
        maReader.setPosn(i);
        assertEquals(rowId * 1000 + i, mReader.scalar("b").getInt());
        assertTrue(Arrays.equals(value, mReader.scalar("c").getBytes()));
      }
      rowId++;
    }

    result.clear();
    rsLoader.close();
  }
  /**
   * Version of the {#link TestResultSetLoaderProtocol#testOverwriteRow()} test
   * that uses nested columns inside an array of maps. To add more stress,
   * the Varchar column itself is an array, so we have an array inside an
   * array.
   */

}
