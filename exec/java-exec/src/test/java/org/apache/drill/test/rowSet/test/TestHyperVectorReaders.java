package org.apache.drill.test.rowSet.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetWriter;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.HyperRowSetImpl;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSet.HyperRowSet;
import org.junit.Test;

public class TestHyperVectorReaders extends SubOperatorTest {

  /**
   * Test the simplest case: a top-level required vector. Has no contained vectors.
   * This test focuses on the SV4 indirection mechanism itself
   */

  @Test
  public void testRequired() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();

    SingleRowSet rowSet1;
    {
      ExtendableRowSet rowSet = fixture.rowSet(schema);
      RowSetWriter writer = rowSet.writer();
      for (int i = 0; i < 10; i++) {
        writer.scalar(0).setInt(i * 10);
        writer.save();
      }
      rowSet1 = writer.done();
    }

    SingleRowSet rowSet2;
    {
      ExtendableRowSet rowSet = fixture.rowSet(schema);
      RowSetWriter writer = rowSet.writer();
      for (int i = 10; i < 20; i++) {
        writer.scalar(0).setInt(i * 10);
        writer.save();
      }
      rowSet2 = writer.done();
    }

    // Build the hyper batch
    // [0, 10, 20, ... 190]

    HyperRowSet hyperSet = HyperRowSetImpl.fromRowSets(fixture.allocator(), rowSet1, rowSet2);
    assertEquals(20, hyperSet.rowCount());

    // Populate the indirection vector:
    // (1, 9), (0, 9), (1, 8), (0, 8), ... (0, 0)

    @SuppressWarnings("resource")
    SelectionVector4 sv4 = hyperSet.getSv4();
    for (int i = 0; i < 20; i++) {
      int batch = i % 2;
      int offset = 9 - i / 2;
      sv4.set(i, batch, offset);
    }

    // Sanity check.

    for (int i = 0; i < 20; i++) {
      int batch = i % 2;
      int offset = 9 - i / 2;
      int encoded = sv4.get(i);
      assertEquals(batch, SelectionVector4.getBatchIndex(encoded));
      assertEquals(offset, SelectionVector4.getRecordIndex(encoded));
    }

    // Verify reader
    // Expected: [190, 90, 180, 80, ... 0]

    RowSetReader reader = hyperSet.reader();
    for (int i = 0; i < 20; i++) {
      assertTrue(reader.next());
      int batch = i % 2;
      int offset = 9 - i / 2;
      int expected = batch * 100 + offset * 10;
      assertEquals(expected, reader.scalar(0).getInt());
    }
    assertFalse(reader.next());

    // Validate using an expected result set.

    RowSetBuilder rsBuilder = fixture.rowSetBuilder(schema);
    for (int i = 0; i < 20; i++) {
      int batch = i % 2;
      int offset = 9 - i / 2;
      int expected = batch * 100 + offset * 10;
      rsBuilder.addRow(expected);
    }

    new RowSetComparison(rsBuilder.build())
      .verifyAndClearAll(hyperSet);
  }

  @Test
  public void testVarWidth() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    SingleRowSet rowSet1;
    {
      ExtendableRowSet rowSet = fixture.rowSet(schema);
      RowSetWriter writer = rowSet.writer();
      writer.scalar(0).setString("second");
      writer.save();
      writer.scalar(0).setString("fourth");
      writer.save();
      rowSet1 = writer.done();
    }

    SingleRowSet rowSet2;
    {
      ExtendableRowSet rowSet = fixture.rowSet(schema);
      RowSetWriter writer = rowSet.writer();
      writer.scalar(0).setString("first");
      writer.save();
      writer.scalar(0).setString("third");
      writer.save();
      rowSet2 = writer.done();
    }

    // Build the hyper batch

    HyperRowSet hyperSet = HyperRowSetImpl.fromRowSets(fixture.allocator(), rowSet1, rowSet2);
    assertEquals(4, hyperSet.rowCount());
    @SuppressWarnings("resource")
    SelectionVector4 sv4 = hyperSet.getSv4();
    sv4.set(0, 1, 0);
    sv4.set(1, 0, 0);
    sv4.set(2, 1, 1);
    sv4.set(3, 0, 1);

    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addRow("first")
        .addRow("second")
        .addRow("third")
        .addRow("fourth")
        .build();

    new RowSetComparison(expected)
      .verifyAndClearAll(hyperSet);
  }

  @Test
  public void testOptional() {
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .buildSchema();

    SingleRowSet rowSet1;
    {
      ExtendableRowSet rowSet = fixture.rowSet(schema);
      RowSetWriter writer = rowSet.writer();
      writer.scalar(0).setString("sixth");
      writer.save();
      writer.scalar(0).setNull();
      writer.save();
      writer.scalar(0).setString("fourth");
      writer.save();
      rowSet1 = writer.done();
    }

    SingleRowSet rowSet2;
    {
      ExtendableRowSet rowSet = fixture.rowSet(schema);
      RowSetWriter writer = rowSet.writer();
      writer.scalar(0).setNull();
      writer.save();
      writer.scalar(0).setString("first");
      writer.save();
      writer.scalar(0).setString("third");
      writer.save();
      rowSet2 = writer.done();
    }

    // Build the hyper batch

    HyperRowSet hyperSet = HyperRowSetImpl.fromRowSets(fixture.allocator(), rowSet1, rowSet2);
    assertEquals(6, hyperSet.rowCount());
    @SuppressWarnings("resource")
    SelectionVector4 sv4 = hyperSet.getSv4();
    sv4.set(0, 1, 1);
    sv4.set(1, 0, 1);
    sv4.set(2, 1, 2);
    sv4.set(3, 0, 2);
    sv4.set(4, 1, 0);
    sv4.set(5, 0, 0);

    SingleRowSet expected = fixture.rowSetBuilder(schema)
        .addSingleCol("first")
        .addSingleCol(null)
        .addSingleCol("third")
        .addSingleCol("fourth")
        .addSingleCol(null)
        .addSingleCol("sixth")
        .build();

    new RowSetComparison(expected)
      .verifyAndClearAll(hyperSet);
  }

  @Test
  public void testRepeated() {
  }

  @Test
  public void testMap() {
  }

  @Test
  public void testRepeatedMap() {
  }

  @Test
  public void testUnion() {
  }

  @Test
  public void testList() {
  }

}
