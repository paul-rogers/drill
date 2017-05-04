package org.apache.drill.exec.physical.rowSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.Exp.RowSetMutator;
import org.apache.drill.exec.physical.rowSet.Exp.RowSetMutatorImpl;
import org.apache.drill.exec.physical.rowSet.Exp.TupleLoader;
import org.apache.drill.exec.physical.rowSet.Exp.TupleSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.Test;

public class LoaderTest extends SubOperatorTest {

  @Test
  public void testBasics() {
    RowSetMutator rsMutator = new RowSetMutatorImpl(fixture.allocator());
    assertEquals(-1, rsMutator.schemaVersion());
    assertEquals(ValueVector.MAX_ROW_COUNT, rsMutator.targetRowCount());
    assertEquals(ValueVector.MAX_BUFFER_SIZE, rsMutator.targetVectorSize());
    assertFalse(rsMutator.isFull());
    assertEquals(0, rsMutator.rowCount());

    TupleLoader rootWriter = rsMutator.writer();
    TupleSchema schema = rootWriter.schema();
    assertEquals(0, schema.columnCount());

    MaterializedField fieldA = MaterializedField.create("a",
        MajorType.newBuilder()
          .setMinorType(MinorType.INT)
          .setMode(DataMode.REQUIRED)
          .build());
    schema.addColumn(fieldA);

    assertEquals(0, rsMutator.schemaVersion());
    assertEquals(1, schema.columnCount());
    assertSame(fieldA, schema.column(0));
    assertSame(fieldA, schema.column("a"));

    rootWriter.column(0).setInt(100);
    assertEquals(0, rsMutator.rowCount());
    rsMutator.save();
    assertEquals(1, rsMutator.rowCount());

    MaterializedField fieldB = MaterializedField.create("b",
        MajorType.newBuilder()
          .setMinorType(MinorType.INT)
          .setMode(DataMode.OPTIONAL)
          .build());
    schema.addColumn(fieldB);

    assertEquals(1, rsMutator.schemaVersion());
    assertEquals(2, schema.columnCount());
    assertSame(fieldB, schema.column(1));
    assertSame(fieldB, schema.column("b"));

    rootWriter.column(0).setInt(200);
    rootWriter.column(1).setInt(210);
    rsMutator.save();
    assertEquals(2, rsMutator.rowCount());

    RowSet result = fixture.wrap(rsMutator.harvest());
    SingleRowSet expected = fixture.rowSetBuilder(result.batchSchema())
        .add(100, null)
        .add(200, 210)
        .build();
    new RowSetComparison(expected)
      .verifyAndClear(result);
  }

  // Test case sensitive and case insensitive
  // Test name collisions: case insensitive
  // Test name collisions: case sensitive
  // Test name aliases: "a" and "A".

}
