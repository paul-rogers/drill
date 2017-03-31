package org.apache.drill.test.rowSet.test;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ComplexAccessorTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void test() {
    RowSetWriter writer = null;

    // Write one row


  }

  @Test
  public void TestTopArray() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .addArray("a", MinorType.INT)
        .build();

    RowSetSchema schema = new RowSetSchema(batchSchema);

    RowSetWriter writer = null;
    TupleWriter row = writer.row();
    row.column(0).setInt(10);
    ArrayWriter array = row.column(1).array();
    array.setInt(100);
    array.setInt(110);
    writer.save();
    writer.done();

    SingleRowSet rowSet2 = new RowSetBuilder(batchSchema)
      .add(20, new int[] {200, 210})
      .build();

  }

}
