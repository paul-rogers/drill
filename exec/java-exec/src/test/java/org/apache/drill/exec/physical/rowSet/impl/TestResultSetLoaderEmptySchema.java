package org.apache.drill.exec.physical.rowSet.impl;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestResultSetLoaderEmptySchema extends SubOperatorTest {

  /**
   * Verify that empty projection works: allows skipping rows and
   * reporting those rows as a batch with no vectors but with the
   * desired row count.
   */

  @Test
  public void testEmptyTopSchema() {
    List<SchemaPath> selection = Lists.newArrayList();
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(selection)
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    assertTrue(rsLoader.isProjectionEmpty());

    // Can't skip rows if batch not started.

    int rowCount = 100_000;
    try {
      rsLoader.skipRows(10);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Loop to skip 100,000 rows. Should occur in two batches.

    rsLoader.startBatch();
    int skipped = rsLoader.skipRows(rowCount);
    assertEquals(skipped, ValueVector.MAX_ROW_COUNT);

    VectorContainer output = rsLoader.harvest();
    assertEquals(skipped, output.getRecordCount());
    assertEquals(0, output.getNumberOfColumns());
    output.zeroVectors();

    // Second batch

    rowCount -= skipped;
    rsLoader.startBatch();
    skipped = rsLoader.skipRows(rowCount);
    assertEquals(skipped, rowCount);

    output = rsLoader.harvest();
    assertEquals(skipped, output.getRecordCount());
    assertEquals(0, output.getNumberOfColumns());
    output.zeroVectors();

    rsLoader.close();
  }

  /**
   * Verify that a disjoint schema (projection does not overlap with
   * table schema) is treated the same as an empty projection.
   */

  @Test
  public void testDisjointSchema() {
    List<SchemaPath> selection = Lists.newArrayList(
        SchemaPath.getSimplePath("a"),
        SchemaPath.getSimplePath("b"));
    TupleMetadata schema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .add("d", MinorType.INT)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(selection)
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    assertTrue(rsLoader.isProjectionEmpty());
    rsLoader.close();
  }

  /**
   * Verify that skip rows is disallowed if the the projection is non-empty.
   */

  @Test
  public void testNonEmptySchema() {
    List<SchemaPath> selection = Lists.newArrayList(
        SchemaPath.getSimplePath("a"),
        SchemaPath.getSimplePath("b"));
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("d", MinorType.INT)
        .buildSchema();
    ResultSetOptions options = new OptionBuilder()
        .setProjection(selection)
        .setSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    assertFalse(rsLoader.isProjectionEmpty());
    rsLoader.startBatch();
    try {
      rsLoader.skipRows(10);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    rsLoader.close();
  }

  @Test
  public void testEmptyMapProjection() {

  }

  @Test
  public void testNonEmptyMapProjection() {

  }

}
