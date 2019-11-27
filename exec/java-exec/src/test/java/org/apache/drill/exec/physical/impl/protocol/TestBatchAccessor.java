package org.apache.drill.exec.physical.impl.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSet.SingleRowSet;
import org.apache.drill.exec.proto.UserBitShared.NamePart;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

public class TestBatchAccessor extends SubOperatorTest {

  @Test
  public void testBatchAccessor() {
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();
    RowSet rs = fixture.rowSetBuilder(schema)
          .addRow(10, "fred")
          .addRow(20, "wilma")
          .build();
    SingleOutgoingContainerAccessor accessor = new SingleOutgoingContainerAccessor(rs.container());
    accessor.registerBatch();

    assertEquals(rs.container().getSchema(), accessor.schema());
    assertEquals(2, accessor.rowCount());
    assertSame(rs.container(), accessor.container());

    Iterator<VectorWrapper<?>> iter = accessor.iterator();
    assertEquals("a", iter.next().getValueVector().getField().getName());
    assertEquals("b", iter.next().getValueVector().getField().getName());

    // Not a full test of the schema path; just make sure that the
    // pass-through to the Vector Container works.

    SchemaPath path = SchemaPath.create(NamePart.newBuilder().setName("a").build());
    TypedFieldId id = accessor.getValueVectorId(path);
    assertEquals(MinorType.INT, id.getFinalType().getMinorType());
    assertEquals(1, id.getFieldIds().length);
    assertEquals(0, id.getFieldIds()[0]);

    path = SchemaPath.create(NamePart.newBuilder().setName("b").build());
    id = accessor.getValueVectorId(path);
    assertEquals(MinorType.VARCHAR, id.getFinalType().getMinorType());
    assertEquals(1, id.getFieldIds().length);
    assertEquals(1, id.getFieldIds()[0]);

    // Sanity check of getValueAccessorById()

    VectorWrapper<?> w = accessor.getValueAccessorById(IntVector.class, 0);
    assertNotNull(w);
    assertEquals("a", w.getValueVector().getField().getName());
    w = accessor.getValueAccessorById(VarCharVector.class, 1);
    assertNotNull(w);
    assertEquals("b", w.getValueVector().getField().getName());

    // getWritableBatch() ?

    // No selection vectors

    try {
      accessor.selectionVector2();
      fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      accessor.selectionVector4();
      fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    rs.clear();
  }

  @Test
  public void testSchemaChange() {
    SerialOutgoingContainerAccessor accessor = new SerialOutgoingContainerAccessor();
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.VARCHAR)
        .buildSchema();
    SingleRowSet rs = fixture.rowSetBuilder(schema)
        .addRow(10, "fred")
        .addRow(20, "wilma")
        .build();
    VectorContainer container = rs.container();
    accessor.registerBatch(container);
    int schemaVersion = accessor.schemaVersion();

    // Be tidy: start at 1.

    assertEquals(1, schemaVersion);

    // Changing data does not trigger schema change

    container.zeroVectors();
    accessor.registerBatch(container);
    assertEquals(schemaVersion, accessor.schemaVersion());

    // Different container, same vectors, does not trigger a change

    VectorContainer c2 = new VectorContainer(fixture.allocator());
    for (VectorWrapper<?> vw : container) {
      c2.add(vw.getValueVector());
    }
    c2.buildSchema(SelectionVectorMode.NONE);
    accessor.registerBatch(c2);
    assertEquals(schemaVersion, accessor.schemaVersion());

    accessor.registerBatch(container);
    assertEquals(schemaVersion, accessor.schemaVersion());

    // Replacing a vector with another of the same type does trigger
    // a change.

    VectorContainer c3 = new VectorContainer(fixture.allocator());
    c3.add(container.getValueVector(0).getValueVector());
    c3.add(TypeHelper.getNewVector(
            container.getValueVector(1).getValueVector().getField(),
            fixture.allocator(), null));
    c3.buildSchema(SelectionVectorMode.NONE);
    accessor.registerBatch(c3);
    assertEquals(schemaVersion + 1, accessor.schemaVersion());
    schemaVersion = accessor.schemaVersion();

    // No change if same schema again

    accessor.registerBatch(c3);
    assertEquals(schemaVersion, accessor.schemaVersion());

    // Adding a vector triggers a change

    MaterializedField c = SchemaBuilder.columnSchema("c", MinorType.INT, DataMode.OPTIONAL);
    c3.add(TypeHelper.getNewVector(c, fixture.allocator(), null));
    c3.buildSchema(SelectionVectorMode.NONE);
    accessor.registerBatch(c3);
    assertEquals(schemaVersion + 1, accessor.schemaVersion());
    schemaVersion = accessor.schemaVersion();

    // No change if same schema again

    accessor.registerBatch(c3);
    assertEquals(schemaVersion, accessor.schemaVersion());

    // Removing a vector triggers a change

    c3.remove(c3.getValueVector(2).getValueVector());
    c3.buildSchema(SelectionVectorMode.NONE);
    assertEquals(2, c3.getNumberOfColumns());
    accessor.registerBatch(c3);
    assertEquals(schemaVersion + 1, accessor.schemaVersion());
    schemaVersion = accessor.schemaVersion();

    // Clean up

    container.clear();
    c2.clear();
    c3.clear();
  }
}
