package org.apache.drill.vector;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestToNullable extends SubOperatorTest {

  @SuppressWarnings("resource")
  @Test
  public void testFixedWidth() {
    MaterializedField intSchema =
        SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    IntVector intVector = new IntVector(intSchema, fixture.allocator());
    IntVector.Mutator intMutator = intVector.getMutator();
    intVector.allocateNew(100);
    for (int i = 0; i < 100; i++) {
      intMutator.set(i, i * 10);
    }
    intMutator.setValueCount(100);

    MaterializedField nullableIntSchema =
        SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.OPTIONAL);
    NullableIntVector nullableIntVector = new NullableIntVector(nullableIntSchema, fixture.allocator());

    intVector.toNullable(nullableIntVector);

    assertEquals(0, intVector.getAccessor().getValueCount());
    NullableIntVector.Accessor niAccessor = nullableIntVector.getAccessor();
    assertEquals(100, niAccessor.getValueCount());
    for (int i = 0; i < 100; i++) {
      assertFalse(niAccessor.isNull(i));
      assertEquals(i * 10, niAccessor.get(i));
    }

    nullableIntVector.clear();

    // Don't clear the intVector, it should be empty.
    // If it is not, the test will fail with a memory leak error.
  }

  @SuppressWarnings("resource")
  @Test
  public void testNullable() {
    MaterializedField nullableIntSchema =
        SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.OPTIONAL);
    NullableIntVector sourceVector = new NullableIntVector(nullableIntSchema, fixture.allocator());
    NullableIntVector.Mutator sourceMutator = sourceVector.getMutator();
    sourceVector.allocateNew(100);
    for (int i = 0; i < 100; i++) {
      sourceMutator.set(i, i * 10);
    }
    sourceMutator.setValueCount(100);

    NullableIntVector destVector = new NullableIntVector(nullableIntSchema, fixture.allocator());

    sourceVector.toNullable(destVector);

    assertEquals(0, sourceVector.getAccessor().getValueCount());
    NullableIntVector.Accessor destAccessor = destVector.getAccessor();
    assertEquals(100, destAccessor.getValueCount());
    for (int i = 0; i < 100; i++) {
      assertFalse(destAccessor.isNull(i));
      assertEquals(i * 10, destAccessor.get(i));
    }

    destVector.clear();

    // Don't clear the intVector, it should be empty.
    // If it is not, the test will fail with a memory leak error.
  }

}
