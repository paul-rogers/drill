package org.apache.drill.test.rowSet.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.StructureType;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.base.Splitter;

public class TestSchema extends SubOperatorTest {

  /**
   * Test a simple physical schema with no maps.
   */

  @Test
  public void testSchema() {
    TupleMetadata tupleSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .add("a", MinorType.INT, DataMode.REPEATED)
        .addNullable("b", MinorType.VARCHAR)
        .buildSchema();

    assertEquals(3, tupleSchema.size());
    assertEquals("c", tupleSchema.column(0).getName());
    assertEquals("a", tupleSchema.column(1).getName());
    assertEquals("b", tupleSchema.column(2).getName());

    // No maps. Flat schema is the same as tuple schema.

    TupleMetadata flatSchema = tupleSchema.flatten();
    assertEquals(3, flatSchema.size());

    crossCheck(flatSchema, 0, "c", MinorType.INT);
    assertEquals(DataMode.REQUIRED, flatSchema.column(0).getDataMode());
    assertEquals(DataMode.REQUIRED, flatSchema.column(0).getType().getMode());
    assertTrue(! flatSchema.column(0).isNullable());

    crossCheck(flatSchema, 1, "a", MinorType.INT);
    assertEquals(DataMode.REPEATED, flatSchema.column(1).getDataMode());
    assertEquals(DataMode.REPEATED, flatSchema.column(1).getType().getMode());
    assertTrue(! flatSchema.column(1).isNullable());

    crossCheck(flatSchema, 2, "b", MinorType.VARCHAR);
    assertEquals(MinorType.VARCHAR, flatSchema.column(2).getType().getMinorType());
    assertEquals(DataMode.OPTIONAL, flatSchema.column(2).getDataMode());
    assertEquals(DataMode.OPTIONAL, flatSchema.column(2).getType().getMode());
    assertTrue(flatSchema.column(2).isNullable());

    // Verify batch schema

    BatchSchema batchSchema = new BatchSchema(SelectionVectorMode.NONE, tupleSchema.toFieldList());
    assertEquals(3, batchSchema.getFieldCount());
    assertSame(batchSchema.getColumn(0), tupleSchema.column(0));
    assertSame(batchSchema.getColumn(1), tupleSchema.column(1));
    assertSame(batchSchema.getColumn(2), tupleSchema.column(2));
  }

  /**
   * Validate that the actual column metadata is as expected by
   * cross-checking: validate that the column at the index and
   * the column at the column name are both correct.
   *
   * @param schema the schema for the row set
   * @param index column index
   * @param fullName expected column name
   * @param type expected type
   */

  public void crossCheck(TupleMetadata schema, int index, String fullName, MinorType type) {
    String name = null;
    for (String part : Splitter.on(".").split(fullName)) {
      name = part;
    }
    assertEquals(name, schema.column(index).getName());
    assertEquals(index, schema.index(fullName));
    assertSame(schema.column(index), schema.column(fullName));
    assertEquals(type, schema.column(index).getType().getMinorType());
  }


  /**
   * Verify that a nested map schema.
   * Schema has non-repeated maps, so can be flattened.
   */

  @Test
  public void testMapSchema() {
    TupleMetadata tupleSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .addMap("a")
          .addNullable("b", MinorType.VARCHAR)
          .add("d", MinorType.INT)
          .addMap("e")
            .add("f", MinorType.VARCHAR)
            .buildMap()
          .add("g", MinorType.INT)
          .buildMap()
        .add("h", MinorType.BIGINT)
        .buildSchema();

    assertEquals(3, tupleSchema.size());
    assertEquals("c", tupleSchema.metadata(0).name());
    assertEquals("c", tupleSchema.metadata(0).fullName());
    assertEquals(StructureType.PRIMITIVE, tupleSchema.metadata(0).structureType());
    assertNull(tupleSchema.metadata(0).mapSchema());

    assertEquals("a", tupleSchema.metadata(1).name());
    assertEquals("a", tupleSchema.metadata(1).fullName());
    assertEquals(StructureType.TUPLE, tupleSchema.metadata(1).structureType());
    assertNotNull(tupleSchema.metadata(1).mapSchema());

    assertEquals("h", tupleSchema.metadata(2).name());
    assertEquals("h", tupleSchema.metadata(2).fullName());
    assertEquals(StructureType.PRIMITIVE, tupleSchema.metadata(2).structureType());
    assertNull(tupleSchema.metadata(2).mapSchema());

    TupleMetadata aSchema = tupleSchema.metadata(1).mapSchema();
    assertEquals(4, aSchema.size());
    assertEquals("b", aSchema.metadata(0).name());
    assertEquals("a.b", aSchema.metadata(0).fullName());
    assertEquals("d", aSchema.metadata(1).name());
    assertEquals("e", aSchema.metadata(2).name());
    assertEquals("g", aSchema.metadata(3).name());

    TupleMetadata eSchema = aSchema.metadata(2).mapSchema();
    assertEquals(1, eSchema.size());
    assertEquals("f", eSchema.metadata(0).name());
    assertEquals("a.e.f", eSchema.metadata(0).fullName());

    // Flattened with maps removed. This is for testing use only
    // as it is ambiguous in production.

    TupleMetadata flatSchema = tupleSchema.flatten();
    assertEquals(6, flatSchema.size());
    crossCheck(flatSchema, 0, "c", MinorType.INT);
    crossCheck(flatSchema, 1, "a.b", MinorType.VARCHAR);
    crossCheck(flatSchema, 2, "a.d", MinorType.INT);
    crossCheck(flatSchema, 3, "a.e.f", MinorType.VARCHAR);
    crossCheck(flatSchema, 4, "a.g", MinorType.INT);
    crossCheck(flatSchema, 5, "h", MinorType.BIGINT);

    // Verify batch schema: should mirror the schema created above.

    BatchSchema batchSchema = new BatchSchema(SelectionVectorMode.NONE, tupleSchema.toFieldList());
    assertEquals(3, batchSchema.getFieldCount());
    assertSame(tupleSchema.column(0), batchSchema.getColumn(0));
    assertSame(tupleSchema.column(2), batchSchema.getColumn(2));

    assertEquals("a", batchSchema.getColumn(1).getName());
    assertEquals(MinorType.MAP, batchSchema.getColumn(1).getType().getMinorType());
    assertNotNull(batchSchema.getColumn(1).getChildren());

    List<MaterializedField> aMap = new ArrayList<>();
    for (MaterializedField field : batchSchema.getColumn(1).getChildren()) {
      aMap.add(field);
    }
    assertEquals(4, aMap.size());
    assertSame(aMap.get(0), aSchema.column(0));
    assertSame(aMap.get(1), aSchema.column(1));
    assertSame(aMap.get(2), aSchema.column(2));
    assertSame(aMap.get(3), aSchema.column(3));

    List<MaterializedField> eMap = new ArrayList<>();
    for (MaterializedField field : aMap.get(2).getChildren()) {
      aMap.add(field);
    }
    assertEquals(1, eMap.size());
    assertSame(eSchema.column(0), eMap.get(0));
  }

}
