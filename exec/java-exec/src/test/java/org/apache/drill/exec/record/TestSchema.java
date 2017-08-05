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
package org.apache.drill.exec.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleMetadata.StructureType;
import org.apache.drill.exec.record.TupleSchema.AbstractColumnMetadata;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

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
    assertFalse(tupleSchema.isEmpty());

    assertEquals("c", tupleSchema.column(0).getName());
    assertEquals("a", tupleSchema.column(1).getName());
    assertEquals("b", tupleSchema.column(2).getName());

    ColumnMetadata md0 = tupleSchema.metadata(0);
    assertEquals(StructureType.PRIMITIVE, md0.structureType());
    assertNull(md0.mapSchema());
    assertEquals(0, md0.index());
    assertSame(md0.schema(), tupleSchema.column(0));
    assertEquals(md0.name(), tupleSchema.column(0).getName());
    assertEquals(MinorType.INT, md0.type());
    assertEquals(DataMode.REQUIRED, md0.mode());
    assertFalse(md0.isVariableWidth());
    assertFalse(md0.isArray());
    assertFalse(md0.isMap());
    assertSame(tupleSchema, md0.parent());
    assertEquals(md0.name(), md0.fullName());
    assertTrue(md0.isEquivalent(md0));
    assertFalse(md0.isEquivalent(tupleSchema.metadata(1)));

    assertEquals(1, tupleSchema.metadata(1).index());
    assertEquals(DataMode.REPEATED, tupleSchema.metadata(1).mode());
    assertFalse(tupleSchema.metadata(1).isVariableWidth());
    assertTrue(tupleSchema.metadata(1).isArray());
    assertFalse(tupleSchema.metadata(1).isMap());

    assertEquals(2, tupleSchema.metadata(2).index());
    assertEquals(DataMode.OPTIONAL, tupleSchema.metadata(2).mode());
    assertTrue(tupleSchema.metadata(2).isVariableWidth());
    assertFalse(tupleSchema.metadata(2).isArray());
    assertFalse(tupleSchema.metadata(2).isMap());

    assertSame(tupleSchema.column(0), tupleSchema.column("c"));
    assertSame(tupleSchema.column(1), tupleSchema.column("a"));
    assertSame(tupleSchema.column(2), tupleSchema.column("b"));

    assertSame(tupleSchema.metadata(0), tupleSchema.metadata("c"));
    assertSame(tupleSchema.metadata(1), tupleSchema.metadata("a"));
    assertSame(tupleSchema.metadata(2), tupleSchema.metadata("b"));
    assertEquals(0, tupleSchema.index("c"));
    assertEquals(1, tupleSchema.index("a"));
    assertEquals(2, tupleSchema.index("b"));

    // Test undefined column

    assertEquals(-1, tupleSchema.index("x"));
    assertNull(tupleSchema.metadata("x"));
    assertNull(tupleSchema.column("x"));

    try {
      tupleSchema.metadata(4);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }

    try {
      tupleSchema.column(4);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }

    // Verify batch schema
    // Tests toFieldList() internally

    BatchSchema batchSchema = new BatchSchema(SelectionVectorMode.NONE, tupleSchema.toFieldList());
    assertEquals(3, batchSchema.getFieldCount());
    assertSame(batchSchema.getColumn(0), tupleSchema.column(0));
    assertSame(batchSchema.getColumn(1), tupleSchema.column(1));
    assertSame(batchSchema.getColumn(2), tupleSchema.column(2));
  }

  @Test
  public void testEmptySchema() {
    TupleMetadata tupleSchema = new SchemaBuilder()
        .buildSchema();

    assertEquals(0, tupleSchema.size());
    assertTrue(tupleSchema.isEmpty());
  }

  @Test
  public void testDuplicateName() {
    try {
      new SchemaBuilder()
          .add("foo", MinorType.INT)
          .add("a", MinorType.INT, DataMode.REPEATED)
          .addNullable("foo", MinorType.VARCHAR)
          .buildSchema();
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains("foo"));
    }
  }

  @Test
  public void testSVMode() {
    BatchSchema batchSchema = new SchemaBuilder()
        .add("c", MinorType.INT)
        .add("a", MinorType.INT, DataMode.REPEATED)
        .addNullable("b", MinorType.VARCHAR)
        .withSVMode(SelectionVectorMode.TWO_BYTE)
        .build();

    assertEquals(3, batchSchema.getFieldCount());
    assertEquals(SelectionVectorMode.TWO_BYTE, batchSchema.getSelectionVectorMode());
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
          .addArray("g", MinorType.INT)
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
    assertTrue(tupleSchema.metadata(1).isMap());

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
    assertTrue(aSchema.metadata(3).isArray());

    TupleMetadata eSchema = aSchema.metadata(2).mapSchema();
    assertEquals(1, eSchema.size());
    assertEquals("f", eSchema.metadata(0).name());
    assertEquals("a.e.f", eSchema.metadata(0).fullName());

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
      eMap.add(field);
    }
    assertEquals(1, eMap.size());
    assertSame(eSchema.column(0), eMap.get(0));
  }

  @Test
  public void testSchemaEvolution() {
    TupleSchema schema = new TupleSchema();
    assertEquals(0, schema.size());

    // Add top-level fields.

    MaterializedField aField = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    ColumnMetadata aSchema = schema.add(aField);
    assertNotNull(aSchema);
    assertEquals(0, aSchema.index());
    assertEquals("a", aSchema.name());
    assertEquals(1, schema.size());
    assertSame(aSchema, schema.metadata(0));
    assertSame(aField, schema.column(0));
    assertSame(aField, schema.column("a"));

    MaterializedField bField = SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED);
    AbstractColumnMetadata bSchema = TupleSchema.fromField(bField);
    schema.add(bSchema);
    assertEquals(1, bSchema.index());
    assertEquals(2, schema.size());
    assertSame(bSchema, schema.metadata(1));
    assertSame(bField, schema.column(1));
    assertSame(bField, schema.column("b"));

    // Create a map field

    MaterializedField cField = SchemaBuilder.columnSchema("c", MinorType.MAP, DataMode.REQUIRED);
    ColumnMetadata cSchema = schema.add(cField);
    assertEquals(2, cSchema.index());
    assertEquals(3, schema.size());

    // Get the map schema

    TupleSchema cMap = (TupleSchema) cSchema.mapSchema();
    assertNotNull(cMap);
    assertEquals(0, cMap.size());

    // Add columns to the map repeating the names at the top level.
    // Name spaces are independent.

    MaterializedField caField = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    ColumnMetadata caSchema = cMap.add(caField);
    assertNotNull(caSchema);
    assertEquals(0, caSchema.index());
    assertEquals("a", caSchema.name());
    assertEquals(1, cMap.size());
    assertSame(caSchema, cMap.metadata(0));
    assertSame(caField, cMap.column(0));
    assertSame(caField, cMap.column("a"));

    MaterializedField cbField = SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED);
    AbstractColumnMetadata cbSchema = TupleSchema.fromField(cbField);
    cMap.add(cbSchema);
    assertEquals(1, cbSchema.index());
    assertEquals(2, cMap.size());
    assertSame(cbSchema, cMap.metadata(1));
    assertSame(cbField, cMap.column(1));
    assertSame(cbField, cMap.column("b"));

    // Map fields went into the map schema.

    assertEquals(3, schema.size());
  }

  @Test
  public void testAllocationMetadata() {
    TupleSchema schema = new TupleSchema();

    MaterializedField aField = SchemaBuilder.columnSchema("a", MinorType.INT, DataMode.REQUIRED);
    ColumnMetadata aSchema = schema.add(aField);

    // Test allocation metadata. Don't bother with width; changing the width
    // for a fixed-width column is allowed, but undefined.

    assertEquals(IntVector.VALUE_WIDTH, aSchema.expectedWidth());
    assertEquals(1, aSchema.expectedElementCount());
    aSchema.setExpectedElementCount(5);
    assertEquals(1, aSchema.expectedElementCount());
    aSchema.setExpectedElementCount(-1);
    aSchema.setExpectedElementCount(1);

    MaterializedField a2Field = SchemaBuilder.columnSchema("a2", MinorType.INT, DataMode.REPEATED);
    ColumnMetadata a2Schema = schema.add(a2Field);

    // Test allocation metadata. Don't bother with width; changing the width
    // for a fixed-width column is allowed, but undefined.

    assertEquals(IntVector.VALUE_WIDTH, a2Schema.expectedWidth());
    assertEquals(ColumnMetadata.DEFAULT_ARRAY_SIZE, a2Schema.expectedElementCount());
    a2Schema.setExpectedElementCount(5);
    assertEquals(5, a2Schema.expectedElementCount());
    a2Schema.setExpectedElementCount(-1);
    a2Schema.setExpectedElementCount(1);

    MaterializedField bField = SchemaBuilder.columnSchema("b", MinorType.VARCHAR, DataMode.REQUIRED);
    ColumnMetadata bSchema = schema.add(bField);

    // Test variable width allocation metadata.

    assertEquals(54, bSchema.expectedWidth());
    assertEquals(1, bSchema.expectedElementCount());
    bSchema.setExpectedWidth(100);
    assertEquals(100, bSchema.expectedWidth());
    bSchema.setExpectedElementCount(0);
    assertEquals(1, bSchema.expectedElementCount());

    // Specify a width in the field schema

    MaterializedField cField = new SchemaBuilder.ColumnBuilder("c", MinorType.VARCHAR)
        .setWidth(20)
        .build();
    ColumnMetadata cSchema = schema.add(cField);
    assertEquals(20, cSchema.expectedWidth());

    MaterializedField dField = SchemaBuilder.columnSchema("d", MinorType.MAP, DataMode.REQUIRED);
    ColumnMetadata dSchema = schema.add(dField);
    assertEquals(0, dSchema.expectedWidth());
    assertEquals(1, dSchema.expectedElementCount());

    MaterializedField d2Field = SchemaBuilder.columnSchema("d2", MinorType.MAP, DataMode.REPEATED);
    ColumnMetadata d2Schema = schema.add(d2Field);
    assertEquals(0, d2Schema.expectedWidth());
    assertEquals(ColumnMetadata.DEFAULT_ARRAY_SIZE, d2Schema.expectedElementCount());
  }

  @Test
  public void testEquivalenceIdentity() {
    BatchSchema schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addArray("b", MinorType.VARCHAR)
        .build();

    TupleSchema tmd = TupleSchema.fromFields(schema);
    BatchSchema schema2 = tmd.toBatchSchema(SelectionVectorMode.NONE);
    assertTrue(schema.isEquivalent(schema2));
    TupleSchema tmd2 = TupleSchema.fromFields(schema2);
    assertTrue(tmd.isEquivalent(tmd2));

    BatchSchema schema3 = new SchemaBuilder()
        .addNullable("a", MinorType.INT) // Different
        .addArray("b", MinorType.VARCHAR)
        .build();
    TupleSchema tmd3 = TupleSchema.fromFields(schema3);
    assertFalse(tmd3.isEquivalent(tmd));
  }

  @Test
  public void testEquivalenceSimilarity() {
    BatchSchema schema1 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addArray("b", MinorType.VARCHAR)
        .addMap("C")
          .add("c1", MinorType.INT)
          .buildMap()
        .build();
    BatchSchema schema2 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addArray("b", MinorType.VARCHAR)
        .addMap("C")
          .add("c1", MinorType.INT)
          .buildMap()
        .build();

    TupleSchema tmd1 = TupleSchema.fromFields(schema1);
    TupleSchema tmd2 = TupleSchema.fromFields(schema2);
    assertTrue(tmd1.isEquivalent(tmd2));
  }

  @Test
  public void testEquivalenceMapDifference() {
    BatchSchema schema1 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addArray("b", MinorType.VARCHAR)
        .addMap("C")
          .add("c1", MinorType.INT)
          .buildMap()
        .build();
    BatchSchema schema2 = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addArray("b", MinorType.VARCHAR)
        .addMap("C")
          .addNullable("c1", MinorType.INT) // Different
          .buildMap()
        .build();

    TupleSchema tmd1 = TupleSchema.fromFields(schema1);
    TupleSchema tmd2 = TupleSchema.fromFields(schema2);
    assertFalse(tmd1.isEquivalent(tmd2));
  }

}
