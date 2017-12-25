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
package org.apache.drill.test.rowSet.test;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.ColumnMetadata.StructureType;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

public class TestRepeatedList extends SubOperatorTest {

  /**
   * Test the intermediate case in which a repeated list
   * does not yet have child type.
   */

  @Test
  public void testSchemaIncompleteBatch() {
    BatchSchema schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .build()
        .build();

    assertEquals(2, schema.getFieldCount());
    MaterializedField list = schema.getColumn(1);
    assertEquals("list2", list.getName());
    assertEquals(MinorType.LIST, list.getType().getMinorType());
    assertEquals(DataMode.REPEATED, list.getType().getMode());
    assertTrue(list.getChildren().isEmpty());
  }

  @Test
  public void testSchemaIncompleteMetadata() {
    BatchSchema schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .build()
        .build();

    assertEquals(2, schema.getFieldCount());
    MaterializedField list = schema.getColumn(1);
    assertEquals("list2", list.getName());
    assertEquals(MinorType.LIST, list.getType().getMinorType());
    assertEquals(DataMode.REPEATED, list.getType().getMode());
    assertTrue(list.getChildren().isEmpty());
  }

  /**
   * Test the case of a simple 2D array. Drill represents
   * this as two levels of materialized fields.
   */

  @Test
  public void testSchema2DBatch() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .addArray(MinorType.VARCHAR)
          .build()
        .buildSchema();

    assertEquals(2, schema.size());
    ColumnMetadata list = schema.metadata(1);
    assertEquals("list2", list.name());
    assertEquals(MinorType.LIST, list.type());
    assertEquals(DataMode.REPEATED, list.mode());
    assertEquals(StructureType.MULTI_ARRAY, list.structureType());
    assertTrue(list.isArray());
    assertEquals(-1, list.dimensions());
    assertNull(list.childSchema());
  }

  /**
   * Test a 2D array using metadata. The metadata also uses
   * a column per dimension as that provides the easiest mapping
   * to the nested fields. A better design might be a single level
   * (as in repeated fields), but with a single attribute that
   * describes the number of dimensions. The <tt>dimensions()</tt>
   * method is a compromise.
   */

  @Test
  public void testSchema2DMetadata() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .addArray(MinorType.VARCHAR)
          .build()
        .buildSchema();

    assertEquals(2, schema.size());
    ColumnMetadata list = schema.metadata(1);
    assertEquals("list2", list.name());
    assertEquals(MinorType.LIST, list.type());
    assertEquals(DataMode.REPEATED, list.mode());
    assertEquals(StructureType.MULTI_ARRAY, list.structureType());
    assertTrue(list.isArray());
    assertEquals(2, list.dimensions());
    assertNotNull(list.childSchema());

    ColumnMetadata child = list.childSchema();
    assertEquals("list2", child.name());
    assertEquals(MinorType.VARCHAR, child.type());
    assertEquals(DataMode.REPEATED, child.mode());
    assertTrue(child.isArray());
    assertEquals(1, child.dimensions());
    assertNull(child.childSchema());
  }

  @Test
  public void testSchema3DBatch() {
    BatchSchema schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .addDimension()
            .addArray(MinorType.VARCHAR)
            .endDimension()
          .build()
        .build();

    assertEquals(2, schema.getFieldCount());
    MaterializedField list = schema.getColumn(1);
    assertEquals("list2", list.getName());
    assertEquals(MinorType.LIST, list.getType().getMinorType());
    assertEquals(DataMode.REPEATED, list.getType().getMode());
    assertEquals(1, list.getChildren().size());

    MaterializedField child1 = list.getChildren().iterator().next();
    assertEquals("list2", child1.getName());
    assertEquals(MinorType.LIST, child1.getType().getMinorType());
    assertEquals(DataMode.REPEATED, child1.getType().getMode());
    assertEquals(1, child1.getChildren().size());

    MaterializedField child2 = child1.getChildren().iterator().next();
    assertEquals("list2", child2.getName());
    assertEquals(MinorType.VARCHAR, child2.getType().getMinorType());
    assertEquals(DataMode.REPEATED, child2.getType().getMode());
    assertEquals(0, child2.getChildren().size());
  }

  @Test
  public void testSchema3DMetadata() {
    TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .addRepeatedList("list2")
          .addDimension()
            .addArray(MinorType.VARCHAR)
            .endDimension()
          .build()
        .buildSchema();

    assertEquals(2, schema.size());
    ColumnMetadata list = schema.metadata(1);
    assertEquals("list2", list.name());
    assertEquals(MinorType.LIST, list.type());
    assertEquals(DataMode.REPEATED, list.mode());
    assertEquals(StructureType.MULTI_ARRAY, list.structureType());
    assertTrue(list.isArray());
    assertEquals(3, list.dimensions());
    assertNotNull(list.childSchema());

    ColumnMetadata child1 = list.childSchema();
    assertEquals("list2", child1.name());
    assertEquals(MinorType.LIST, child1.type());
    assertEquals(DataMode.REPEATED, child1.mode());
    assertEquals(StructureType.MULTI_ARRAY, child1.structureType());
    assertTrue(child1.isArray());
    assertEquals(2, child1.dimensions());
    assertNotNull(child1.childSchema());

    ColumnMetadata child2 = child1.childSchema();
    assertEquals("list2", child2.name());
    assertEquals(MinorType.VARCHAR, child2.type());
    assertEquals(DataMode.REPEATED, child2.mode());
    assertTrue(child2.isArray());
    assertEquals(1, child2.dimensions());
    assertNull(child2.childSchema());
  }

}
