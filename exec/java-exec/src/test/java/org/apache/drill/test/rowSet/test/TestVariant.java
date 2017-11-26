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

import java.util.Collection;
import java.util.List;

import javax.json.JsonValue.ValueType;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.ColumnMetadata.StructureType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.VariantColumnMetadata;
import org.apache.drill.exec.record.VariantMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantReader;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.UnionVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetWriter;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.junit.Test;

public class TestVariant extends SubOperatorTest {
  @SuppressWarnings("resource")
  @Test
  public void testBuildRowSetUnion() {
    TupleMetadata schema = new SchemaBuilder()

        // Union with simple and complex types

        .addUnion("u")
          .addType(MinorType.INT)
          .addMap()
            .addNullable("c", MinorType.BIGINT)
            .addNullable("d", MinorType.VARCHAR)
            .buildNested()
          .addList()
            .addType(MinorType.VARCHAR)
            .buildNested()
          .build()
        .buildSchema();

    ExtendableRowSet rowSet = fixture.rowSet(schema);
    VectorContainer vc = rowSet.container();
    assertEquals(1, vc.getNumberOfColumns());

    // Single union

    ValueVector vector = vc.getValueVector(0).getValueVector();
    assertTrue(vector instanceof UnionVector);
    UnionVector union = (UnionVector) vector;

    MapVector typeMap = union.getTypeMap();
    ValueVector member = typeMap.getChild(MinorType.INT.name());
    assertTrue(member instanceof NullableIntVector);

    // Inner map

    member = typeMap.getChild(MinorType.MAP.name());
    assertTrue(member instanceof MapVector);
    member = typeMap.getChild(MinorType.MAP.name());
    assertTrue(member instanceof MapVector);
    MapVector childMap = (MapVector) member;
    ValueVector mapMember = childMap.getChild("c");
    assertNotNull(mapMember);
    assertTrue(mapMember instanceof NullableBigIntVector);
    mapMember = childMap.getChild("d");
    assertNotNull(mapMember);
    assertTrue(mapMember instanceof NullableVarCharVector);

    // Inner list

    member = typeMap.getChild(MinorType.LIST.name());
    assertTrue(member instanceof ListVector);
    ListVector list = (ListVector) member;
    assertTrue(list.getDataVector() instanceof NullableVarCharVector);

    rowSet.clear();
  }

  /**
   * Test a variant (AKA "union vector") at the top level, using
   * just scalar values.
   */

  @Test
  public void testScalarVariant() {
    TupleMetadata schema = new SchemaBuilder()
        .addUnion("u")
          .addType(MinorType.INT)
          .addType(MinorType.VARCHAR)
          .addType(MinorType.FLOAT8)
          .build()
        .buildSchema();

    ExtendableRowSet rs = fixture.rowSet(schema);
    RowSetWriter writer = rs.writer();

    // Sanity check of writer structure

    ObjectWriter wo = writer.column(0);
    assertEquals(ObjectType.VARIANT, wo.type());
    VariantWriter vw = wo.variant();
    assertSame(vw, writer.variant(0));
    assertSame(vw, writer.variant("u"));
    assertTrue(vw.hasType(MinorType.INT));
    assertTrue(vw.hasType(MinorType.VARCHAR));
    assertTrue(vw.hasType(MinorType.FLOAT8));

    // Write values of different types

    vw.scalar(MinorType.INT).setInt(10);
    writer.save();

    vw.scalar(MinorType.VARCHAR).setString("fred");
    writer.save();

    // The entire variant is null

    vw.setNull();
    writer.save();

    vw.scalar(MinorType.FLOAT8).setDouble(123.45);
    writer.save();

    // Strange case: just the value is null, but the variant
    // is not null.

    vw.scalar(MinorType.INT).setNull();
    writer.save();

    // Marker to avoid fill-empty issues (fill-empties tested elsewhere.)

    vw.scalar(MinorType.INT).setInt(20);
    writer.save();

    SingleRowSet result = writer.done();
    assertEquals(6, result.rowCount());

    // Read the values.

    RowSetReader reader = result.reader();

    // Sanity check of structure

    ObjectReader ro = reader.column(0);
    assertEquals(ObjectType.VARIANT, ro.type());
    VariantReader vr = ro.variant();
    assertSame(vr, reader.variant(0));
    assertSame(vr, reader.variant("u"));
    for (MinorType type : MinorType.values()) {
      if (type == MinorType.INT || type == MinorType.VARCHAR || type == MinorType.FLOAT8) {
        assertTrue(vr.hasType(type));
      } else {
        assertFalse(vr.hasType(type));
      }
    }

    // Can get readers up front

    ScalarReader intReader = vr.scalar(MinorType.INT);
    ScalarReader strReader = vr.scalar(MinorType.VARCHAR);
    ScalarReader floatReader = vr.scalar(MinorType.FLOAT8);

    // Verify the data

    // Int 10

    assertTrue(reader.next());
    assertFalse(vr.isNull());
    assertTrue(vr.dataType() == MinorType.INT);
    assertSame(intReader, vr.scalar());
    assertNotNull(vr.member());
    assertSame(vr.scalar(), vr.member().scalar());
    assertFalse(intReader.isNull());
    assertEquals(10, intReader.getInt());
    assertTrue(strReader.isNull());
    assertTrue(floatReader.isNull());

    // String "fred"

    assertTrue(reader.next());
    assertFalse(vr.isNull());
    assertTrue(vr.dataType() == MinorType.VARCHAR);
    assertSame(strReader, vr.scalar());
    assertFalse(strReader.isNull());
    assertEquals("fred", strReader.getString());
    assertTrue(intReader.isNull());
    assertTrue(floatReader.isNull());

    // Null value

    assertTrue(reader.next());
    assertTrue(vr.isNull());
    assertNull(vr.dataType());
    assertNull(vr.scalar());
    assertTrue(intReader.isNull());
    assertTrue(strReader.isNull());
    assertTrue(floatReader.isNull());

    // Double 123.45

    assertTrue(reader.next());
    assertFalse(vr.isNull());
    assertTrue(vr.dataType() == MinorType.FLOAT8);
    assertSame(floatReader, vr.scalar());
    assertFalse(floatReader.isNull());
    assertEquals(123.45, vr.scalar().getDouble(), 0.001);
    assertTrue(intReader.isNull());
    assertTrue(strReader.isNull());

    // Strange case: null int (but union is not null)

    assertTrue(reader.next());
    assertFalse(vr.isNull());
    assertTrue(vr.dataType() == MinorType.INT);
    assertTrue(intReader.isNull());

    // Int 20

    assertTrue(reader.next());
    assertFalse(vr.isNull());
    assertFalse(intReader.isNull());
    assertEquals(20, intReader.getInt());

    assertFalse(reader.next());
    result.clear();
  }


  @SuppressWarnings("resource")
  @Test
  public void testBuildRowSetScalarList() {
    TupleMetadata schema = new SchemaBuilder()

        // Top-level single-element list

        .addList("list2")
          .addType(MinorType.VARCHAR)
          .build()
        .buildSchema();

    ExtendableRowSet rowSet = fixture.rowSet(schema);
    VectorContainer vc = rowSet.container();
    assertEquals(1, vc.getNumberOfColumns());


    // Single-type list

    ValueVector vector = vc.getValueVector(0).getValueVector();
    assertTrue(vector instanceof ListVector);
    ListVector list = (ListVector) vector;
    assertTrue(list.getDataVector() instanceof NullableVarCharVector);

    rowSet.clear();
  }

  @SuppressWarnings("resource")
  @Test
  public void testBuildRowSetUnionArray() {
    TupleMetadata schema = new SchemaBuilder()

        // List with multiple types

        .addList("list1")
          .addType(MinorType.BIGINT)
          .addMap()
            .addNullable("a", MinorType.INT)
            .addNullable("b", MinorType.VARCHAR)
            .buildNested()

          // Nested single-element list

          .addList()
            .addType(MinorType.FLOAT8)
            .buildNested()
          .build()
        .buildSchema();

    ExtendableRowSet rowSet = fixture.rowSet(schema);
    VectorContainer vc = rowSet.container();
    assertEquals(1, vc.getNumberOfColumns());

    // List with complex internal structure

    ValueVector vector = vc.getValueVector(0).getValueVector();
    assertTrue(vector instanceof ListVector);
    ListVector list = (ListVector) vector;
    assertTrue(list.getDataVector() instanceof UnionVector);
    UnionVector union = (UnionVector) list.getDataVector();

    // Union inside the list

    MajorType unionType = union.getField().getType();
    List<MinorType> types = unionType.getSubTypeList();
    assertEquals(3, types.size());
    assertTrue(types.contains(MinorType.BIGINT));
    assertTrue(types.contains(MinorType.MAP));
    assertTrue(types.contains(MinorType.LIST));

    MapVector typeMap = union.getTypeMap();
    ValueVector member = typeMap.getChild(MinorType.BIGINT.name());
    assertTrue(member instanceof NullableBigIntVector);

    // Map inside the list

    member = typeMap.getChild(MinorType.MAP.name());
    assertTrue(member instanceof MapVector);
    MapVector childMap = (MapVector) member;
    ValueVector mapMember = childMap.getChild("a");
    assertNotNull(mapMember);
    assertTrue(mapMember instanceof NullableIntVector);
    mapMember = childMap.getChild("b");
    assertNotNull(mapMember);
    assertTrue(mapMember instanceof NullableVarCharVector);

    // Single-type list inside the outer list

    member = typeMap.getChild(MinorType.LIST.name());
    assertTrue(member instanceof ListVector);
    ListVector childList = (ListVector) member;
    assertTrue(childList.getDataVector() instanceof NullableFloat8Vector);

    rowSet.clear();
  }

  /**
   * Test a variant (AKA "union vector") at the top level which
   * includes a map.
   */

  @Test
  public void testUnionWithMap() {
    TupleMetadata schema = new SchemaBuilder()
        .addUnion("u")
          .addType(MinorType.VARCHAR)
          .addMap()
            .addNullable("a", MinorType.INT)
            .addNullable("b", MinorType.VARCHAR)
            .buildNested()
          .build()
        .buildSchema();

    SingleRowSet result;

    // Write values

    {
      ExtendableRowSet rs = fixture.rowSet(schema);
      RowSetWriter writer = rs.writer();

      // Sanity check of writer structure

      ObjectWriter wo = writer.column(0);
      assertEquals(ObjectType.VARIANT, wo.type());
      VariantWriter vw = wo.variant();

      assertTrue(vw.hasType(MinorType.VARCHAR));
      ObjectWriter strObj = vw.member(MinorType.VARCHAR);
      ScalarWriter strWriter = strObj.scalar();
      assertSame(strWriter, vw.scalar(MinorType.VARCHAR));

      assertTrue(vw.hasType(MinorType.MAP));
      ObjectWriter mapObj = vw.member(MinorType.MAP);
      TupleWriter mWriter = mapObj.tuple();
      assertSame(mWriter, vw.tuple());

      ScalarWriter aWriter = mWriter.scalar("a");
      ScalarWriter bWriter = mWriter.scalar("b");

      // First row: string "first"

      vw.setType(MinorType.VARCHAR);
      strWriter.setString("first");
      writer.save();

      // Second row: a map

      vw.setType(MinorType.MAP);
      aWriter.setInt(20);
      bWriter.setString("fred");
      writer.save();

      // Third row: null

      vw.setNull();
      writer.save();

      // Fourth row: map with a null string

      vw.setType(MinorType.MAP);
      aWriter.setInt(40);
      bWriter.setNull();
      writer.save();

      // Fifth row: string "last"

      vw.setType(MinorType.VARCHAR);
      strWriter.setString("last");
      writer.save();

      result = writer.done();
      assertEquals(5, result.rowCount());
    }

    // Read the values.

    {
      RowSetReader reader = result.reader();

      // Sanity check of structure

      ObjectReader ro = reader.column(0);
      assertEquals(ObjectType.VARIANT, ro.type());
      VariantReader vr = ro.variant();

      assertTrue(vr.hasType(MinorType.VARCHAR));
      ObjectReader strObj = vr.member(MinorType.VARCHAR);
      ScalarReader strReader = strObj.scalar();
      assertSame(strReader, vr.scalar(MinorType.VARCHAR));

      assertTrue(vr.hasType(MinorType.MAP));
      ObjectReader mapObj = vr.member(MinorType.MAP);
      TupleReader mReader = mapObj.tuple();
      assertSame(mReader, vr.tuple());

      ScalarReader aReader = mReader.scalar("a");
      ScalarReader bReader = mReader.scalar("b");

      // First row: string "first"

      assertTrue(reader.next());
      assertFalse(vr.isNull());
      assertEquals(MinorType.VARCHAR, vr.dataType());
      assertFalse(strReader.isNull());
      assertTrue(mReader.isNull());
      assertEquals("first", strReader.getString());

      // Second row: a map

      assertTrue(reader.next());
      assertFalse(vr.isNull());
      assertEquals(MinorType.MAP, vr.dataType());
      assertTrue(strReader.isNull());
      assertFalse(mReader.isNull());
      assertFalse(aReader.isNull());
      assertEquals(20, aReader.getInt());
      assertFalse(bReader.isNull());
      assertEquals("fred", bReader.getString());

      // Third row: null

      assertTrue(reader.next());
      assertTrue(vr.isNull());
      assertTrue(strReader.isNull());
      assertTrue(mReader.isNull());
      assertTrue(aReader.isNull());
      assertTrue(bReader.isNull());

      // Fourth row: map with a null string

      assertTrue(reader.next());
      assertEquals(MinorType.MAP, vr.dataType());
      assertEquals(40, aReader.getInt());
      assertTrue(bReader.isNull());

      // Fifth row: string "last"

      assertTrue(reader.next());
      assertEquals(MinorType.VARCHAR, vr.dataType());
      assertEquals("last", strReader.getString());

      assertFalse(reader.next());
    }

    result.clear();
  }

  /**
   * Test a scalar list. Should act just like a repeated type, with the
   * addition of allowing the list for a row to be null.
   */

  @Test
  public void testScalarList() {
    TupleMetadata schema = new SchemaBuilder()
        .addList("list")
          .addType(MinorType.VARCHAR)
          .build()
        .buildSchema();

    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer();

    {
      ObjectWriter listObj = writer.column(0);
      assertEquals(ObjectType.ARRAY, listObj.type());
      ArrayWriter listArray = listObj.array();

      // The list contains only a scalar. But, because lists can,
      // in general, contain multiple contents, the list requires
      // an explicit save after each entry.

      ObjectWriter itemObj = listArray.entry();
      assertEquals(ObjectType.SCALAR, itemObj.type());
      ScalarWriter strWriter = itemObj.scalar();

      // First row: two strings and a null
      // Unlike a repeated type, a list can mark individual elements
      // as null.
      // List will automagically detect that data was written.

      strWriter.setString("fred");
      listArray.save();
      strWriter.setNull();
      listArray.save();
      strWriter.setString("wilma");
      listArray.save();
      writer.save();

      // Second row: null

      writer.save();

      // Third row: one string

      strWriter.setString("dino");
      listArray.save();
      writer.save();

      // Fourth row: empty array. Note that there is no trigger
      // to say that the column is not null, so we have to do it
      // explicitly.

      listArray.setNull(false);
      writer.save();

      // Last row: a null string and non-null

      strWriter.setNull();
      listArray.save();
      strWriter.setString("pebbles");
      listArray.save();
      writer.save();
    }

    SingleRowSet result = writer.done();
    assertEquals(5, result.rowCount());

    {
      RowSetReader reader = result.reader();

      ObjectReader listObj = reader.column(0);
      assertEquals(ObjectType.ARRAY, listObj.type());
      ArrayReader listArray = listObj.array();

      // The list is a repeated scalar

      assertEquals(ObjectType.SCALAR, listArray.entry().type());
      ScalarReader strReader = listArray.scalar();

      // First row: two strings and a null

      assertTrue(reader.next());
      assertFalse(listArray.isNull());
      assertEquals(3, listArray.size());
      assertTrue(listArray.next());
      assertFalse(strReader.isNull());
      assertEquals("fred", strReader.getString());
      assertTrue(listArray.next());
      assertTrue(strReader.isNull());
      assertTrue(listArray.next());
      assertFalse(strReader.isNull());
      assertEquals("wilma", strReader.getString());
      assertFalse(listArray.next());

      // Second row: null

      assertTrue(reader.next());
      assertTrue(listArray.isNull());
      assertEquals(0, listArray.size());

      // Third row: one string

      assertTrue(reader.next());
      assertFalse(listArray.isNull());
      assertEquals(1, listArray.size());
      assertTrue(listArray.next());
      assertEquals("dino", strReader.getString());
      assertFalse(listArray.next());

      // Fourth row: empty array.

      assertTrue(reader.next());
      assertFalse(listArray.isNull());
      assertEquals(0, listArray.size());
      assertFalse(listArray.next());

      // Last row: a null string and non-null

      assertTrue(reader.next());
      assertFalse(listArray.isNull());
      assertEquals(2, listArray.size());
      assertTrue(listArray.next());
      assertTrue(strReader.isNull());
      assertTrue(listArray.next());
      assertFalse(strReader.isNull());
      assertEquals("pebbles", strReader.getString());
      assertFalse(listArray.next());

      assertFalse(reader.next());
    }

    result.clear();
  }


  /**
   * Test a scalar list. Should act just like a repeated type, with the
   * addition of allowing the list for a row to be null.
   */

  @Test
  public void testVariantList() {
    TupleMetadata schema = new SchemaBuilder()
        .addList("list")
          .addType(MinorType.VARCHAR)
          .build()
        .buildSchema();

    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer();

    {
      ObjectWriter listObj = writer.column(0);
      assertEquals(ObjectType.ARRAY, listObj.type());
      ArrayWriter listArray = listObj.array();

      // The list is known to contain only a scalar, so
      // at this point, this looks like an array of scalars.

      ObjectWriter elementObj = listArray.entry();
      assertEquals(ObjectType.VARIANT, elementObj.type());
      VariantWriter variant = elementObj.variant();
      assertEquals(1, variant.schema().types());
      assertTrue(variant.schema().types().contains(MinorType.VARCHAR));
      assertEquals(1, variant.size());

      // But, this list is a single type, so it should act like
      // a repeated type.

      ObjectWriter itemObj = variant.member(MinorType.VARCHAR);
      assertEquals(ObjectType.SCALAR, itemObj.type());
      ScalarWriter strWriter = itemObj.scalar();

      // First row: three strings
      // List will automagically detect that data was written.

      strWriter.setString("fred");
      strWriter.setString("barney");
      strWriter.setString("wilma");
      writer.save();

      // Second row: null

      writer.save();

      // Third row: one string

      strWriter.setString("dino");
      writer.save();

      // Fourth row: empty array. Note that there is no trigger
      // to say that the column is not null, so we have to do it
      // explicitly.

      listArray.setNull(false);
      writer.save();

      // Last row: another two strings

      strWriter.setString("bambam");
      strWriter.setString("pebbles");
      writer.save();
    }

    SingleRowSet result = writer.done();
    assertEquals(5, result.rowCount());

    {
      RowSetReader reader = result.reader();

      ObjectReader listObj = reader.column(0);
      assertEquals(ObjectType.ARRAY, listObj.type());
      ArrayReader listArray = listObj.array();

      // The list is a repeated variant (union)

      ObjectReader elementObj = listArray.entry();
      assertEquals(ObjectType.VARIANT, elementObj.type());
      VariantReader variant = elementObj.variant();
      assertEquals(1, variant.schema().types());
      assertTrue(variant.schema().types().contains(MinorType.VARCHAR));
      assertEquals(1, variant.size());

      // But, this list is a single type, so it should act like
      // a repeated type.

      ObjectReader itemObj = variant.member(MinorType.VARCHAR);
      assertEquals(ObjectType.SCALAR, itemObj.type());
      ScalarElementReader strReader = listArray.elements();

      // First row: three strings

      assertTrue(reader.next());
      assertFalse(listArray.isNull());
      assertEquals(3, listArray.size());
      assertEquals(3, strReader.size());
      assertEquals("fred", strReader.getString(0));
      assertEquals("barney", strReader.getString(1));
      assertEquals("wilma", strReader.getString(2));

      // Second row: null

      assertTrue(reader.next());
      assertTrue(listArray.isNull());
      assertEquals(0, listArray.size());
      assertEquals(0, strReader.size());

      // Third row: one string

      assertTrue(reader.next());
      assertFalse(listArray.isNull());
      assertEquals(3, listArray.size());
      assertEquals("dino", strReader.getString(0));

      // Fourth row: empty array.

      assertTrue(reader.next());
      assertFalse(listArray.isNull());
      assertEquals(0, listArray.size());
      assertEquals(0, strReader.size());

      // Last row: another two strings

      assertTrue(reader.next());
      assertFalse(listArray.isNull());
      assertEquals(2, strReader.size());
      assertEquals("bambam", strReader.getString(0));
      assertEquals("pebbles", strReader.getString(1));

      assertFalse(reader.next());
    }

    result.clear();
  }

  /**
   * Test a variant (AKA "union vector") at the top level which includes
   * a list.
   */

  @Test
  public void testUnionWithList() {
    fail("Implement after list");
  }

  /**
   * Test a variant (AKA "union vector") at the top level, using
   * just scalar values.
   */

  @Test
  public void testAddTypes() {
    BatchSchema batchSchema = new SchemaBuilder()
        .addNullable("v", MinorType.UNION)
        .build();

    ExtendableRowSet rs = fixture.rowSet(batchSchema);
    RowSetWriter writer = rs.writer();

    // Sanity check of writer structure

    ObjectWriter wo = writer.column(0);
    assertEquals(ObjectType.VARIANT, wo.type());
    VariantWriter vw = wo.variant();
    assertSame(vw, writer.variant(0));
    assertSame(vw, writer.variant("v"));
    for (MinorType type : MinorType.values()) {
      assertFalse(vw.hasType(type));
    }

    // Write values of different types

    vw.scalar(MinorType.INT).setInt(10);
    assertTrue(vw.hasType(MinorType.INT));
    assertFalse(vw.hasType(MinorType.VARCHAR));
    writer.save();

    vw.scalar(MinorType.VARCHAR).setString("fred");
    assertTrue(vw.hasType(MinorType.VARCHAR));
    writer.save();

    vw.setNull();
    writer.save();

    vw.scalar(MinorType.FLOAT8).setDouble(123.45);
    assertTrue(vw.hasType(MinorType.INT));
    assertTrue(vw.hasType(MinorType.FLOAT8));
    writer.save();

    SingleRowSet result = writer.done();

    assertEquals(4, result.rowCount());

    // Read the values.

    RowSetReader reader = result.reader();

    // Sanity check of structure

    ObjectReader ro = reader.column(0);
    assertEquals(ObjectType.VARIANT, ro.type());
    VariantReader vr = ro.variant();
    assertSame(vr, reader.variant(0));
    assertSame(vr, reader.variant("v"));
    for (MinorType type : MinorType.values()) {
      if (type == MinorType.INT || type == MinorType.VARCHAR || type == MinorType.FLOAT8) {
        assertTrue(vr.hasType(type));
      } else {
        assertFalse(vr.hasType(type));
      }
    }

    // Verify the data

    assertTrue(reader.next());
    assertFalse(vr.isNull());
    assertTrue(vr.dataType() == MinorType.INT);
    assertSame(vr.scalar(MinorType.INT), vr.scalar());
    assertNotNull(vr.member());
    assertSame(vr.scalar(), vr.member().scalar());
    assertEquals(10, vr.scalar().getInt());

    assertTrue(reader.next());
    assertFalse(vr.isNull());
    assertTrue(vr.dataType() == MinorType.VARCHAR);
    assertSame(vr.scalar(MinorType.VARCHAR), vr.scalar());
    assertEquals("fred", vr.scalar().getString());

    assertTrue(reader.next());
    assertTrue(vr.isNull());
    assertNull(vr.dataType());
    assertNull(vr.scalar());

    assertTrue(reader.next());
    assertFalse(vr.isNull());
    assertTrue(vr.dataType() == MinorType.FLOAT8);
    assertSame(vr.scalar(MinorType.FLOAT8), vr.scalar());
    assertEquals(123.45, vr.scalar().getDouble(), 0.001);

    assertFalse(reader.next());
    result.clear();
  }

  @Test
  public void testSimpleScalarList() {
    TupleMetadata schema = new SchemaBuilder()
        .addList("list")
          .addType(MinorType.VARCHAR)
          .build()
        .buildSchema();

    ExtendableRowSet emptyRowSet = fixture.rowSet(schema);
    SingleRowSet rowSet;

    // Write items

    {
      // Verify writer structure

      RowSetWriter writer = emptyRowSet.writer();
      ObjectWriter listObj = writer.column(0);
      assertEquals(ObjectType.ARRAY, listObj.type());
      ArrayWriter listArr = listObj.array();
      assertNotNull(listArr);
      assertEquals(ObjectType.SCALAR, listArr.entryType());
      ObjectWriter entryObj = listArr.entry();
      assertEquals(ObjectType.SCALAR, entryObj.type());
      ScalarWriter entry = entryObj.scalar();
      assertEquals(ValueType.STRING, entry.valueType());

      // First row: three entries
      // Creation of an entry marks row as not null

      entry.setString("fred");
      entry.setString("barney");
      entry.setString("wilma");
      writer.save();

      // Second row, 0 items
      // Explicitly set not null

      listArr.setNull(false);
      writer.save();

      // Third row, 1 item

      entry.setString("bedrock");
      writer.save();

      // Fourth row, 0 items, row is null by default
      // (This differs from normal repeated types.)

      writer.save();

      // Fifth row, 1 item.

      entry.setString("dino");
      writer.save();

      rowSet = writer.done();
    }

    // Read items

    {
      // Verify reader structure

      RowSetReader reader = rowSet.reader();
      ObjectReader listObj = reader.column(0);
      assertEquals(ObjectType.ARRAY, listObj.type());
      ArrayReader listArr = listObj.array();
      assertNotNull(listArr);
      assertEquals(ObjectType.SCALAR, listArr.entryType());
      ObjectReader entryObj = listArr.entry();
      assertEquals(ObjectType.SCALAR, entryObj.type());
      ScalarReader entry = entryObj.scalar();
      assertEquals(ValueType.STRING, entry.valueType());

      assertEquals(3, reader.rowCount());

      // First row, 3 items

      assertTrue(reader.next());
      assertFalse(listArr.isNull());
      assertEquals(3, listArr.size());
      assertEquals("fred", listArr.entry(0).scalar().getString());
      assertEquals("barney", listArr.entry(1).scalar().getString());
      assertEquals("wilma", listArr.entry(2).scalar().getString());

      // Second row, 0 items

      assertTrue(reader.next());
      assertFalse(listArr.isNull());
      assertEquals(0, listArr.size());

      // Third row, 1 item

      assertTrue(reader.next());
      assertFalse(listArr.isNull());
      assertEquals(1, listArr.size());
      assertEquals("bedrock", listArr.entry(0).scalar().getString());

      // Fourth row, null

      assertTrue(reader.next());
      assertTrue(listArr.isNull());
      assertEquals(0, listArr.size());

      // Fifth row, 1 item

      assertTrue(reader.next());
      assertFalse(listArr.isNull());
      assertEquals(1, listArr.size());
      assertEquals("dino", listArr.entry(0).scalar().getString());

      assertFalse(reader.next());
    }

    rowSet.clear();
  }

  @Test
  public void testUnionList() {

  }

  @Test
  public void testRepeatedList() {

  }

}
