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
package org.apache.drill.exec.record.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;

public class TestColumnMetadata {

  @Test
  public void testConstructors() throws IOException {
    {
      ColumnMetadata col = AbstractColumnMetadata.createColumnMetadata("foo", "INT", DataMode.REQUIRED, null, null, null);
      assertEquals("foo", col.name());
      assertEquals(MinorType.INT, col.type());
      assertEquals(DataMode.REQUIRED, col.mode());
      assertEquals(ColumnMetadata.UNDEFINED, col.precision());
      assertEquals(ColumnMetadata.UNDEFINED, col.scale());

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata((PrimitiveColumnMetadata) col);
      assertEquals("foo", col2.name());
      assertEquals(MinorType.INT, col2.type());
      assertEquals(DataMode.REQUIRED, col2.mode());
      assertEquals(ColumnMetadata.UNDEFINED, col2.precision());
      assertEquals(ColumnMetadata.UNDEFINED, col2.scale());

      assertEquals(col, col2);
    }
    {
      ColumnMetadata col = AbstractColumnMetadata.createColumnMetadata("foo", "DECIMAL", DataMode.REQUIRED, null, null, null);
      assertEquals("foo", col.name());
      assertEquals(MinorType.VARDECIMAL, col.type());
      assertEquals(DataMode.REQUIRED, col.mode());
      assertEquals(ColumnMetadata.MAX_DECIMAL_PRECISION, col.precision());
      assertEquals(0, col.scale());

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata((PrimitiveColumnMetadata) col);
      assertEquals("foo", col2.name());
      assertEquals(MinorType.VARDECIMAL, col2.type());
      assertEquals(DataMode.REQUIRED, col2.mode());
      assertEquals(ColumnMetadata.MAX_DECIMAL_PRECISION, col2.precision());
      assertEquals(0, col2.scale());

      assertEquals(col, col2);
    }
    {
      ColumnMetadata col = AbstractColumnMetadata.createColumnMetadata("foo", "DECIMAL(10,4)", DataMode.REQUIRED, null, null, null);
      assertEquals(10, col.precision());
      assertEquals(4, col.scale());

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata((PrimitiveColumnMetadata) col);
      assertEquals(col, col2);
    }
    {
      ColumnMetadata col = AbstractColumnMetadata.createColumnMetadata("foo", "DECIMAL(10,4)", DataMode.REQUIRED, "##,###", null, null);
      assertEquals("##,###", col.format());
      assertEquals("##,###", col.property(ColumnMetadata.FORMAT_PROP));

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata((PrimitiveColumnMetadata) col);
      assertEquals(col, col2);
    }
    {
      ColumnMetadata col = AbstractColumnMetadata.createColumnMetadata("foo", "DECIMAL(10,4)", DataMode.REQUIRED, null, "10", null);
      assertEquals("10", col.defaultValue());
      assertEquals("10", col.property(ColumnMetadata.DEFAULT_VALUE_PROP));

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata((PrimitiveColumnMetadata) col);
      assertEquals(col, col2);
    }
    {
      Map<String,String> props = new HashMap<>();
      props.put(ColumnMetadata.FORMAT_PROP, "foo");
      props.put(ColumnMetadata.FORMAT_PROP, "foo");
      props.put("foo", "bar");
      ColumnMetadata col = AbstractColumnMetadata.createColumnMetadata("foo", "DECIMAL", DataMode.REQUIRED, "##,###", "10", props);
      assertEquals("##,###", col.format());
      assertEquals("10", col.defaultValue());
      assertEquals("bar", col.property("foo"));

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata((PrimitiveColumnMetadata) col);
      assertEquals(col, col2);
    }
    try {
      PrimitiveColumnMetadata.createColumnMetadata("foo", "BOGUS", DataMode.REQUIRED, null, null, null);
      fail();
    } catch (Exception e) {
      // Expected
    }
  }

  @SuppressWarnings("unlikely-arg-type")
  @Test
  public void testEquals() throws IOException {
    ColumnMetadata col1 = ColumnBuilder.required("foo", MinorType.VARDECIMAL);
    assertFalse(col1.equals(null));
    assertFalse(col1.equals("foo"));

    // Type differs

    ColumnMetadata col2 = ColumnBuilder.required("foo", MinorType.BIGINT);
    assertNotEquals(col1, col2);

    // Mode differs

    ColumnMetadata col3 = ColumnBuilder.nullable("foo", MinorType.VARDECIMAL);
    assertNotEquals(col1, col3);

    // Precision & scale

    ColumnMetadata col4 = ColumnBuilder.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
        .precision(10)
        .scale(4)
        .build();
    assertNotEquals(col1, col4);

    // Precision differs

    ColumnMetadata col5 = ColumnBuilder.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
        .precision(11)
        .scale(4)
        .build();
   assertNotEquals(col3, col5);

    // Scale differs

    ColumnMetadata col6 = ColumnBuilder.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
        .precision(10)
        .scale(5)
        .build();
    assertNotEquals(col3, col6);

    // Properties differ

    ColumnMetadata col7 = ColumnBuilder.required("foo", MinorType.VARDECIMAL);
    col7.setProperty(ColumnMetadata.FORMAT_PROP, "foo");
    assertNotEquals(col1, col7);
  }

  @Test
  public void testSerialization() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    ObjectReader reader = mapper.readerFor(AbstractColumnMetadata.class);

    {
      ColumnMetadata col = ColumnBuilder.required("foo", MinorType.VARDECIMAL);
      String ser = mapper.writeValueAsString(col);

      ColumnMetadata deser = reader.readValue(ser);
      assertEquals(col, deser);
    }

    {
      ColumnMetadata col = ColumnBuilder.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
          .precision(10)
          .scale(4)
          .build();
      String ser = mapper.writeValueAsString(col);

      ColumnMetadata deser = reader.readValue(ser);
      assertEquals(col, deser);
    }

    {
      ColumnMetadata col = ColumnBuilder.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
          .precision(10)
          .scale(4)
          .build();
      col.setProperty("foo", "bar");
      String ser = mapper.writeValueAsString(col);

      ColumnMetadata deser = reader.readValue(ser);
      assertEquals(col, deser);
    }

    {
      ColumnMetadata col = ColumnBuilder.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
          .precision(10)
          .scale(4)
          .build();
      col.setProperty("foo", "bar");
      col.setProperty(ColumnMetadata.FORMAT_PROP, "##,##");
      col.setProperty(ColumnMetadata.DEFAULT_VALUE_PROP, "10");
      String ser = mapper.writeValueAsString(col);
      assertTrue(ser.contains("\"format\" : \"##,##\""));
      assertTrue(ser.contains("\"default\" : \"10\""));
      assertTrue(ser.contains("\"properties\" : {\n    \"foo\" : \"bar\"\n  }"));

      ColumnMetadata deser = reader.readValue(ser);
      assertEquals(col, deser);
    }
  }

  @Test
  public void testToStrings() {
    {
      ColumnMetadata col = ColumnBuilder.required("foo", MinorType.INT);
      assertEquals("(`foo` INT NOT NULL)", col.planString());
      assertEquals("INT", col.sqlTypeString());
    }
    {
      ColumnMetadata col = ColumnBuilder.nullable("foo", MinorType.VARDECIMAL);
      assertEquals("(`foo` VARDECIMAL(38,0))", col.planString());
      assertEquals("DECIMAL(38, 0)", col.sqlTypeString());
    }
    {
      ColumnMetadata col = ColumnBuilder.builder("foo", MinorType.VARDECIMAL, DataMode.REPEATED)
          .precision(10)
          .scale(4)
          .build();
      assertEquals("(`foo` ARRAY<VARDECIMAL(10,4)>)", col.planString());
      assertEquals("DECIMAL(10, 4)", col.sqlTypeString());
    }
    {
      ColumnMetadata col = ColumnBuilder.builder("foo", MinorType.VARCHAR, DataMode.OPTIONAL)
          .precision(10)
          .build();
      assertEquals("(`foo` VARCHAR(10))", col.planString());
      assertEquals("VARCHAR(10)", col.sqlTypeString());
    }
    {
      ColumnMetadata col = ColumnBuilder.required("foo", MinorType.INT);
      col.setProperty(ColumnMetadata.FORMAT_PROP, "bar");
      assertEquals("(`foo` INT NOT NULL, properties={drill.format=bar})", col.planString());
      assertEquals("INT", col.sqlTypeString());
    }
  }
}
