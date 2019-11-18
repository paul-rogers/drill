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
import com.fasterxml.jackson.databind.SerializationFeature;

public class TestColumnMetadata {

  @Test
  public void testConstructors() throws IOException {
    {
      PrimitiveColumnMetadata col = PrimitiveColumnMetadata.createColumnMetadata("foo", "INT", DataMode.REQUIRED, null, null, null);
      assertEquals("foo", col.name());
      assertEquals(MinorType.INT, col.type());
      assertEquals(DataMode.REQUIRED, col.mode());
      assertEquals(ColumnMetadata.UNDEFINED, col.precision());
      assertEquals(ColumnMetadata.UNDEFINED, col.scale());

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata(col);
      assertEquals("foo", col2.name());
      assertEquals(MinorType.INT, col2.type());
      assertEquals(DataMode.REQUIRED, col2.mode());
      assertEquals(ColumnMetadata.UNDEFINED, col2.precision());
      assertEquals(ColumnMetadata.UNDEFINED, col2.scale());

      assertEquals(col, col2);
    }
    {
      PrimitiveColumnMetadata col = PrimitiveColumnMetadata.createColumnMetadata("foo", "DECIMAL", DataMode.REQUIRED, null, null, null);
      assertEquals("foo", col.name());
      assertEquals(MinorType.VARDECIMAL, col.type());
      assertEquals(DataMode.REQUIRED, col.mode());
      assertEquals(ColumnMetadata.MAX_DECIMAL_PRECISION, col.precision());
      assertEquals(0, col.scale());

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata(col);
      assertEquals("foo", col2.name());
      assertEquals(MinorType.VARDECIMAL, col2.type());
      assertEquals(DataMode.REQUIRED, col2.mode());
      assertEquals(ColumnMetadata.MAX_DECIMAL_PRECISION, col2.precision());
      assertEquals(0, col2.scale());

      assertEquals(col, col2);
    }
    {
      PrimitiveColumnMetadata col = PrimitiveColumnMetadata.createColumnMetadata("foo", "DECIMAL(10,4)", DataMode.REQUIRED, null, null, null);
      assertEquals(10, col.precision());
      assertEquals(4, col.scale());

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata(col);
      assertEquals(col, col2);
    }
    {
      PrimitiveColumnMetadata col = PrimitiveColumnMetadata.createColumnMetadata("foo", "DECIMAL(10,4)", DataMode.REQUIRED, "##,###", null, null);
      assertEquals("##,###", col.format());
      assertEquals("##,###", col.property(ColumnMetadata.FORMAT_PROP));

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata(col);
      assertEquals(col, col2);
    }
    {
      PrimitiveColumnMetadata col = PrimitiveColumnMetadata.createColumnMetadata("foo", "DECIMAL(10,4)", DataMode.REQUIRED, null, "10", null);
      assertEquals("10", col.defaultValue());
      assertEquals("10", col.property(ColumnMetadata.DEFAULT_VALUE_PROP));

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata(col);
      assertEquals(col, col2);
    }
    {
      Map<String,String> props = new HashMap<>();
      props.put(ColumnMetadata.FORMAT_PROP, "foo");
      props.put(ColumnMetadata.FORMAT_PROP, "foo");
      props.put("foo", "bar");
      PrimitiveColumnMetadata col = PrimitiveColumnMetadata.createColumnMetadata("foo", "DECIMAL", DataMode.REQUIRED, "##,###", "10", props);
      assertEquals("##,###", col.format());
      assertEquals("10", col.defaultValue());
      assertEquals("bar", col.property("foo"));

      PrimitiveColumnMetadata col2 = new PrimitiveColumnMetadata(col);
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
    ColumnMetadata col1 = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED).build();
    assertFalse(col1.equals(null));
    assertFalse(col1.equals("foo"));

    // Type differs

    ColumnMetadata col2 = PrimitiveColumnMetadata.builder("foo", MinorType.BIGINT, DataMode.REQUIRED).build();
    assertNotEquals(col1, col2);

    // Mode differs

    ColumnMetadata col3 = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.OPTIONAL).build();
    assertNotEquals(col1, col3);

    // Precision & scale

    ColumnMetadata col4 = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
        .precision(10)
        .scale(4)
        .build();
    assertNotEquals(col1, col4);

    // Precision differs

    ColumnMetadata col5 = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
        .precision(12)
        .scale(4)
        .build();
   assertNotEquals(col3, col5);

    // Scale differs

    ColumnMetadata col6 = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
        .precision(10)
        .scale(5)
        .build();
    assertNotEquals(col3, col6);

    // Properties differ

    ColumnMetadata col7 = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED).build();
    col7.setProperty(ColumnMetadata.FORMAT_PROP, "foo");
    assertNotEquals(col1, col7);
  }

  @Test
  public void testSerialization() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    {
      ColumnMetadata col = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED).build();
      String ser = mapper.writeValueAsString(col);
      assertTrue(ser.contains("\"kind\" : \"primitive\""));

      ColumnMetadata deser = mapper.readerFor(PrimitiveColumnMetadata.class).readValue(ser);
      assertEquals(col, deser);
    }

    {
      ColumnMetadata col = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
          .precision(10)
          .scale(4)
          .build();
      String ser = mapper.writeValueAsString(col);

      ColumnMetadata deser = mapper.readerFor(PrimitiveColumnMetadata.class).readValue(ser);
      assertEquals(col, deser);
    }

    {
      ColumnMetadata col = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
          .precision(10)
          .scale(4)
          .build();
      col.setProperty("foo", "bar");
      String ser = mapper.writeValueAsString(col);

      ColumnMetadata deser = mapper.readerFor(PrimitiveColumnMetadata.class).readValue(ser);
      assertEquals(col, deser);
    }

    {
      ColumnMetadata col = PrimitiveColumnMetadata.builder("foo", MinorType.VARDECIMAL, DataMode.REQUIRED)
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

      ColumnMetadata deser = mapper.readerFor(PrimitiveColumnMetadata.class).readValue(ser);
      assertEquals(col, deser);
    }
  }
}
