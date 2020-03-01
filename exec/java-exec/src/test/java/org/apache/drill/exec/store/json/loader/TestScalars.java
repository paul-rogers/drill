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
package org.apache.drill.exec.store.json.loader;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.EmptyErrorContext;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonStructureOptions;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

public class TestScalars extends SubOperatorTest {

  public static class JsonLoaderFixture {

    public OptionBuilder rsLoaderOptions = new OptionBuilder();
    public TupleMetadata providedSchema;
    public JsonStructureOptions jsonOptions = new JsonStructureOptions();
    public CustomErrorContext errorContext = new EmptyErrorContext();
    private ResultSetLoader rsLoader;
    private JsonLoader loader;

    public void open(InputStream is) {
      rsLoader = new ResultSetLoaderImpl(fixture.allocator(), rsLoaderOptions.build());
      loader = new JsonLoaderImpl(rsLoader, providedSchema, jsonOptions, errorContext, is);
    }

    public void open(String json) {
      InputStream stream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
      open(stream);
    }

    public RowSet next() {
      if (!loader.next()) {
        return null;
      }
      return fixture.wrap(rsLoader.harvest());
    }

    public void close() {
      loader.close();
      rsLoader.close();
    }
  }

  @Test
  public void testBoolean() {
    String json = "{a: true} {a: false} {a: null} " +
        "{a: 1} {a: 0} " +
        "{a: 1.0} {a: 0.0} " +
        "{a: \"true\"} {a: \"\"} {a: \"false\"} {a: \"other\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIT)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(true)   // true
        .addRow(false)  // false
        .addRow((Boolean) null)   // null
        .addRow(true)   // 1
        .addRow(false)  // 0
        .addRow(true)   // 1.0
        .addRow(false)  // 0.0
        .addRow(true)   // "true"
        .addRow(false)  // ""
        .addRow(false)  // "false"
        .addRow(false)  // "other"
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testInt() {
    String json =
        "{a: 1} {a: 0} {a: -300} {a: null} " +
        "{a: true} {a: false} " +
        "{a: 1.0} {a: 1.4} {a: 1.5} {a: 0.0} " +
        "{a: \"\"} {a: \"3\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1)      // 1
        .addRow(0)      // 0
        .addRow(-300)   // -300
        .addRow((Long) null)   // null
        .addRow(1)      // true
        .addRow(0)      // false
        .addRow(1)      // 1.0
        .addRow(1)      // 1.4
        .addRow(2)      // 1.5
        .addRow(0)      // 0.0
        .addRow((Long) null)   // ""
        .addRow(3)      // "3"
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testFloat() {
    String json =
        "{a: 0.0} {a: 1.0} {a: 1.25} {a: -123.125} {a: null} " +
        "{a: -Infinity} {a: NaN} {a: Infinity} " +
        "{a: 0} {a: 12} " +
        "{a: true} {a: false} " +
        "{a: \"\"} {a: \"3.75\"}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.jsonOptions.allowNanInf = true;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(0.0)      // 0.0
        .addRow(1.0)      // 1.0
        .addRow(1.25)     // 1.25
        .addRow(-123.125) // -123.125
        .addRow((Double) null)   // null
        .addRow(Double.NEGATIVE_INFINITY) // -Inf
        .addRow(Double.NaN) // Nan
        .addRow(Double.POSITIVE_INFINITY) // Inf
        .addRow(0.0)      // 0
        .addRow(12.0)     // 12
        .addRow(1.0)      // true
        .addRow(0.0)      // false
        .addRow((Double) null)   // ""
        .addRow(3.75)     // "3.75"
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void testString() {
    String json =
        "{a: \"\"} {a: \"foo\"} {a: \" bar \"} {a: null} " +
        "{a: 0} {a: 12} " +
        "{a: true} {a: false} " +
        "{a: 0.0} {a: 1.25}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("")       // ""
        .addRow("foo")    // "foo"
        .addRow(" bar ")  // " bar "
        .addRow((String) null) // null
        .addRow("0")      // 0
        .addRow("12")     // 12
        .addRow("true")   // true
        .addRow("false")  // false
        .addRow("0.0")    // 0.0
        .addRow("1.25")   // 1.25
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }
}
