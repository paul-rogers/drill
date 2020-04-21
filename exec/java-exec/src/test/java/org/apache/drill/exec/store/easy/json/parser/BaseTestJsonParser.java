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
package org.apache.drill.exec.store.easy.json.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.exec.store.easy.json.parser.JsonStructureParser.JsonStructureParserBuilder;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

public class BaseTestJsonParser {

  /**
   * Retain to the error type and error message so they
   * can be verified in a test.
   */
  @SuppressWarnings("serial")
  protected static class JsonErrorFixture extends RuntimeException {
    String errorType;

    public JsonErrorFixture(String errorType, String msg, Exception e) {
      super(msg, e);
      this.errorType = errorType;
    }

    public JsonErrorFixture(String errorType, String msg) {
      super(msg);
      this.errorType = errorType;
    }
  }

  /**
   * Convert JSON errors to a simple form for use in tests.
   * Not all errors are throw in normal operation; some require
   * faults in the I/O system or in the Jackson parser.
   */
  protected static class ErrorFactoryFixture implements ErrorFactory {

    @Override
    public RuntimeException parseError(String msg, JsonParseException e) {
      return new JsonErrorFixture("parseError", msg, e);
    }

    @Override
    public RuntimeException ioException(IOException e) {
      return new JsonErrorFixture("ioException", "", e);
    }

    @Override
    public RuntimeException structureError(String msg) {
      return new JsonErrorFixture("structureError", msg);
    }

    @Override
    public RuntimeException syntaxError(JsonParseException e) {
      return new JsonErrorFixture("syntaxError", "", e);
    }

    @Override
    public RuntimeException typeError(UnsupportedConversionError e) {
      return new JsonErrorFixture("typeError", "", e);
    }

    @Override
    public RuntimeException syntaxError(JsonToken token) {
      return new JsonErrorFixture("syntaxError", token.toString());
    }

    @Override
    public RuntimeException unrecoverableError() {
      return new JsonErrorFixture("unrecoverableError", "");
    }

    @Override
    public RuntimeException messageParseError(MessageParser.MessageContextException e) {
      return new JsonErrorFixture("messageParseError", "Message parse error", e);
    }
  }

  protected static class ValueListenerFixture implements ValueListener {

    final ValueDef valueDef;
    int nullCount;
    int valueCount;
    Object value;
    Consumer<ValueListener> host;
    ObjectListenerFixture objectValue;
    ArrayListenerFixture arrayValue;

    public ValueListenerFixture(ValueDef valueDef) {
      this.valueDef = valueDef;
    }

    @Override
    public void onNull() {
      nullCount++;
    }

    @Override
    public void onBoolean(boolean value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public void onInt(long value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public void onFloat(double value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public void onString(String value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public void onEmbeddedObject(String value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public ObjectListener object() {
      assertNull(objectValue);
      objectValue = new ObjectListenerFixture();
      return objectValue;
    }

    @Override
    public ArrayListener array(ValueDef valueDef) {
      if (arrayValue == null) {
        arrayValue = new ArrayListenerFixture(valueDef);
      }
      return arrayValue;
    }

    @Override
    public void bind(Consumer<ValueListener> host) {
      this.host = host;
    }
  }

  protected static class ArrayListenerFixture implements ArrayListener {

    final ValueDef valueDef;
    int startCount;
    int endCount;
    int elementCount;
    ValueListenerFixture element;

    public ArrayListenerFixture(ValueDef valueDef) {
      this.valueDef = valueDef;
    }

    @Override
    public void onStart() {
      startCount++;
    }

    @Override
    public void onElementStart() {
      elementCount++;
    }

    @Override
    public void onElementEnd() { }

    @Override
    public void onEnd() {
      endCount++;
    }

    @Override
    public ValueListener element(ValueDef valueDef) {
      if (element == null) {
        element = new ValueListenerFixture(valueDef);
      }
      return element;
    }
  }

  enum FieldType {

    /**
     * Parse the JSON object according to its type.
     */
    TYPED,

    /**
     * The field is to be treated as "all-text". Used when the parser-level
     * setting for {@code allTextMode} is {@code false}; allows per-field
     * overrides to, perhaps, ride over inconsistent scalar types for a
     * single field. The listener will receive only strings.
     */
    TEXT,

    /**
     * Parse the value, and all its children, as JSON.
     * That is, converts the parsed JSON back into a
     * JSON string. The listener will receive only strings.
     */
    JSON
  }

  protected static class ObjectListenerFixture implements ObjectListener {

    final Map<String, ValueListenerFixture> fields = new HashMap<>();
    Set<String> projectFilter;
    FieldType fieldType = FieldType.TYPED;
    int startCount;
    int endCount;

    @Override
    public void onStart() {
      startCount++;
    }

    @Override
    public void onEnd() {
      endCount++;
    }

    /**
     * Create a new field listener depending on the test setup.
     */
    @Override
    public ElementParser onField(FieldDefn fieldDefn) {
      if (projectFilter != null && !projectFilter.contains(fieldDefn.key())) {
        return fieldDefn.fieldFactory().ignoredFieldParser();
      }
      assertFalse(fields.containsKey(fieldDefn.key()));
      ValueListenerFixture fieldListener = makeField(fieldDefn.key(), fieldDefn.lookahead());
      fields.put(fieldDefn.key(), fieldListener);
      switch (fieldType) {
      case JSON:
        return fieldDefn.fieldFactory().jsonTextParser(fieldDefn, fieldListener);
      case TEXT:
        return fieldDefn.fieldFactory().textValueParser(fieldDefn, fieldListener);
      default:
        return fieldDefn.fieldFactory().valueParser(fieldDefn, fieldListener);
      }
    }

    public ValueListenerFixture makeField(String key, ValueDef valueDef) {
      return new ValueListenerFixture(valueDef);
    }

    public ValueListenerFixture field(String key) {
      ValueListenerFixture field = fields.get(key);
      assertNotNull(field);
      return field;
    }
  }

  /**
   * Wrapper around the JsonStructure parser to hold all the knick-knacks
   * needed for a test.
   */
  protected static class JsonParserFixture {
    JsonStructureParserBuilder builder;
    JsonStructureOptions options = new JsonStructureOptions();
    JsonStructureParser parser;
    ObjectListenerFixture rootObject = new ObjectListenerFixture();
    ErrorFactory errorFactory = new ErrorFactoryFixture();

    public JsonParserFixture() {
      builder = new JsonStructureParserBuilder();
    }

    public void open(String json) {
      InputStream inStream = new
          ReaderInputStream(new StringReader(json));
      builder
          .fromStream(inStream)
          .options(options)
          .rootListener(rootObject)
          .errorFactory(errorFactory);
      parser = builder.build();
    }

    public boolean next() {
      assertNotNull(parser);
      return parser.next();
    }

    public int read() {
      int i = 0;
      while (next()) {
        i++;
      }
      return i;
    }

    public ValueListenerFixture field(String key) {
      return rootObject.field(key);
    }

    public void expect(String key, Object[] values) {
      ValueListenerFixture valueListener = null;
      int expectedNullCount = 0;
      for (int i = 0; i < values.length; i++) {
        assertTrue(next());
        if (valueListener == null) {
          valueListener = field(key);
          expectedNullCount = valueListener.nullCount;
        }
        Object value = values[i];
        if (value == null) {
          expectedNullCount++;
        } else {
          assertEquals(value, valueListener.value);
        }
        assertEquals(expectedNullCount, valueListener.nullCount);
      }
    }

    public void close() {
      if (parser != null) {
        parser.close();
      }
    }
  }

  protected static void expectError(String json, String kind) {
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    expectError(fixture, kind);
    fixture.close();
  }

  protected static void expectError(JsonParserFixture fixture, String kind) {
    try {
      fixture.read();
      fail();
    } catch (JsonErrorFixture e) {
      assertEquals(kind, e.errorType);
    }
  }
}
