package org.apache.drill.exec.store.json.parser;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.exec.store.easy.json.structparser.v3.ArrayListener;
import org.apache.drill.exec.store.easy.json.structparser.v3.ErrorFactory;
import org.apache.drill.exec.store.easy.json.structparser.v3.JsonOptions;
import org.apache.drill.exec.store.easy.json.structparser.v3.JsonStructureParser;
import org.apache.drill.exec.store.easy.json.structparser.v3.JsonType;
import org.apache.drill.exec.store.easy.json.structparser.v3.ObjectListener;
import org.apache.drill.exec.store.easy.json.structparser.v3.ValueListener;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

public class TestJsonParserBasics {

  public static class JsonErrorFixture extends RuntimeException {
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

  public static class ErrorFactoryFixture implements ErrorFactory {

    @Override
    public RuntimeException parseError(String msg, JsonParseException e) {
      throw new JsonErrorFixture("parseError", msg, e);
    }

    @Override
    public RuntimeException ioException(IOException e) {
      throw new JsonErrorFixture("ioException", "", e);
    }

    @Override
    public RuntimeException structureError(String msg) {
      throw new JsonErrorFixture("structureError", msg);
    }

    @Override
    public RuntimeException syntaxError(JsonParseException e) {
      throw new JsonErrorFixture("syntaxError", "", e);
    }

    @Override
    public RuntimeException typeError(UnsupportedConversionError e) {
      throw new JsonErrorFixture("typeError", "", e);
    }

    @Override
    public RuntimeException ioException(String string, IOException e) {
      throw new JsonErrorFixture("ioException", "", e);
    }

    @Override
    public RuntimeException syntaxError(JsonToken token) {
      throw new JsonErrorFixture("syntaxError", token.toString());
    }

    @Override
    public RuntimeException unrecoverableError() {
      throw new JsonErrorFixture("unrecoverableError", "");
    }

    @Override
    public RuntimeException structureError(String key, JsonToken token) {
      throw new JsonErrorFixture("structureError",
          String.format("%s: %s", key, token.toString()));
    }
  }

  public static class ValueListenerFixture implements ValueListener {

    final int dimCount;
    final JsonType type;
    int nullCount;
    int valueCount;
    Object value;
    ObjectListener objectValue;
    ArrayListener arrayValue;

    public ValueListenerFixture(int dimCount, JsonType type) {
      this.dimCount = dimCount;
      this.type = type;
    }

    @Override
    public boolean isText() { return false; }

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
    public void onEmbedddObject(String value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public ObjectListener object() {
      assertNotNull(objectValue);
      objectValue = new ObjectListenerFixture();
      return objectValue;
    }

    @Override
    public ArrayListener array(int arrayDims, JsonType type) {
      assertNotNull(arrayValue);
      arrayValue = new ArrayListenerFixture(arrayDims, type);
      return arrayValue;
    }

    @Override
    public ArrayListener objectArray(int arrayDims) {
      assertNotNull(arrayValue);
      arrayValue = new ArrayListenerFixture(arrayDims, JsonType.OBJECT);
      return arrayValue;
    }
  }

  public static class ArrayListenerFixture implements ArrayListener {

    final int dimCount;
    final JsonType type;
    int startCount;
    int endCount;
    int elementCount;
    ValueListener scalarElement;
    ValueListener objectElement;
    ValueListener arrayElement;

    public ArrayListenerFixture(int dimCount, JsonType type) {
      this.dimCount = dimCount;
      this.type = type;
    }

    @Override
    public void onStart() {
      startCount++;
    }

    @Override
    public void onElement() {
      elementCount++;
    }

    @Override
    public void onEnd() {
      endCount++;
    }

    @Override
    public ValueListener objectArrayElement(int arrayDims) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ValueListener objectElement() {
      assertNotNull(objectElement);
      objectElement = new ValueListenerFixture(0, JsonType.OBJECT);
      return objectElement;
    }

    @Override
    public ValueListener arrayElement(int arrayDims, JsonType type) {
      assertNotNull(arrayElement);
      arrayElement = new ValueListenerFixture(arrayDims, JsonType.ARRAY);
      return arrayElement;
    }

    @Override
    public ValueListener scalarElement(JsonType type) {
      assertNotNull(scalarElement);
      scalarElement = new ValueListenerFixture(0, type);
      return scalarElement;
    }
  }

  public static class ObjectListenerFixture implements ObjectListener {

    final Map<String, ValueListenerFixture> fields = new HashMap<>();
    Set<String> projectFilter;
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

    @Override
    public boolean isProjected(String key) {
      return projectFilter == null | projectFilter.contains(key);
    }

    @Override
    public ValueListener addScalar(String key, JsonType type) {
      return field(key, 0, type);
    }

    @Override
    public ValueListener addArray(String key, int dims, JsonType type) {
      return field(key, dims, type);
    }

    @Override
    public ValueListener addObject(String key) {
      return field(key, 0, JsonType.OBJECT);
    }

    @Override
    public ValueListener addObjectArray(String key, int dims) {
      return field(key, dims, JsonType.OBJECT);
    }

    private ValueListener field(String key, int dims, JsonType type) {
      assertFalse(fields.containsKey(key));
      ValueListenerFixture field = new ValueListenerFixture(dims, type);
      fields.put(key, field);
      return field;
    }
  }

  public static class JsonParserFixture {
    JsonOptions options = new JsonOptions();
    JsonStructureParser parser;
    ObjectListenerFixture rootObject = new ObjectListenerFixture();

    public JsonParserFixture() {
      options.rootListener = rootObject;
      options.errorFactory = new ErrorFactoryFixture();
    }

    public void open(String json) {
      InputStream inStream = new
          ReaderInputStream(new StringReader(json));
      parser = new JsonStructureParser(inStream, options);
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
      ValueListenerFixture field = rootObject.fields.get(key);
      assertNotNull(field);
      return field;
    }

    public void close() {
      if (parser != null) {
        parser.close();
      }
    }
  }

  @Test
  public void testEmpty() {
    String json = "";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertFalse(fixture.next());
    assertEquals(0, fixture.rootObject.startCount);
    fixture.close();
  }

  @Test
  public void testEmptyTuple() {
    final String json = "{} {} {}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertEquals(3, fixture.read());
    assertEquals(3, fixture.rootObject.startCount);
    assertEquals(3, fixture.rootObject.endCount);
    assertTrue(fixture.rootObject.fields.isEmpty());
    fixture.close();
  }

  @Test
  public void testBoolean() {
    final String json = "{a: true} {a: false} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    assertEquals(1, fixture.rootObject.startCount);
    assertEquals(1, fixture.rootObject.fields.size());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.BOOLEAN, a.type);
    assertEquals(0, a.dimCount);
    assertEquals(0, a.nullCount);
    assertEquals(Boolean.TRUE, a.value);
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(Boolean.FALSE, a.value);
    fixture.close();
  }

  @Test
  public void testInteger() {
    final String json = "{a: 0} {a: 100} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.INTEGER, a.type);
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(100L, a.value);
    fixture.close();
  }

  @Test
  public void testFloat() {
    final String json = "{a: 0.0} {a: 100.5} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.FLOAT, a.type);
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(2, a.valueCount);
    assertEquals(100.5D, a.value);
    fixture.close();
  }

  @Test
  public void testExtendedFloat() {
    final String json =
        "{a: NaN} {a: Infinity} {a: -Infinity}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.options.readNumbersAsDouble = true;
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.FLOAT, a.type);
    assertEquals(2, fixture.read());
    assertEquals(3, a.valueCount);
    assertEquals(Double.NEGATIVE_INFINITY, a.value);
    fixture.close();
  }

  @Test
  public void testString() {
    final String json = "{a: \"\"} {a: \"hi\"} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.STRING, a.type);
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(2, a.valueCount);
    assertEquals("hi", a.value);
    fixture.close();
  }

  @Test
  public void testMixedTypes() {
    final String json = "{a: \"hi\"} {a: 10} {a: 10.5}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.STRING, a.type);
    assertEquals("hi", a.value);
    assertTrue(fixture.next());
    assertEquals(10L, a.value);
    assertTrue(fixture.next());
    assertEquals(10.5D, a.value);
    assertFalse(fixture.next());
    fixture.close();
  }

}
