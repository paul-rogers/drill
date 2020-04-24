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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.drill.categories.JsonTest;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(JsonTest.class)
public class TestJsonParserUnknowns extends BaseTestJsonParser {

  @Test
  public void testNullToScalar() {
    final String json =
        "{a: null} {a: null} {a: 2} {a: 3}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    // {a: null}
    assertTrue(fixture.next());
    assertTrue(fixture.rootObject.fieldParser("a") instanceof NullValueParser);

    // {a: null} - still null
    assertTrue(fixture.next());
    assertTrue(fixture.rootObject.fieldParser("a") instanceof NullValueParser);

    // See a scalar, can revise estimate of field type
    // {a: 2}
    assertTrue(fixture.next());
    ValueListenerFixture a2 = fixture.field("a");
    assertEquals(2L, a2.lastValue);
    assertEquals(0, a2.nullCount);
    assertEquals(1, a2.valueCount);

    assertTrue(fixture.next());
    assertEquals(3L, a2.lastValue);

    fixture.close();
  }

  @Test
  public void testNullToObject() {
    final String json =
        "{a: null} {a: {}} {a: {b: 3}}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    // {a: null}
    assertTrue(fixture.next());
    assertTrue(fixture.rootObject.fieldParser("a") instanceof NullValueParser);

    // See an object, can revise estimate of field type
    // {a: {}}
    assertTrue(fixture.next());
    ValueListenerFixture a2 = fixture.field("a");
    assertNotNull(a2.objectValue);

    assertTrue(fixture.next());
    ValueListenerFixture b = a2.objectValue.field("b");
    assertEquals(3L, b.lastValue);

    fixture.close();
  }

  @Test
  public void testNullToEmptyArray() {
    final String json =
        "{a: null} {a: []} {a: []} {a: [10, 20]} {a: [30, 40]}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    // {a: null}
    assertTrue(fixture.next());
    assertTrue(fixture.rootObject.fieldParser("a") instanceof NullValueParser);

    // See an empty array, can revise estimate of field type
    // {a: []}
    assertTrue(fixture.next());
    assertTrue(fixture.rootObject.fieldParser("a") instanceof EmptyArrayParser);

    // Ensure things are stable
    // {a: []}
    assertTrue(fixture.next());
    assertTrue(fixture.rootObject.fieldParser("a") instanceof EmptyArrayParser);

    // Revise again once we see element type
    // {a: [10, 20]}
    assertTrue(fixture.next());
    ValueListenerFixture a2 = fixture.field("a");
    assertEquals(1, a2.arrayValue.startCount);
    assertEquals(1, a2.arrayValue.endCount);
    assertEquals(2, a2.arrayValue.element.valueCount);
    assertEquals(20L, a2.arrayValue.element.lastValue);

    // Check stability again
    // {a: [30, 40]}
    assertTrue(fixture.next());
    assertSame(a2, fixture.field("a"));
    assertEquals(2, a2.arrayValue.startCount);
    assertEquals(4, a2.arrayValue.element.valueCount);
    assertEquals(40L, a2.arrayValue.element.lastValue);

    fixture.close();
  }

  /**
   * As above, but skips the intermediate empty array.
   * The array, when it appears, has type info.
   */
  @Test
  public void testNullToArray() {
    final String json =
        "{a: null} {a: [10, 20]} {a: [30, 40]}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    // {a: null}
    assertTrue(fixture.next());
    assertTrue(fixture.rootObject.fieldParser("a") instanceof NullValueParser);

    // See a typed empty array, can revise estimate of field type
    // {a: [10, 20]}
    assertTrue(fixture.next());
    ValueListenerFixture a2 = fixture.field("a");
    assertEquals(1, a2.arrayValue.startCount);
    assertEquals(1, a2.arrayValue.startCount);
    assertEquals(JsonType.INTEGER, a2.arrayValue.valueDef.type());
    assertEquals(1, a2.arrayValue.valueDef.dimensions());
    assertEquals(JsonType.INTEGER, a2.arrayValue.element.valueDef.type());
    assertEquals(0, a2.arrayValue.element.valueDef.dimensions());
    assertEquals(2, a2.arrayValue.element.valueCount);
    assertEquals(20L, a2.arrayValue.element.lastValue);

    // Ensure things are stable
    // {a: [30, 40]}
    assertTrue(fixture.next());
    assertSame(a2, fixture.field("a"));
    assertEquals(2, a2.arrayValue.startCount);
    assertEquals(4, a2.arrayValue.element.valueCount);
    assertEquals(40L, a2.arrayValue.element.lastValue);

    fixture.close();
  }
}
