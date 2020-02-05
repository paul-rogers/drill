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
package org.apache.drill.exec.store.easy.json.structparser;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.easy.json.structparser.ContainerListener.ArrayListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

import com.fasterxml.jackson.core.JsonToken;

public abstract class ContainerParser extends AbstractParser {

  public ContainerParser(JsonStructureParser structParser, String key) {
    super(structParser, key);
  }

  public ContainerParser(JsonElementParser parent, String key) {
    super(parent, key);
  }

  protected abstract ContainerListener listener();

  /**
   * Create a parser for a scalar array implemented as a repeated type.
   *
   * @param parent
   * @param token
   * @param key
   * @return
   */
  protected JsonElementParser detectScalarArrayParser(JsonToken token, String key) {
    return parserOrDummy(listener().scalarArrayParser(key, token), key);
  }

  protected ObjectParser objectParser(String key) {
    return new ObjectParser(this, key, listener().objectListener(key));
  }

  protected JsonElementParser typedScalar(JsonToken token, String key) {
    return parserOrDummy(listener().scalarParser(key, token), key);
  }

  private JsonElementParser parserOrDummy(JsonElementParser parser, String key) {
    return parser == null ? new DummyValueParser(this, key) : parser;
  }

  protected ArrayParser objectArrayParser(String key) {
    ArrayListener arrayListener = listener().arrayListener(key);
    return new ArrayParser.ObjectArrayParser(this, key, arrayListener,
        new ObjectParser(this, key,
            arrayListener.objectListener(null)));
  }

  /**
   * Detect the type of an array member by "sniffing" the first element.
   * Creates a simple repeated type if requested and possible. Else, creates
   * an array depending on the array contents. May have to look ahead
   * multiple tokens if the array is multi-dimensional.
   * <p>
   * Note that repeated types can only appear directly inside maps; they
   * cannot be used inside a list.
   *
   * @param parent the object parser that will hold the array element
   * @param key field name
   * @return the parse state for this array
   */
  protected JsonElementParser detectArrayParser(TokenIterator tokenizer, String key) {

    JsonElementParser listParser = listener().arrayParser(key);
    if (listParser != null) {
      return listParser;
    }

    // Implement JSON lists using the standard repeated types or the
    // repeated list type.
    // Detect the type of that array. Or, detect that the the first value
    // dictates that a list be used because this is a nested list, contains
    // nulls, etc.

    JsonToken token = tokenizer.requireNext();
    switch (token) {
      case START_ARRAY:

        // Can't use an array, must use a list since this is nested
        // or null.

        listParser = repeatedListParser(key);
        break;

      case VALUE_NULL:
      case END_ARRAY:

        // Don't know what this is. Defer judgment until later.

        listParser = nullArrayParser(key);
        break;

      case START_OBJECT:
        listParser = objectArrayParser(key);
        break;

      default:
        listParser = detectScalarArrayParser(token, key);
        break;
    }
    tokenizer.unget(token);
    return listParser;
  }

  protected abstract JsonElementParser nullArrayParser(String key);

  protected void replaceChild(String key, JsonElementParser newParser) {
    throw new UnsupportedOperationException();
  }
}
