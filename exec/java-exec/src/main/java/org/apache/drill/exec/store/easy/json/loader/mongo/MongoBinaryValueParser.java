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
package org.apache.drill.exec.store.easy.json.loader.mongo;

import org.apache.drill.exec.store.easy.json.loader.values.ScalarListener;
import org.apache.drill.exec.store.easy.json.parser.JsonStructureParser;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parsers a binary. Ignores the subtype field.</pre>
 */
public class MongoBinaryValueParser extends BaseExtendedValueParser {

  protected static final String BINARY_HINT =
      "{\"$binary\": {base64: (\"<payload>\", subType: \"<t>\" }) | " +
        "(\"<payload>\", \"$type\": \"<t>\") }";

  public MongoBinaryValueParser(JsonStructureParser structParser, ScalarListener listener) {
    super(structParser, listener);
  }

  @Override
  protected String typeName() { return ExtendedTypeNames.BINARY; }

  @Override
  public void parse(TokenIterator tokenizer) {

    // Null: assume the value is null
    // (Extension to extended types)
    JsonToken token = tokenizer.requireNext();
    if (token == JsonToken.VALUE_NULL) {
      listener.onValue(token, tokenizer);
      return;
    }

    // Value is a scalar, assume binary value as a string.
    // This is a harmless extension to the standard.
     if (token.isScalarValue()) {
      listener.onValue(token, tokenizer);
      return;
    }

    // Must be an object
    requireToken(token, JsonToken.START_OBJECT);

    // Field name must be correct
    requireField(tokenizer, ExtendedTypeNames.BINARY);

    token = tokenizer.requireNext();

    if (token == JsonToken.START_OBJECT) {
      // V2: { "base64": "<payload>", "subType": "<t>" }
      // With fields in either order
      for (;;) {
        token = tokenizer.requireNext();
        if (token == JsonToken.END_OBJECT) {
          break;
        } else if (token != JsonToken.FIELD_NAME) {
          throw syntaxError();
        }
        switch (tokenizer.textValue()) {
          case "base64":
            listener.onValue(requireScalar(tokenizer), tokenizer);
            break;
          case "subType":
            requireScalar(tokenizer);
            break;
          default:
            throw syntaxError();
        }
      }
    } else if (token.isScalarValue()) {
      // V1: { "$binary": "<bindata>", "$type": "<t>" }
      // With fields in either order
      listener.onValue(token, tokenizer);
      token = tokenizer.requireNext();
      if (token == JsonToken.FIELD_NAME) {
        tokenizer.unget(token);
        requireField(tokenizer, "$type");
        requireScalar(tokenizer);
      } else {
        tokenizer.unget(token);
      }
    } else {
      syntaxError();
    }

    requireToken(tokenizer, JsonToken.END_OBJECT);
  }

  @Override
  protected String formatHint() {
    return BINARY_HINT;
  }
}
