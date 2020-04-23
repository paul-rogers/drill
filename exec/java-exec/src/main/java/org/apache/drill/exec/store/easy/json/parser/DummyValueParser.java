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

import com.fasterxml.jackson.core.JsonToken;

import org.apache.drill.exec.store.easy.json.parser.ElementParser.ValueParser;

/**
 * Parse and ignore an unprojected value. The parsing just "free wheels", we
 * care only about matching brackets, but not about other details.
 */
public class DummyValueParser implements ValueParser {

  public static final ValueParser INSTANCE = new DummyValueParser();

  private DummyValueParser() { }

  @Override
  public void parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    switch (token) {
      case START_ARRAY:
      case START_OBJECT:
        parseTail(tokenizer);
        break;

      default:
        break;
    }
  }

  private void parseTail(TokenIterator tokenizer) {

    // Parse (field: value)* }
    for (;;) {
      JsonToken token = tokenizer.requireNext();
      switch (token) {

        // Not exactly precise, but the JSON parser handles the
        // details.
        case END_OBJECT:
        case END_ARRAY:
          return;

        case START_OBJECT:
        case START_ARRAY:
          parseTail(tokenizer); // Recursively ignore objects
          break;

        default:
          break; // Ignore all else
      }
    }
  }

  @Override
  public void bindListener(ValueListener listener) {
    immutableError();
  }

  @Override
  public <T extends ValueListener> T listener() { return null; }

  @Override
  public void bindArrayParser(ArrayParser arrayParser) {
    immutableError();
  }

  @Override
  public ArrayParser arrayParser() { return null; }

  @Override
  public void bindObjectParser(ObjectParser objectParser) {
    immutableError();
  }

  private void immutableError() {
    throw new IllegalStateException("Dummy parser is immutable");
  }

  @Override
  public ObjectParser objectParser() { return null; }
}
