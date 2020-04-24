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

public abstract class DynamicValueParser extends ValueParser {

  public DynamicValueParser(JsonStructureParser structParser) {
    super(structParser);
  }

  /**
   * Parses <code>true | false | null | integer | float | string|
   *              embedded-object | { ... } | [ ... ]</code>
   */
  @Override
  public void parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    switch (token) {
    case START_OBJECT:
      // Position: { ^
      if (objectParser == null) {
        // No object parser yet. May be that the value was null,
        // or may be that it changed types.
        addObjectParser();
      }
      objectParser.parse(tokenizer);
      break;

    case START_ARRAY:
      // Position: [ ^
      if (arrayParser == null) {
        // No array parser yet. May be that the value was null,
        // or may be that it changed types.
        addArrayParser(ValueDefFactory.arrayLookAhead(tokenizer));
      }
      arrayParser.parse(tokenizer);
      break;

    default:
      parseValue(tokenizer, token);
    }
  }

  protected abstract void parseValue(TokenIterator tokenizer, JsonToken token);

  private void addArrayParser(ValueDef valueDef) {
    ArrayParser arrayImpl = new ArrayParser(structParser, listener().array(valueDef));
    bindArrayParser(arrayImpl);
    arrayImpl.expandStructure(this, valueDef);
  }

  public void expandStructure(ValueDef valueDef) {
    if (valueDef.isArray()) {
      addArrayParser(valueDef);
    } else if (valueDef.type().isObject()) {
      addObjectParser();
    }
  }
}
