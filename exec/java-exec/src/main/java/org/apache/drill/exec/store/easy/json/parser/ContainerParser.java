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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonElementParser;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

import com.fasterxml.jackson.core.JsonToken;

public abstract class ContainerParser extends AbstractParser {

  public ContainerParser(JsonLoaderImpl loader, String key) {
    super(loader, key);
  }

  public ContainerParser(JsonElementParser parent, String key) {
    super(parent, key);
  }

  public MinorType typeForToken(JsonToken token) {
    if (loader.options.allTextMode) {
      return MinorType.VARCHAR;
    }
    switch (token) {
    case VALUE_FALSE:
    case VALUE_TRUE:
      return MinorType.TINYINT;

    case VALUE_NUMBER_INT:
      if (! loader.options.readNumbersAsDouble) {
        return MinorType.BIGINT;
      } // else fall through

    case VALUE_NUMBER_FLOAT:
      return MinorType.FLOAT8;

    case VALUE_STRING:
      return MinorType.VARCHAR;

    default:
      throw loader.syntaxError(token);
    }
  }

  public AbstractParser scalarParserForToken(JsonToken token, String key, ScalarWriter writer) {
    if (loader.options.allTextMode) {
      return new ScalarParser.TextParser(this, key, writer);
    }
    switch (token) {
    case VALUE_FALSE:
    case VALUE_TRUE:
      return new ScalarParser.BooleanParser(this, key, writer);

    case VALUE_NUMBER_INT:
      if (! loader.options.readNumbersAsDouble) {
        return new ScalarParser.IntParser(this, key, writer);
      } // else fall through

    case VALUE_NUMBER_FLOAT:
      return new ScalarParser.FloatParser(this, key, writer);

    case VALUE_STRING:
      return new ScalarParser.StringParser(this, key, writer);

    default:
      throw loader.syntaxError(token);
    }
  }

  protected void replaceChild(String key, JsonElementParser newParser) {
    throw new UnsupportedOperationException();
  }
}