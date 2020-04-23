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
package org.apache.drill.exec.store.easy.json.loader;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ElementParser.ValueParser;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Abstract base class for value (field or array element) listeners.
 */
public abstract class AbstractValueListener implements ValueListener {

  protected final JsonLoaderImpl loader;

  public AbstractValueListener(JsonLoaderImpl loader) {
    this.loader = loader;
  }

  @Override
  public void bind(ValueParser parser) { }

  @Override
  public void onValue(JsonToken token, TokenIterator tokenizer) {
    throw typeConversionError(token.name());
  }

  @Override
  public void onText(String value) {
    throw typeConversionError("text");
  }

  @Override
  public ObjectListener object() {
    throw typeConversionError("object");
  }

  @Override
  public ArrayListener array(ValueDef valueDef) {
    throw loader.typeConversionError(schema(), valueDef);
  }

  protected UserException typeConversionError(String jsonType) {
    return loader.typeConversionError(schema(), jsonType);
  }

  protected ColumnMetadata schema() {
    throw new IllegalStateException("Unknown column has no schema");
  }
}
