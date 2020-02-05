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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ObjectWriter;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses a top-level array: <code>^[ ... ]$<code>
 * <p>
 * Drill's JSON supports "proper" JSON: an array of top-level JSON
 * objects.
 */
class RootArrayParser extends ContainerParser {

  private final RowSetLoader rootWriter;
  private final ObjectParser rootTuple;

  public RootArrayParser(JsonLoaderImpl loader, RowSetLoader rootWriter, TupleProjection projection) {
    super(loader, JsonLoaderImpl.ROOT_NAME, projection);
    this.rootWriter = rootWriter;
    this.rootTuple = new ObjectParser(this, key() + "[]", rootWriter, projection);
  }

  @Override
  public boolean parse() {
    rootWriter.start();
    JsonToken token = loader.tokenizer.requireNext();
    if (token == JsonToken.END_ARRAY) {
      return false;
    }
    loader.tokenizer.unget(token);
    rootTuple.parse();
    return true;
  }

  @Override
  public ColumnMetadata schema() { return null; }

  @Override
  protected ObjectWriter newWriter(String key, MinorType type,
      DataMode mode) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected JsonElementParser nullArrayParser(String key) {
    throw new UnsupportedOperationException();
  }
}
