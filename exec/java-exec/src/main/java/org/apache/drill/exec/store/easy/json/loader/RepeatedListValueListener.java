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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Represents a JSON value that holds a RepeatedList (2D array) value.
 * The structure is:
 * <ul>
 * <li>Value - {@code RepeatedListValueListener}</li>
 * <li>Array - {@code RepeatedArrayListener}</li>
 * <li>Value - {@code RepeatedListElementListener} or
 * {@code ListListener}</li>
 * <li>Array - Depends on type</li>
 * <li>Value - Depends on type</li>
 * <li>Object - If a repeated list of maps</li>
 * </ul>
 */
public class RepeatedListValueListener extends AbstractValueListener {

  private final ObjectWriter repeatedListWriter;
  private final RepeatedArrayListener outerArrayListener;

  protected RepeatedListValueListener(JsonLoaderImpl loader, ObjectWriter writer,
      ValueListener elementListener) {
    this(loader,  writer,
        new RepeatedArrayListener(loader, writer.schema(),
            writer.array(), elementListener));
  }

  protected RepeatedListValueListener(JsonLoaderImpl loader, ObjectWriter writer,
      RepeatedArrayListener outerArrayListener) {
    super(loader);
    this.repeatedListWriter = writer;
    this.outerArrayListener = outerArrayListener;
  }
  @Override
  public ArrayListener array(ValueDef valueDef) {
    return outerArrayListener;
  }

  @Override
  public void onValue(JsonToken token, TokenIterator tokenizer) {
    if (token != JsonToken.VALUE_NULL) {
      super.onValue(token, tokenizer);
    }
  }

  @Override
  protected ColumnMetadata schema() {
    return repeatedListWriter.schema();
  }

  /**
   * Represents the outer array for a repeated (2D) list
   */
  protected static class RepeatedArrayListener extends AbstractArrayListener {

    private final ArrayWriter outerArrayWriter;
    private final ColumnMetadata colMetadata;

    public RepeatedArrayListener(JsonLoaderImpl loader,
        ColumnMetadata colMetadata, ArrayWriter outerArrayWriter,
        ValueListener outerValue) {
      super(loader, outerValue);
      this.colMetadata = colMetadata;
      this.outerArrayWriter = outerArrayWriter;
    }

    @Override
    public ValueListener element(ValueDef valueDef) {
      return elementListener;
    }

    @Override
    public void onElementEnd() {
      outerArrayWriter.save();
    }

    @Override
    protected ColumnMetadata schema() {
      return colMetadata;
    }
  }

  /**
   * Represents each item in the outer array of a RepeatedList. Such elements should
   * only be arrays. However, Drill is forgiving if the value happens to be null, which
   * is defined to be the same as an empty inner array.
   */
  protected static class RepeatedListElementListener extends AbstractValueListener {

    private final ColumnMetadata colMetadata;
    private final ArrayListener innerArrayListener;
    private final ArrayWriter innerArrayWriter;

    public RepeatedListElementListener(JsonLoaderImpl loader, ColumnMetadata colMetadata,
        ArrayWriter innerArrayWriter, ArrayListener innerArrayListener) {
      super(loader);
      this.colMetadata = colMetadata;
      this.innerArrayListener = innerArrayListener;
      this.innerArrayWriter = innerArrayWriter;
    }

    @Override
    public ArrayListener array(ValueDef valueDef) {
      return innerArrayListener;
    }

    @Override
    public void onValue(JsonToken token, TokenIterator tokenizer) {
      if (token == JsonToken.VALUE_NULL) {
        innerArrayWriter.save();
      } else {
        super.onValue(token, tokenizer);
      }
    }

    @Override
    protected ColumnMetadata schema() {
      return colMetadata;
    }
  }
}
