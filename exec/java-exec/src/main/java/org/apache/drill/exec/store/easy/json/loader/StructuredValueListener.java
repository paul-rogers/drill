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
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ObjectArrayListener;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ScalarArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Base class for structured value listeners: arrays and objects.
 * Contains the concrete implementations as nested static classes.
 */
public abstract class StructuredValueListener extends AbstractValueListener {

  public StructuredValueListener(JsonLoaderImpl loader) {
    super(loader);
  }

  @Override
  public void onValue(JsonToken token, TokenIterator tokenizer) {
    // Ignore array nulls: {a: null} is the same as omitting
    // array column a: an array of zero elements
    if (token != JsonToken.VALUE_NULL) {
      super.onValue(token, tokenizer);
    }
  }

  /**
   * Abstract base class for array values which hold a nested array
   * listener.
   */
  public static abstract class ArrayValueListener extends StructuredValueListener {

    protected final AbstractArrayListener arrayListener;

    public ArrayValueListener(JsonLoaderImpl loader, AbstractArrayListener arrayListener) {
      super(loader);
      this.arrayListener = arrayListener;
    }

    public AbstractArrayListener arrayListener() { return arrayListener; }

    public ValueListener elementListener() { return arrayListener.elementListener(); }
  }

  /**
   * Value listener for a scalar array (Drill repeated primitive).
   * Maps null values for the entire array to an empty array.
   * Maps a scalar to an array with a single value.
   */
  public static class ScalarArrayValueListener extends ArrayValueListener {

    public ScalarArrayValueListener(JsonLoaderImpl loader, ScalarArrayListener arrayListener) {
      super(loader, arrayListener);
    }

    @Override
    public ArrayListener array(ValueDef valueDef) {
      Preconditions.checkArgument(valueDef.dimensions() == 1);
      return arrayListener;
    }

    @Override
    public void onValue(JsonToken token, TokenIterator tokenizer) {
      // Treat null as an empty array
      if (token != JsonToken.VALUE_NULL) {
        elementListener().onValue(token, tokenizer);
      }
    }

    @Override
    public void onText(String value) {
      elementListener().onText(value);
    }

    @Override
    protected ColumnMetadata schema() {
      return ((AbstractValueListener) elementListener()).schema();
    }
  }

  /**
   * Value listener for object (MAP) values.
   */
  public static class ObjectValueListener extends StructuredValueListener {

    private final TupleListener tupleListener;

    public ObjectValueListener(JsonLoaderImpl loader, TupleListener tupleListener) {
      super(loader);
      this.tupleListener = tupleListener;
    }

    @Override
    public ObjectListener object() {
      return tupleListener;
    }

    @Override
    protected ColumnMetadata schema() {
      return tupleListener.writer().schema();
    }
  }

  /**
   * Value listener for object array (repeated MAP) values.
   */
  public static class ObjectArrayValueListener extends ArrayValueListener {

    public ObjectArrayValueListener(JsonLoaderImpl loader,
        ObjectArrayListener arrayListener) {
      super(loader, arrayListener);
     }

    @Override
    public ArrayListener array(ValueDef valueDef) {
      Preconditions.checkArgument(valueDef.dimensions() == 1);
      // Called with a provided schema where the initial array
      // value is empty.
      Preconditions.checkArgument(!valueDef.type().isScalar());
      return arrayListener;
    }
  }
}
