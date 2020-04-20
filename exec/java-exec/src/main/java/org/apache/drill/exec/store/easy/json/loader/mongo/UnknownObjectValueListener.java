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

import java.util.function.Consumer;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.loader.AbstractValueListener;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.loader.TupleListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener.FieldType;

public class UnknownObjectValueListener extends AbstractValueListener {

  private final ExtendedTypeFieldFactory fieldFactory;
  protected final String fieldName;
  protected Consumer<ValueListener> host;
  protected final UnknownObjectListener objectListener;

  public UnknownObjectValueListener(ExtendedTypeFieldFactory fieldFactory, String fieldName) {
    super(fieldFactory.loader());
    this.fieldFactory = fieldFactory;
    this.fieldName = fieldName;
    this.objectListener = new UnknownObjectListener(this);
  }

  @Override
  public void bind(Consumer<ValueListener> host) {
    this.host = host;
  }

  @Override
  public void onNull() {
    // Unknown tuple fields are short lived, from "{" to the first field
    // or the closing "}". There is no room for a null value.
    throw new IllegalStateException("Unknown tuple fields should never be null");
  }

  @Override
  public ObjectListener object() {
    return objectListener;
  }

  protected ObjectValueListener becomeMap() {
    return fieldFactory.becomeMap(fieldName);
  }

  /**
   * When parsing Mongo-style JSON, and we encounter a map, we must
   * wait to see which fields appear in the map to decide if it is a
   * real map, or a map-encoded scalar. Example of a Mongo map-encoded
   * scalar:<pre><code>
   * { myDate: { "$date" : 1436241583488 }, ... }</code></pre>
   * <p>
   * This listener is replaced with either a type-specific listener
   * or a normal tuple listener once we know the type.
   */
  public class UnknownObjectListener implements ObjectListener {

    private final UnknownObjectValueListener parentField;
    private Consumer<ObjectListener> host;

     protected UnknownObjectListener(UnknownObjectValueListener parentField) {
      this.parentField = parentField;
   }

    @Override
    public void bind(Consumer<ObjectListener> host) {
      this.host = host;
    }

    @Override
    public void onStart() { }

    @Override
    public FieldType fieldType(String key) {
      if (!parentField.fieldFactory.isExtendedTypeName(key)) {
        becomeMap();
      }
      return FieldType.TYPED;
    }

    @Override
    public ValueListener addField(String key, ValueDef valueDef) {
      // TODO: This won't work: We are already inside the map; need to
      // resync on this new map somehow.
      return parentField.fieldFactory.extendedTypeListenerFor(key);
    }

    @Override
    public void onEnd() {
      // Did not see a Mongo-style field, so this is not an extended
      // type.
      becomeMap();
    }
  }
}
