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

import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ArrayValueListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnknownArrayListener implements ArrayListener {
  protected static final Logger logger = LoggerFactory.getLogger(UnknownArrayListener.class);

  private final UnknownArrayValueListener valueListener;

  public UnknownArrayListener(JsonLoaderImpl loader, TupleWriter tupleWriter, String key) {
    this.valueListener = new UnknownArrayValueListener(loader, tupleWriter, key);
    valueListener.arrayListener = this;
  }

  public static ValueListener listenerFor(JsonLoaderImpl loader, TupleWriter tupleWriter, String key) {
    return new UnknownArrayListener(loader, tupleWriter, key).valueListener;
  }

  @Override
  public void onStart() { }

  @Override
  public void onElementStart() { }

  @Override
  public void onElementEnd() { }

  @Override
  public void onEnd() { }

  @Override
  public ValueListener element(ValueDef valueDef) {
    return valueListener.resolveTo(valueDef);
  }

  public static class UnknownArrayValueListener extends UnknownValueListener {

    protected UnknownArrayListener arrayListener;

    public UnknownArrayValueListener(JsonLoaderImpl loader, TupleWriter tupleWriter, String key) {
      super(loader, tupleWriter, key);
    }

    @Override
    public ArrayListener array(ValueDef valueDef) {
      return arrayListener;
    }

    public ValueListener resolveTo(ValueDef valueDef) {
      ValueListener fieldListener = loader.listenerFor(tupleWriter, key,
          new ValueDef(valueDef.type(), valueDef.dimensions() + 1));
      resolveTo(fieldListener);
      ArrayValueListener arrayValueListener = (ArrayValueListener) fieldListener;
      return arrayValueListener.elementListener();
    }

    @Override
    public void forceResolution() {
      logger.warn("Ambiguous type! JSON field {} contains all nulls. Assuming VARCHAR.",
          key);
      resolveTo(AbstractArrayListener.listenerFor(loader, tupleWriter, key, JsonType.STRING));
    }
  }
}
