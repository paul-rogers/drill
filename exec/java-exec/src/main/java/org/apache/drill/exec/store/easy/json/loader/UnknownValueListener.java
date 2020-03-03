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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.NullTypeMarker;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnknownValueListener extends AbstractValueListener implements NullTypeMarker {
  protected static final Logger logger = LoggerFactory.getLogger(UnknownValueListener.class);

  private final TupleWriter tupleWriter;
  private final String key;
  private ValueHost host;

  public UnknownValueListener(JsonLoaderImpl loader, TupleWriter tupleWriter, String key) {
    super(loader);
    this.tupleWriter = tupleWriter;
    this.key = key;
    loader.addNullMarker(this);
  }

  @Override
  public void onBoolean(boolean value) {
    resolveScalar(JsonType.BOOLEAN).onBoolean(value);
  }

  @Override
  public void onInt(long value) {
    resolveScalar(JsonType.INTEGER).onInt(value);
  }

  @Override
  public void onFloat(double value) {
    resolveScalar(JsonType.FLOAT).onFloat(value);
  }

  @Override
  public void onString(String value) {
    resolveScalar(JsonType.STRING).onString(value);
  }

  @Override
  public void onEmbedddObject(String value) {
    resolveScalar(JsonType.EMBEDDED_OBJECT).onEmbedddObject(value);
  }

  /**
   * The column type is now known from context. Create a new, scalar
   * column, writer and listener to replace ourself: this is the last
   * call that this listener will receive.
   */
  private ValueListener resolveScalar(JsonType type) {
    ValueListener newListener = ScalarListener.listenerFor(loader, tupleWriter, key, type);
    resolveTo(newListener);
    return newListener;
  }

  private void resolveTo(ValueListener newListener) {
    host.bindListener(newListener);
    loader.removeNullMarker(this);
  }

  @Override
  public ObjectListener object() {
    return resolveScalar(JsonType.OBJECT).object();
  }

  @Override
  public ArrayListener array(int arrayDims, JsonType type) {
    throw typeConversionError(type.name() + " array[" + arrayDims + "]");
  }

  @Override
  public ArrayListener objectArray(int arrayDims) {
    throw typeConversionError("object array[" + arrayDims + "]");
  }

  @Override
  public void bind(ValueHost host) {
    this.host = host;
  }

  @Override
  public void onNull() {
    // Ignore: still don't know what this is
  }

  @Override
  protected ColumnMetadata schema() {
    throw new IllegalStateException("Unknown column has no schema");
  }

  @Override
  public void forceResolution() {
    logger.warn("Ambiguous type! JSON field {}" +
        " contains all nulls. Assuming VARCHAR.",
        key);
    ColumnMetadata colSchema = MetadataUtils.newScalar(key, Types.optional(MinorType.VARCHAR));
//    colSchema.setProperty(ColumnMetadata.JSON_MODE, ColumnMetadata.JSON_TEXT_MODE);
    ValueListener newListener = ScalarListener.listenerFor(loader, tupleWriter, colSchema);
    resolveTo(newListener);
  }
}
