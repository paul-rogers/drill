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
import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;

public class ScalarListener implements ValueListener {

  protected final JsonLoaderImpl loader;
  protected final ScalarWriter writer;

  public ScalarListener(JsonLoaderImpl loader, ScalarWriter writer) {
    this.loader = loader;
    this.writer = writer;
  }

  public static ValueListener listenerFor(JsonLoaderImpl loader, TupleWriter parent, ColumnMetadata col) {
    ScalarWriter writer = parent.scalar(parent.addColumn(col));
    switch (col.type()) {
    case BIGINT:
      return new BigIntListener(loader, writer);
    case BIT:
      return new BooleanListener(loader, writer);
    case FLOAT8:
      return new DoubleListener(loader, writer);
    case VARCHAR:
      return new VarCharListener(loader, writer);
    case DATE:
    case FLOAT4:
    case INT:
    case INTERVAL:
    case INTERVALDAY:
    case INTERVALYEAR:
    case SMALLINT:
    case TIME:
    case TIMESTAMP:
    case VARBINARY:
    case VARDECIMAL:
      // TODO: Implement conversions for aoblve
    default:
      throw loader.buildError(
          UserException.internalError(null)
            .message("Unsupported JSON reader type: %s", col.type().name()));

    }
  }

  @Override
  public boolean isText() { return false; }

  @Override
  public void onNull() {
    try {
      writer.setNull();
    } catch (UnsupportedConversionError e) {
      throw buildError(
          UserException.dataReadError()
            .message("Null value encountered in JSON input where Drill does not allow nulls."));
    }
  }

  @Override
  public void onBoolean(boolean value) {
    throw typeConversionError("Boolean");
  }

  @Override
  public void onInt(long value) {
    throw typeConversionError("integer");
  }

  @Override
  public void onFloat(double value) {
    throw typeConversionError("float");
  }

  @Override
  public void onString(String value) {
    throw typeConversionError("string");
  }

  @Override
  public void onEmbedddObject(String value) {
    throw typeConversionError("object");
  }

  @Override
  public ObjectListener object() {
    throw typeConversionError("object");
  }

  @Override
  public ArrayListener array(int arrayDims, JsonType type) {
    throw typeConversionError(type.name() + " array[" + arrayDims + "]");
  }

  @Override
  public ArrayListener objectArray(int arrayDims) {
    throw typeConversionError("object array[" + arrayDims + "]");
  }

  protected UserException typeConversionError(String tokenType) {
    return buildError(
        UserException.dataReadError()
          .message("Type of JSON token is not compatible with its column")
          .addContext("JSON token type", tokenType));
  }

  protected UserException dataConversionError(String tokenType, String value) {
    return buildError(
        UserException.dataReadError()
          .message("Type of JSON token is not compatible with its column")
          .addContext("JSON token type", tokenType)
          .addContext("JSON token", value));
  }

  protected UserException buildError(UserException.Builder builder) {
    return loader.buildError(builder
        .addContext("Column", writer.schema().name())
        .addContext("Column type", writer.schema().typeString()));
  }
}
