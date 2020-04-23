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
package org.apache.drill.exec.store.easy.json.loader.values;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.loader.AbstractValueListener;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;

/**
 * Base class for scalar field listeners
 */
public abstract class ScalarListener extends AbstractValueListener {

  protected final ScalarWriter writer;
  protected final boolean isArray;

  public ScalarListener(JsonLoaderImpl loader, ScalarWriter writer) {
    super(loader);
    this.writer = writer;
    isArray = writer.schema().isArray();
  }

  @Override
  public ColumnMetadata schema() { return writer.schema(); }

  protected void setNull() {
    try {
      if (isArray) {
        setArrayNull();
      } else {
        writer.setNull();
      }
    } catch (UnsupportedConversionError e) {
      throw loader.buildError(schema(),
          UserException.dataReadError()
            .message("Null value encountered in JSON input where Drill does not allow nulls."));
    }
  }

  protected void setArrayNull() {
    // Default is no "natural" null value
    throw loader.nullDisallowedError(schema());
  }

  @Override
  public ArrayListener array(ValueDef valueDef) {
    if (isArray) {
      valueDef = new ValueDef(valueDef.type(), valueDef.dimensions() + 1);
    }
    throw loader.typeConversionError(schema(), valueDef);
  }
}
