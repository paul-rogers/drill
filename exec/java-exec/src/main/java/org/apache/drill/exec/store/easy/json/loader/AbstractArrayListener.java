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
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.ScalarListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;

/**
 * Base class for scalar and object arrays. Represents the array
 * behavior of a field.
 */
public abstract class AbstractArrayListener implements ArrayListener {

  protected final JsonLoaderImpl loader;

  public AbstractArrayListener(JsonLoaderImpl loader) {
    this.loader = loader;
  }

  protected abstract ColumnMetadata schema();

  @Override
  public void onStart() { }

  @Override
  public void onElementStart() { }

  @Override
  public void onElementEnd() { }

  @Override
  public void onEnd() { }

  public static class ScalarArrayListener extends AbstractArrayListener {

    public ScalarArrayListener(JsonLoaderImpl loader) {
      super(loader);
    }
  }

  public static class ObjectArrayListener extends AbstractArrayListener {
    private final ArrayWriter arrayWriter;

    public ObjectArrayListener(JsonLoaderImpl loader, ArrayWriter arrayWriter) {
      super(loader);
      this.arrayWriter = arrayWriter;
    }

    @Override
    public void onElementEnd() {
      arrayWriter.save();
    }

    @Override
    protected ColumnMetadata schema() {
      return arrayWriter.schema();
    }
  }
}
