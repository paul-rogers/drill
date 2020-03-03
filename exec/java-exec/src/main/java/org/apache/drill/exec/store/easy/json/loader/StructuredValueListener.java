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
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ObjectArrayListener;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ScalarArrayListener;
import org.apache.drill.exec.store.easy.json.loader.TupleListener.MapListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public abstract class StructuredValueListener extends AbstractValueListener {

  private final ColumnMetadata colSchema;

  public StructuredValueListener(JsonLoaderImpl loader, ColumnMetadata colSchema) {
    super(loader);
    this.colSchema = colSchema;
  }

  @Override
  public ColumnMetadata schema() { return colSchema; }

  // Ignore array nulls: {a: null} is the same as omitting
  // array column a: an array of zero elements
  @Override
  public void onNull() { }

  public static abstract class ArrayValueListener extends StructuredValueListener {

    protected final AbstractArrayListener arrayListener;

    public ArrayValueListener(JsonLoaderImpl loader, ColumnMetadata colSchema, AbstractArrayListener arrayListener) {
      super(loader, colSchema);
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

    public ScalarArrayValueListener(JsonLoaderImpl loader, ColumnMetadata colSchema, ScalarArrayListener arrayListener) {
      super(loader, colSchema, arrayListener);
    }

    public static ArrayValueListener listenerFor(JsonLoaderImpl loader,
        TupleWriter tupleWriter, ColumnMetadata colSchema) {
      return new ScalarArrayValueListener(loader, colSchema,
          new ScalarArrayListener(loader, colSchema,
              ScalarListener.listenerFor(loader, tupleWriter, colSchema)));
    }

    @Override
    public ArrayListener array(ValueDef valueDef) {
      Preconditions.checkArgument(valueDef.dimensions() == 1);
      return arrayListener;
    }

    @Override
    public void onBoolean(boolean value) {
      elementListener().onBoolean(value);
    }

    @Override
    public void onInt(long value) {
      elementListener().onInt(value);
    }

    @Override
    public void onFloat(double value) {
      elementListener().onFloat(value);
    }

    @Override
    public void onString(String value) {
      elementListener().onString(value);
    }
  }

  public static class ObjectValueListener extends StructuredValueListener {

    private final ObjectListener tupleListener;

    public ObjectValueListener(JsonLoaderImpl loader, ColumnMetadata colSchema, ObjectListener tupleListener) {
      super(loader, colSchema);
      this.tupleListener = tupleListener;
    }

    public static ObjectValueListener listenerFor(JsonLoaderImpl loader,
        TupleWriter tupleWriter, String key, TupleMetadata providedSchema) {
      ColumnMetadata colSchema = MetadataUtils.newMap(key);
      int index = tupleWriter.addColumn(colSchema);
      return new ObjectValueListener(loader, colSchema,
          new MapListener(loader, tupleWriter.tuple(index), providedSchema));
    }

    @Override
    public ObjectListener object() {
      return tupleListener;
    }
  }

  public static class ObjectArrayValueListener extends ArrayValueListener {

    public ObjectArrayValueListener(JsonLoaderImpl loader,
        ColumnMetadata colSchema, ObjectArrayListener arrayListener) {
      super(loader, colSchema, arrayListener);
     }

    public static ArrayValueListener listenerFor(JsonLoaderImpl loader,
        TupleWriter tupleWriter, String key, TupleMetadata providedSchema) {
      ColumnMetadata colSchema = MetadataUtils.newMapArray(key);
      int index = tupleWriter.addColumn(colSchema);
      ArrayWriter arrayWriter = tupleWriter.array(index);
      return new ObjectArrayValueListener(loader, colSchema,
          new ObjectArrayListener(loader, arrayWriter,
              new ObjectValueListener(loader, colSchema,
                  new MapListener(loader, arrayWriter.tuple(), providedSchema))));
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
