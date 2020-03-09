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
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ObjectArrayListener;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ScalarArrayListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;

/**
 * Represents a JSON value that holds a RepeatedList (2D array) value
 */
public class RepeatedListValueListener extends AbstractValueListener {

  private final ObjectWriter repeatedListWriter;
  private final RepeatedArrayListener outerArrayListener;

  private RepeatedListValueListener(JsonLoaderImpl loader, ObjectWriter writer,
      ArrayListener innerArrayListener) {
    super(loader);
    this.repeatedListWriter = writer;
    ArrayWriter outerArrayWriter = writer.array();
    ArrayWriter innerArrayWriter = outerArrayWriter.array();
    this.outerArrayListener = new RepeatedArrayListener(loader, writer.schema(),
        outerArrayWriter,
        new RepeatedListElementListener(loader,
            writer.schema(), innerArrayWriter, innerArrayListener));
  }

  public static ValueListener repeatedListFor(JsonLoaderImpl loader, ObjectWriter writer) {
    ColumnMetadata elementSchema = writer.schema().childSchema();
    ArrayWriter outerArrayWriter = writer.array();
    ArrayListener innerArrayListener;
    if (elementSchema.isVariant()) {
      // Not yet
      throw new UnsupportedOperationException();
    } else {
      innerArrayListener = new ScalarArrayListener(loader, elementSchema,
          ScalarListener.listenerFor(loader, outerArrayWriter.entry()));
    }
    return new RepeatedListValueListener(loader, writer, innerArrayListener);
  }

  public static ValueListener repeatedObjectListFor(JsonLoaderImpl loader,
      ObjectWriter writer, TupleMetadata providedSchema) {
    ArrayWriter outerArrayWriter = writer.array();
    ArrayWriter innerArrayWriter = outerArrayWriter.array();
    ArrayListener innerArrayListener =
        new ObjectArrayListener(loader, innerArrayWriter,
            new ObjectValueListener(loader, outerArrayWriter.entry().schema(),
                new TupleListener(loader, innerArrayWriter.tuple(), providedSchema)));
    return new RepeatedListValueListener(loader, writer, innerArrayListener);
  }

  @Override
  public ArrayListener array(ValueDef valueDef) {
    return outerArrayListener;
  }

  @Override
  public void onNull() { }

  @Override
  protected ColumnMetadata schema() {
    return repeatedListWriter.schema();
  }

  /**
   * Represents the outer array for a repeated (2D) list
   */
  private static class RepeatedArrayListener extends AbstractArrayListener {

    private final ArrayWriter outerArrayWriter;

    public RepeatedArrayListener(JsonLoaderImpl loader,
        ColumnMetadata colMetadata, ArrayWriter outerArrayWriter,
        RepeatedListElementListener outerValue) {
      super(loader, colMetadata, outerValue);
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
  }

  /**
   * Represents each item in the outer array of a RepeatedList. Such elements should
   * only be arrays. However, Drill is forgiving if the value happens to be null, which
   * is defined to be the same as an empty inner array.
   */
  private static class RepeatedListElementListener extends AbstractValueListener {

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
    public void onNull() {
      innerArrayWriter.save();
    }

    @Override
    protected ColumnMetadata schema() {
      return colMetadata;
    }
  }
}
