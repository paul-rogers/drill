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
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;

public class RepeatedListValueListener extends AbstractValueListener {

  private final ObjectWriter repeatedListWriter;
  private final RepeatedArrayListener outerArrayListener;

  public RepeatedListValueListener(JsonLoaderImpl loader, ObjectWriter writer) {
    super(loader);
    this.repeatedListWriter = writer;
    ColumnMetadata elementSchema = writer.schema().childSchema();
    ArrayWriter outerArrayWriter = writer.array();
    ArrayWriter innerArrayWriter = outerArrayWriter.array();
    if (elementSchema.isVariant()) {
      // Not yet
      throw new UnsupportedOperationException();
    } else if (elementSchema.isMap()) {
      this.outerArrayListener = new RepeatedObjectArrayListener(loader, outerArrayWriter,
          new ObjectValueListener(loader, outerArrayWriter.entry().schema(),
              new TupleListener(loader, innerArrayWriter.tuple(), null)));
    } else {
      this.outerArrayListener = new RepeatedArrayListener(loader, outerArrayWriter,
          ScalarListener.listenerFor(loader, outerArrayWriter.entry()));
    }
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

  private static class RepeatedArrayListener extends AbstractArrayListener {

    private final ArrayWriter repeatedListWriter;

    public RepeatedArrayListener(JsonLoaderImpl loader, ArrayWriter arrayWriter, ValueListener elementListener) {
      super(loader, arrayWriter.schema(), elementListener);
      this.repeatedListWriter = arrayWriter;
    }

    @Override
    public ValueListener element(ValueDef valueDef) {
      return elementListener;
    }

    @Override
    public void onStart(int level) {
      if (level > 2) {
        loader.unsupportedArrayException(repeatedListWriter.schema().name(), level);
      }
    }

    @Override
    public void onEnd(int level) {
      if (level == 2) {
        repeatedListWriter.save();
      }
    }
  }

  private static class RepeatedObjectArrayListener extends RepeatedArrayListener {
    protected final ArrayWriter innerArrayWriter;

    public RepeatedObjectArrayListener(JsonLoaderImpl loader,
        ArrayWriter arrayWriter, ValueListener elementListener) {
      super(loader, arrayWriter, elementListener);
      this.innerArrayWriter = arrayWriter.array();
   }

    @Override
    public void onElementEnd() {
      innerArrayWriter.save();
    }
  }
}
