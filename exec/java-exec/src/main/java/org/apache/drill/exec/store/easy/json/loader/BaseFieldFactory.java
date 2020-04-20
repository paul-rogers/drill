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
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ArrayValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectArrayValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ScalarArrayValueListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

import jersey.repackaged.com.google.common.base.Preconditions;

/**
 * Base field factor class which handles the common tasks for
 * building column writers and JSON listeners.
 */
public abstract class BaseFieldFactory implements FieldFactory {
  protected final TupleListener tupleListener;
  protected final FieldFactory child;

  public BaseFieldFactory(TupleListener tupleListener) {
    this(tupleListener, null);
  }

  public BaseFieldFactory(TupleListener tupleListener, FieldFactory child) {
    this.tupleListener = tupleListener;
    this.child = child;
  }

  @Override
  public ValueListener resolveField(String key, ValueDef valueDef) {
    Preconditions.checkState(child != null);
    return child.resolveField(key, valueDef);
  }

  protected JsonLoaderImpl loader() { return tupleListener.loader(); }

  public TupleWriter writer() { return tupleListener.writer(); }

  /**
   * Create a scalar array column and array listener for the given column
   * schema.
   */
  protected ArrayValueListener scalarArrayListenerFor(ColumnMetadata colSchema) {
    return new ScalarArrayValueListener(loader(), colSchema,
        new ScalarArrayListener(loader(), colSchema,
            scalarListenerFor(colSchema)));
  }

  /**
   * Create a scalar column and listener given the column schema.
   */
  public ScalarListener scalarListenerFor(ColumnMetadata colSchema) {
    return ScalarListener.listenerFor(loader(),
        tupleListener.fieldwriterFor(colSchema));
  }

  /**
   * Create a multi- (2+) dimensional scalar array from a column schema and dimension
   * count hint.
   */
  protected ValueListener multiDimScalarArrayListenerFor(ColumnMetadata colSchema, int dims) {
    return RepeatedListValueListener.multiDimScalarArrayFor(loader(),
        tupleListener.fieldwriterFor(colSchema), dims);
  }

  /**
   * Create a map column and its associated object value listener for the
   * a JSON object value given the value's key.
   */
  public ObjectValueListener objectListenerFor(String key) {
    return objectListenerFor(MetadataUtils.newMap(key), null);
  }

  /**
   * Create a map column and its associated object value listener for the
   * given key and optional provided schema.
   */
  protected ObjectValueListener objectListenerFor(ColumnMetadata colSchema, TupleMetadata providedSchema) {
    return new ObjectValueListener(loader(), colSchema,
        new TupleListener(loader(), tupleListener.fieldwriterFor(colSchema).tuple(),
            providedSchema));
  }

  /**
   * Create a map array column and its associated object array listener
   * for the given column schema and optional provided schema.
   */
  protected ArrayValueListener objectArrayListenerFor(
      ColumnMetadata colSchema, TupleMetadata providedSchema) {
    ArrayWriter arrayWriter = tupleListener.fieldwriterFor(colSchema).array();
    return new ObjectArrayValueListener(loader(), colSchema,
        new ObjectArrayListener(loader(), arrayWriter,
            new ObjectValueListener(loader(), colSchema,
                new TupleListener(loader(), arrayWriter.tuple(), providedSchema))));
  }

  /**
   * Create a multi- (2+) dimensional scalar array from a column schema, dimension
   * count hint, and optional provided schema.
   */
  protected ValueListener multiDimObjectArrayListenerFor(ColumnMetadata colSchema,
      int dims, TupleMetadata providedSchema) {
    return RepeatedListValueListener.multiDimObjectArrayFor(loader(),
        tupleListener.fieldwriterFor(colSchema), dims, providedSchema);
  }

  /**
   * Create a multi- (2+) dimensional variant array from a column schema and dimension
   * count hint. This is actually an (n-1) dimensional array of lists, where a LISt
   * is a repeated UNION.
   */
  protected ValueListener multiDimVariantArrayListenerFor(ColumnMetadata colSchema, int dims) {
    return RepeatedListValueListener.repeatedVariantListFor(loader(),
        tupleListener.fieldwriterFor(colSchema));
  }

  /**
   * Build up a repeated list column definition given a specification of the
   * number of dimensions and the JSON type. Creation of the element type is
   * via a closure that builds the needed schema.
   */
  protected ColumnMetadata repeatedListSchemaFor(String key, int dims,
      ColumnMetadata innerArray) {
    ColumnMetadata prev = innerArray;
    for (int i = 1; i < dims; i++) {
      prev = MetadataUtils.newRepeatedList(key, prev);
    }
    return prev;
  }
}
