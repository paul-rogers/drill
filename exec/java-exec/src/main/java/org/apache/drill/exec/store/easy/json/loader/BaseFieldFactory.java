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

import java.util.function.Function;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ObjectArrayListener;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ScalarArrayListener;
import org.apache.drill.exec.store.easy.json.loader.RepeatedListValueListener.RepeatedArrayListener;
import org.apache.drill.exec.store.easy.json.loader.RepeatedListValueListener.RepeatedListElementListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ArrayValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectArrayValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ScalarArrayValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.BigIntListener;
import org.apache.drill.exec.store.easy.json.loader.values.BooleanListener;
import org.apache.drill.exec.store.easy.json.loader.values.DoubleListener;
import org.apache.drill.exec.store.easy.json.loader.values.ScalarListener;
import org.apache.drill.exec.store.easy.json.loader.values.VarCharListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ElementParser.ValueParser;
import org.apache.drill.exec.store.easy.json.parser.FieldParserFactory;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
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

  protected FieldParserFactory parserFactory() {
    return loader().parser().fieldFactory();
  }

  @Override
  public ValueParser ignoredFieldParser() {
    return parserFactory().ignoredFieldParser();
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
    return new ScalarArrayValueListener(loader(),
        new ScalarArrayListener(loader(),
            scalarListenerFor(colSchema)));
  }

  /**
   * Create a scalar column and listener given the column schema.
   */
  public ScalarListener scalarListenerFor(ColumnMetadata colSchema) {
    return scalarListenerFor(loader(),
        tupleListener.fieldwriterFor(colSchema));
  }

  /**
   * Create a multi- (2+) dimensional scalar array from a column schema and dimension
   * count hint.
   */
  protected ValueListener multiDimScalarArrayListenerFor(ColumnMetadata colSchema, int dims) {
    return multiDimScalarArrayFor(loader(),
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
    return new ObjectValueListener(loader(),
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
    return new ObjectArrayValueListener(loader(),
        new ObjectArrayListener(loader(), arrayWriter,
            new ObjectValueListener(loader(),
                new TupleListener(loader(), arrayWriter.tuple(), providedSchema))));
  }

  /**
   * Create a multi- (2+) dimensional scalar array from a column schema, dimension
   * count hint, and optional provided schema.
   */
  protected ValueListener multiDimObjectArrayListenerFor(ColumnMetadata colSchema,
      int dims, TupleMetadata providedSchema) {
    return multiDimObjectArrayFor(loader(),
        tupleListener.fieldwriterFor(colSchema), dims, providedSchema);
  }

  /**
   * Create a multi- (2+) dimensional variant array from a column schema and dimension
   * count hint. This is actually an (n-1) dimensional array of lists, where a LISt
   * is a repeated UNION.
   */
  protected ValueListener multiDimVariantArrayListenerFor(ColumnMetadata colSchema, int dims) {
    return repeatedVariantListFor(loader(),
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

  public static ScalarListener scalarListenerFor(JsonLoaderImpl loader, ObjectWriter colWriter) {
    ScalarWriter writer = colWriter.type() == ObjectType.ARRAY ?
        colWriter.array().scalar() : colWriter.scalar();
    switch (writer.schema().type()) {
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
        // TODO: Implement conversions for above
      default:
        throw loader.buildError(
            UserException.internalError(null)
              .message("Unsupported JSON reader type: %s",
                  writer.schema().type().name()));
    }
  }

  /**
   * Create a repeated list listener for a scalar value.
   */
  public static ValueListener multiDimScalarArrayFor(JsonLoaderImpl loader, ObjectWriter writer, int dims) {
    return buildOuterArrays(loader, writer, dims,
        innerWriter ->
          new ScalarArrayListener(loader,
              scalarListenerFor(loader, innerWriter))
        );
  }

  /**
   * Create a repeated list listener for a Map.
   */
  public static ValueListener multiDimObjectArrayFor(JsonLoaderImpl loader,
      ObjectWriter writer, int dims, TupleMetadata providedSchema) {
    return buildOuterArrays(loader, writer, dims,
        innerWriter ->
          new ObjectArrayListener(loader, innerWriter.array(),
              new ObjectValueListener(loader,
                  new TupleListener(loader, innerWriter.array().tuple(), providedSchema))));
  }

  /**
   * Create layers of repeated list listeners around the type-specific
   * array. If the JSON has three array levels, the outer two are repeated
   * lists, the inner is type-specific: say an array of {@code BIGINT} or
   * a map array.
   */
  public static ValueListener buildOuterArrays(JsonLoaderImpl loader, ObjectWriter writer, int dims,
      Function<ObjectWriter, ArrayListener> innerCreator) {
    ColumnMetadata colSchema = writer.schema();
    ObjectWriter writers[] = new ObjectWriter[dims];
    writers[0] = writer;
    for (int i = 1; i < dims; i++) {
      writers[i] = writers[i-1].array().entry();
    }
    ArrayListener prevArrayListener = innerCreator.apply(writers[dims - 1]);
    RepeatedArrayListener innerArrayListener = null;
    for (int i = dims - 2; i >= 0; i--) {
      innerArrayListener = new RepeatedArrayListener(loader, colSchema,
          writers[i].array(),
          new RepeatedListElementListener(loader, colSchema,
              writers[i+1].array(), prevArrayListener));
      prevArrayListener = innerArrayListener;
    }
    return new RepeatedListValueListener(loader, writer, innerArrayListener);
  }

  /**
   * Create a repeated list listener for a variant. Here, the inner
   * array is provided by a List (which is a repeated Union.)
   */
  public static ValueListener repeatedVariantListFor(JsonLoaderImpl loader,
      ObjectWriter writer) {
    return new RepeatedListValueListener(loader, writer,
        new ListListener(loader, writer.array().entry()));
  }
}
