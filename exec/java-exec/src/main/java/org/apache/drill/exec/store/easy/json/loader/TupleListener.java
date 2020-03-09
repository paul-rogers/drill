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
import org.apache.drill.exec.record.metadata.RepeatedListBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ObjectArrayListener;
import org.apache.drill.exec.store.easy.json.loader.AbstractArrayListener.ScalarArrayListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ArrayValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectArrayValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ScalarArrayValueListener;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Accepts { name : value ... }
 * <p>
 * The structure parser maintains a map of known fields. Each time a
 * field is parsed, looks up the field in the map. If not found, the parser
 * looks ahead to find a value token, if any, and calls this class to add
 * a new column. This class creates a column writer based either on the
 * type provided in a provided schema, or inferred from the JSON token.
 * <p>
 * As it turns out, most of the semantic action occurs at the tuple level:
 * that is where fields are defined, types inferred, and projection is
 * computed.
 *
 * <h4>Nulls</h4>
 *
 * Much code here deals with null types, especially leading nulls, leading
 * empty arrays, and so on. The object parser creates a parser for each
 * value; a parser which "does the right thing" based on the data type.
 * For example, for a Boolean, the parser recognizes {@code true},
 * {@code false} and {@code null}.
 * <p>
 * But what happens if the first value for a field is {@code null}? We
 * don't know what kind of parser to create because we don't have a schema.
 * Instead, we have to create a temporary placeholder parser that will consume
 * nulls, waiting for a real type to show itself. Once that type appears, the
 * null parser can replace itself with the correct form. Each vector's
 * "fill empties" logic will back-fill the newly created vector with nulls
 * for prior rows.
 * <p>
 * Two null parsers are needed: one when we see an empty list, and one for
 * when we only see {@code null}. The one for {@code null{@code  must morph into
 * the one for empty lists if we see:<br>
 * {@code {a: null} {a: [ ]  }}<br>
 * <p>
 * If we get all the way through the batch, but have still not seen a type,
 * then we have to guess. A prototype type system can tell us, otherwise we
 * guess {@code VARCHAR}. ({@code VARCHAR} is the right choice for all-text
 * mode, it is as good a guess as any for other cases.)
 *
 * <h4>Projection List Hints</h4>
 *
 * To help, we consult the projection list, if any, for a column. If the
 * projection is of the form {@code a[0]}, we know the column had better
 * be an array. Similarly, if the projection list has {@code b.c}, then
 * {@code b} had better be an object.
 *
 * <h4>Array Handling</h4>
 *
 * The code here handles arrays in two ways. JSON normally uses the
 * {@code LIST} type. But, that can be expensive if lists are
 * well-behaved. So, the code here also implements arrays using the
 * classic {@code REPEATED} types. The repeated type option is disabled
 * by default. It can be enabled, for efficiency, if Drill ever supports
 * a JSON schema. If an array is well-behaved, mark that column as able
 * to use a repeated type.
 *
 * <h4>Ambiguous Types</h4>
 *
 * JSON nulls are untyped. A run of nulls does not tell us what type will
 * eventually appear. The best solution is to provide a schema. Without a
 * schema, the code is forgiving: defers selection of the column type until
 * the first non-null value (or, forces a type at the end of the batch.)
 * <p>
 * For scalars the pattern is: <code>{a: null} {a: "foo"}</code>. Type
 * selection happens on the value {@code "foo"}.
 * <p>
 * For arrays, the pattern is: <code>{a: []} {a: ["foo"]}</code>. Type
 * selection happens on the first array element. Note that type selection
 * must happen on the first element, even if tha element is null (which,
 * as we just said, ambiguous.)
 * <p>
 * If we are forced to pick a type (because we hit the end of a batch, or
 * we see {@code [null]}, then we pick {@code VARCHAR} as we allow any
 * scalar to be converted to {@code VARCHAR}. This helps for a single-file
 * query, but not if multiple fragments each make their own (inconsistent)
 * decisions. Only a schema provides a consistent answer.
 */
public class TupleListener implements ObjectListener {

  protected final JsonLoaderImpl loader;
  protected final TupleWriter tupleWriter;
  private final TupleMetadata providedSchema;

  public TupleListener(JsonLoaderImpl loader, TupleWriter tupleWriter, TupleMetadata providedSchema) {
    this.loader = loader;
    this.tupleWriter = tupleWriter;
    this.providedSchema = providedSchema;
  }

  public JsonLoaderImpl loader() { return loader; }

  @Override
  public void onStart() { }

  @Override
  public void onEnd() { }

  @Override
  public FieldType fieldType(String key) {
    // tupleWriter.isProjected(key); // TODO
    ColumnMetadata providedCol = providedColumn(key);
    if (providedCol == null) {
      return FieldType.TYPED;
    }
    String mode = providedCol.property(ColumnMetadata.JSON_MODE);
    if (mode == null) {
      return FieldType.TYPED;
    }
    switch (mode) {
      case ColumnMetadata.JSON_TEXT_MODE:
        return FieldType.TEXT;
      case ColumnMetadata.JSON_LITERAL_MODE:
        return FieldType.JSON;
      default:
        return FieldType.TYPED;
    }
  }

  @Override
  public ValueListener addField(String key, ValueDef valueDef) {
    ColumnMetadata colSchema = providedColumn(key);
    if (colSchema != null) {
      return listenerFor(colSchema);
    } else {
      return listenerFor(key, valueDef);
    }
  }

  protected ValueListener listenerFor(String key, ValueDef valueDef) {
    if (!valueDef.isArray()) {
      if (valueDef.type().isUnknown()) {
        return unknownListenerFor(key);
      } else if (valueDef.type().isObject()) {
        return objectListenerFor(key, null);
      } else {
        return scalarListenerFor(key, valueDef.type());
      }
    } else if (valueDef.dimensions() == 1) {
      if (valueDef.type().isUnknown()) {
        return unknownArrayListenerFor(key, valueDef);
      } else if (valueDef.type().isObject()) {
        return objectArrayListenerFor(key, null);
      } else {
        return arrayListenerFor(key, valueDef.type());
      }
    } else if (valueDef.dimensions() == 2) {
      if (valueDef.type().isUnknown()) {
        return unknownArrayListenerFor(key, valueDef);
      } else if (valueDef.type().isObject()) {
        return repeatedListOfObjectsListenerFor(key, null);
      } else {
        return repeatedListListenerFor(key, valueDef);
      }
    } else {
      throw loader.unsupportedArrayException(key, valueDef.dimensions());
    }
  }

  private ValueListener listenerFor(ColumnMetadata colSchema) {
    switch (colSchema.structureType()) {
      case PRIMITIVE:
        if (colSchema.isArray()) {
          return scalarArrayListenerFor(colSchema);
        } else {
          return scalarListenerFor(colSchema);
        }
      case TUPLE:
        if (colSchema.isArray()) {
          return objectArrayListenerFor(colSchema);
        } else {
          return objectListenerFor(colSchema);
        }
      case VARIANT:
        if (colSchema.isArray()) {
          return variantArrayListenerFor(colSchema);
        } else {
          return variantListenerFor(colSchema);
        }
      case MULTI_ARRAY:
        return repeatedListListenerFor(colSchema);
      default:
    }
    throw loader.unsupportedType(colSchema);
  }

  public ScalarListener scalarListenerFor(String key, JsonType jsonType) {
    MinorType colType = loader.drillTypeFor(jsonType);
    Preconditions.checkArgument(colType != null, "Not a scalar: " + jsonType.name());
    ColumnMetadata colSchema = MetadataUtils.newScalar(key, Types.optional(colType));
    return scalarListenerFor(colSchema);
  }

  public ScalarListener scalarListenerFor(ColumnMetadata colSchema) {
    int index = tupleWriter.addColumn(colSchema);
    return ScalarListener.listenerFor(loader, tupleWriter.column(index));
  }

  public ObjectValueListener objectListenerFor(ColumnMetadata providedCol) {
    return objectListenerFor(providedCol.name(), providedCol.tupleSchema());
  }

  public ObjectValueListener objectListenerFor(String key, TupleMetadata providedSchema) {
    ColumnMetadata colSchema = MetadataUtils.newMap(key);
    int index = tupleWriter.addColumn(colSchema);
    return new ObjectValueListener(loader, colSchema,
        new TupleListener(loader, tupleWriter.tuple(index), providedSchema));
  }

  public ArrayValueListener objectArrayListenerFor(ColumnMetadata providedCol) {
    return objectArrayListenerFor(providedCol.name(), providedCol.tupleSchema());
  }

  public ArrayValueListener objectArrayListenerFor(
      String key, TupleMetadata providedSchema) {
    ColumnMetadata colSchema = MetadataUtils.newMapArray(key);
    int index = tupleWriter.addColumn(colSchema);
    ArrayWriter arrayWriter = tupleWriter.array(index);
    return new ObjectArrayValueListener(loader, colSchema,
        new ObjectArrayListener(loader, arrayWriter,
            new ObjectValueListener(loader, colSchema,
                new TupleListener(loader, arrayWriter.tuple(), providedSchema))));
  }

  public ArrayValueListener arrayListenerFor(String key, JsonType jsonType) {
    MinorType colType = loader.drillTypeFor(jsonType);
    if (colType == null) {
      throw loader.unsupportedJsonTypeException(key, jsonType);
    }
    ColumnMetadata colSchema = MetadataUtils.newScalar(key, Types.repeated(colType));
    return scalarArrayListenerFor(colSchema);
  }

  public ArrayValueListener scalarArrayListenerFor(ColumnMetadata colSchema) {
    return new ScalarArrayValueListener(loader, colSchema,
        new ScalarArrayListener(loader, colSchema,
            scalarListenerFor(colSchema)));
  }

  private ValueListener unknownListenerFor(String key) {
    return new UnknownFieldListener(this, key);
  }

  private ValueListener unknownArrayListenerFor(String key, ValueDef valueDef) {
    UnknownFieldListener fieldListener = new UnknownFieldListener(this, key);
    fieldListener.array(valueDef);
    return fieldListener;
  }

  private ValueListener variantListenerFor(ColumnMetadata colSchema) {
    int index = tupleWriter.addColumn(colSchema);
    return new VariantListener(loader, tupleWriter.column(index).variant());
  }

  private ValueListener variantArrayListenerFor(ColumnMetadata colSchema) {
    int index = tupleWriter.addColumn(colSchema);
    return new ListListener(loader, tupleWriter.column(index));
  }

  private ValueListener repeatedListListenerFor(String key, ValueDef valueDef) {
    MinorType colType = loader.drillTypeFor(valueDef.type());
    if (colType == null) {
      throw loader.unsupportedJsonTypeException(key, valueDef.type());
    }
    ColumnMetadata colSchema = new RepeatedListBuilder(key)
        .addArray(colType)
        .buildColumn();
    return repeatedListListenerFor(colSchema);
  }

  private ValueListener repeatedListOfObjectsListenerFor(String key, ColumnMetadata providedCol) {
    ColumnMetadata colSchema = new RepeatedListBuilder(key)
        .addMapArray()
          .resumeList()
        .buildColumn();
    int index = tupleWriter.addColumn(colSchema);
    TupleMetadata providedSchema = providedCol == null ? null
        : providedCol.childSchema().tupleSchema();
    return RepeatedListValueListener.repeatedObjectListFor(loader,
        tupleWriter.column(index), providedSchema);
  }

  private ValueListener repeatedListListenerFor(ColumnMetadata colSchema) {
    ColumnMetadata childSchema = colSchema.childSchema();
    if (childSchema != null && childSchema.isMap()) {
      return repeatedListOfObjectsListenerFor(colSchema.name(), colSchema);
    } else {
      int index = tupleWriter.addColumn(colSchema);
      return RepeatedListValueListener.repeatedListFor(loader, tupleWriter.column(index));
    }
  }

  public ColumnMetadata providedColumn(String key) {
    return providedSchema == null ? null : providedSchema.metadata(key);
  }
}
