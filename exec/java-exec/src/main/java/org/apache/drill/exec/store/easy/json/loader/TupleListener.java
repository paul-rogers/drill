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
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ObjectValueListener;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ScalarArrayValueListener;
import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.TupleWriter;

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
 */
public class TupleListener implements ObjectListener {

  protected final JsonLoaderImpl loader;
  private final TupleWriter tupleWriter;
  private final TupleMetadata providedSchema;

  public TupleListener(JsonLoaderImpl loader, TupleWriter tupleWriter, TupleMetadata providedSchema) {
    this.loader = loader;
    this.tupleWriter = tupleWriter;
    this.providedSchema = providedSchema;
  }

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
  public ValueListener addScalar(String key, JsonType type) {
    ColumnMetadata colSchema = providedColumn(key);
    if (colSchema != null) {
      return listenerFor(colSchema);
    }
    MinorType colType = drillTypeFor(type);
    if (colType != null) {
      colSchema = MetadataUtils.newScalar(key, Types.optional(colType));
      return ScalarListener.listenerFor(loader, tupleWriter, colSchema);
    }
    switch (type) {
    case ARRAY:
      break;
    case EMPTY:
      break;
    case NULL:
      break;
    case OBJECT:
      break;
    default:
      break;
    }
    // TODO
    throw new IllegalStateException();
  }

  private MinorType drillTypeFor(JsonType type) {
    if (loader.options().allTextMode) {
      return MinorType.VARCHAR;
    }
    switch (type) {
    case BOOLEAN:
      return MinorType.BIT;
    case FLOAT:
      return MinorType.FLOAT8;
    case INTEGER:
      if (loader.options().readNumbersAsDouble) {
        return MinorType.FLOAT8;
      } else {
        return MinorType.BIGINT;
      }
    case STRING:
      return MinorType.VARCHAR;
    default:
      return null;
    }
  }

  private ValueListener listenerFor(ColumnMetadata colSchema) {
    switch (colSchema.structureType()) {
    case DICT:
      break;
    case MULTI_ARRAY:
      break;
    case PRIMITIVE:
      return primitiveListenerFor(colSchema);
    case TUPLE:
      return ObjectValueListener.listenerFor(loader, tupleWriter,
          colSchema.name(), colSchema.tupleSchema());
    case VARIANT:
      break;
    default:
      break;
    }
    // TODO
    throw new IllegalStateException();
  }

  private ValueListener primitiveListenerFor(ColumnMetadata colSchema) {
    if (colSchema.isArray()) {
      return ScalarArrayValueListener.listenerFor(loader, tupleWriter, colSchema);
    } else {
      return ScalarListener.listenerFor(loader, tupleWriter, colSchema);
    }
  }

  @Override
  public ValueListener addArray(String key, int arrayDims, JsonType type) {
    ColumnMetadata colSchema = providedColumn(key);
    if (colSchema != null) {
      return listenerFor(colSchema);
    } else if (arrayDims == 1) {
      return addRepeated(key, type);
    } else {
      return addList(key, type);
    }
  }

  private ValueListener addRepeated(String key, JsonType type) {
    MinorType colType = drillTypeFor(type);
    if (colType != null) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(key, Types.repeated(colType));
      return ScalarArrayValueListener.listenerFor(loader, tupleWriter, colSchema);
    }
    switch (type) {
    case ARRAY:
      break;
    case EMPTY:
      break;
    case NULL:
      break;
    case OBJECT:
      break;
    default:
      break;
    }
    // TODO
    throw new IllegalStateException();
  }

  private ValueListener addList(String key, JsonType type) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ValueListener addObject(String key) {
    return ObjectValueListener.listenerFor(loader, tupleWriter, key, null);
  }

  @Override
  public ValueListener addObjectArray(String key, int dims) {
    // TODO Auto-generated method stub
    return null;
  }

  public ColumnMetadata providedColumn(String key) {
    return providedSchema == null ? null : providedSchema.metadata(key);
  }

  public static class RowListener extends TupleListener {

    public RowListener(JsonLoaderImpl loader, TupleWriter tupleWriter,
        TupleMetadata providedSchema) {
      super(loader, tupleWriter, providedSchema);
    }
  }

  public static class MapListener extends TupleListener {

    public MapListener(JsonLoaderImpl loader, TupleWriter tupleWriter,
        TupleMetadata providedSchema) {
      super(loader, tupleWriter, providedSchema);
    }
  }
}
