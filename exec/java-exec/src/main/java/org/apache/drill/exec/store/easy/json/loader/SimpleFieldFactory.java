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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.store.easy.json.loader.StructuredValueListener.ArrayValueListener;
import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener.FieldDefn;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class SimpleFieldFactory extends BaseFieldFactory {

  public SimpleFieldFactory(TupleListener tupleListener) {
    super(tupleListener);
  }

  /**
   * Build a column and its listener based on a look-ahead hint.
   */
  @Override
  public ElementParser addField(FieldDefn fieldDefn) {
    return fieldDefn.fieldFactory().valueParser(fieldDefn, listenerFor(fieldDefn));
  }

  private ValueListener listenerFor(FieldDefn fieldDefn) {
    String key = fieldDefn.key();
    ValueDef valueDef = fieldDefn.lookahead();
    if (valueDef.type().isUnknown()) {
      return listenerForUnknown(key, valueDef);
    } else {
      return resolveField(key, valueDef);
    }
  }

  private ValueListener listenerForUnknown(String key, ValueDef valueDef) {
    Preconditions.checkArgument(valueDef.type().isUnknown());
    if (valueDef.isArray()) {
      return unknownArrayListenerFor(key, valueDef);
    } else {
      return unknownListenerFor(key);
    }
  }

  @Override
  public ValueListener resolveField(String key, ValueDef valueDef) {
    Preconditions.checkArgument(!valueDef.type().isUnknown());
    if (!valueDef.isArray()) {
      if (valueDef.type().isObject()) {
        return objectListenerFor(key);
      } else {
        return scalarListenerForValue(key, valueDef.type());
      }
    } else if (valueDef.dimensions() == 1) {
      if (valueDef.type().isObject()) {
        return objectArrayListenerForValue(key);
      } else {
        return scalarArrayListenerForValue(key, valueDef.type());
      }
    } else { // 2+ dimensions
      if (valueDef.type().isObject()) {
        return multiDimObjectArrayListenerForValue(key, valueDef);
      } else {
        return multiDimScalarArrayListenerForValue(key, valueDef);
      }
    }
  }

  /**
   * Create a listener when we don't have type information. For the case
   * {@code null} appears before other values.
   */
  private ValueListener unknownListenerFor(String key) {
    return new UnknownFieldListener(tupleListener, key);
  }

  /**
   * Create a listener when we don't have type information. For the case
   * {@code []} appears before other values.
   */
  private ValueListener unknownArrayListenerFor(String key, ValueDef valueDef) {
    UnknownFieldListener fieldListener = new UnknownFieldListener(tupleListener, key);
    fieldListener.array(valueDef);
    return fieldListener;
  }

  /**
   * Create a scalar column and listener given the definition of a JSON
   * scalar value.
   */
  public ScalarListener scalarListenerForValue(String key, JsonType jsonType) {
    return scalarListenerFor(MetadataUtils.newScalar(key,
        Types.optional(scalarTypeFor(key, jsonType))));
  }

  /**
   * Create a scalar array column and listener given the definition of a JSON
   * array of scalars.
   */
  public ArrayValueListener scalarArrayListenerForValue(String key, JsonType jsonType) {
    return scalarArrayListenerFor(MetadataUtils.newScalar(key,
        Types.repeated(scalarTypeFor(key, jsonType))));
  }

  /**
   * Create a multi- (2+) dimensional scalar array from a JSON value description.
   */
  private ValueListener multiDimScalarArrayListenerForValue(String key, ValueDef valueDef) {
    return multiDimScalarArrayListenerFor(
        repeatedListSchemaFor(key, valueDef.dimensions(),
            MetadataUtils.newScalar(key, scalarTypeFor(key, valueDef.type()), DataMode.REPEATED)),
        valueDef.dimensions());
  }

  /**
   * Create a map array column and its associated object array listener
   * for the given key.
   */
  public ArrayValueListener objectArrayListenerForValue(String key) {
    ColumnMetadata colSchema = MetadataUtils.newMapArray(key);
    return objectArrayListenerFor(colSchema, colSchema.tupleSchema());
  }

  /**
   * Create a RepeatedList which contains (empty) Map objects using the provided
   * schema. That is, create a multi-dimensional array of maps.
   * The map fields are created on the fly, optionally using the provided schema.
   */
  private ValueListener multiDimObjectArrayListenerForValue(String key, ValueDef valueDef) {
    return multiDimObjectArrayListenerFor(
        repeatedListSchemaFor(key, valueDef.dimensions(),
            MetadataUtils.newMapArray(key)),
        valueDef.dimensions(), null);
  }

  /**
   * Create a RepeatedList which contains Unions. (Actually, this is an
   * array of List objects internally.) The variant is variable, it makes no
   * sense to specify a schema for the variant. Also, omitting the schema
   * save a large amount of complexity that will likely never be needed.
   */
  @SuppressWarnings("unused")
  private ValueListener repeatedListOfVariantListenerFor(String key, ValueDef valueDef) {
    return multiDimVariantArrayListenerFor(
        MetadataUtils.newVariant(key, DataMode.REPEATED),
        valueDef.dimensions());
  }

  /**
   * Convert the JSON type, obtained by looking ahead one token, to a Drill
   * scalar type. Report an error if the JSON type does not map to a Drill
   * type (which can occur in a context where we expect a scalar, but got
   * an object or array.)
   */
  private MinorType scalarTypeFor(String key, JsonType jsonType) {
    MinorType colType = drillTypeFor(jsonType);
    if (colType == null) {
      throw loader().unsupportedJsonTypeException(key, jsonType);
    }
    return colType;
  }

  public MinorType drillTypeFor(JsonType type) {
    if (loader().options().allTextMode) {
      return MinorType.VARCHAR;
    }
    switch (type) {
    case BOOLEAN:
      return MinorType.BIT;
    case FLOAT:
      return MinorType.FLOAT8;
    case INTEGER:
      if (loader().options().readNumbersAsDouble) {
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
}