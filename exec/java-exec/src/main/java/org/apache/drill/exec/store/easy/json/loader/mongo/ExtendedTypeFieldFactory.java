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
package org.apache.drill.exec.store.easy.json.loader.mongo;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.store.easy.json.loader.BaseFieldFactory;
import org.apache.drill.exec.store.easy.json.loader.FieldFactory;
import org.apache.drill.exec.store.easy.json.loader.TupleListener;
import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener.FieldDefn;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

import com.fasterxml.jackson.core.JsonToken;

public class ExtendedTypeFieldFactory extends BaseFieldFactory {

  public ExtendedTypeFieldFactory(TupleListener tupleListener, FieldFactory child) {
    super(tupleListener, child);
  }

  @Override
  public ElementParser addField(FieldDefn fieldDefn) {
    ElementParser parser = buildExtendedTypeParser(fieldDefn);
    if (parser == null) {
      return child.addField(fieldDefn);
    } else {
      return parser;
    }
  }

  private ElementParser buildExtendedTypeParser(FieldDefn fieldDefn) {

    // Extended types are objects: { "$type": ... }

    TokenIterator tokenizer = fieldDefn.tokenizer();
    JsonToken token1 = tokenizer.requireNext();
    if (token1 != JsonToken.START_OBJECT) {
      tokenizer.unget(token1);
      return null;
    }

    JsonToken token2 = tokenizer.requireNext();
    tokenizer.unget(token2);
    tokenizer.unget(token1);
    if (token2 != JsonToken.FIELD_NAME) {
      return null;
    }

    String key = tokenizer.textValue().trim();
    if (!key.startsWith(ExtendedTypeNames.TYPE_PREFIX)) {
      return null;
    }
    return parserFor(fieldDefn, key);
  }

  private ElementParser parserFor(FieldDefn fieldDefn, String key) {
    switch (key) {
    case ExtendedTypeNames.LONG:
      return numberLongParser(fieldDefn);
    case ExtendedTypeNames.DECIMAL:
      return numberDecimalParser(fieldDefn);
    case ExtendedTypeNames.DOUBLE:
      return numberDoubleParser(fieldDefn);
    case ExtendedTypeNames.INT:
      return numberIntParser(fieldDefn);
    case ExtendedTypeNames.DATE:
      return dateParser(fieldDefn);
    case ExtendedTypeNames.BINARY:
      return binaryParser(fieldDefn);
    default:
      return null;
    }
  }

  private ElementParser numberLongParser(FieldDefn fieldDefn) {
    return new MongoSimpleValueParser(ExtendedTypeNames.LONG,
        new Int64ValueListener(loader(),
            defineColumn(fieldDefn.key(), MinorType.BIGINT)),
        fieldDefn.errorFactory());
  }

  private ElementParser numberDecimalParser(FieldDefn fieldDefn) {
    return new MongoSimpleValueParser(ExtendedTypeNames.DECIMAL,
        new DecimalValueListener(loader(),
            defineColumn(fieldDefn.key(), MinorType.VARDECIMAL)),
        fieldDefn.errorFactory());
  }

  private ElementParser numberDoubleParser(FieldDefn fieldDefn) {
    return new MongoSimpleValueParser(ExtendedTypeNames.DOUBLE,
        new DoubleValueListener(loader(),
            defineColumn(fieldDefn.key(), MinorType.FLOAT8)),
        fieldDefn.errorFactory());
  }

  private ElementParser numberIntParser(FieldDefn fieldDefn) {
    return new MongoSimpleValueParser(ExtendedTypeNames.INT,
        new Int32ValueListener(loader(),
            defineColumn(fieldDefn.key(), MinorType.INT)),
        fieldDefn.errorFactory());
  }

  private ElementParser dateParser(FieldDefn fieldDefn) {
    return new MongoDateValueParser(
        new DateValueListener(loader(),
            defineColumn(fieldDefn.key(), MinorType.TIMESTAMP)),
        fieldDefn.errorFactory());
  }

  private ElementParser binaryParser(FieldDefn fieldDefn) {
    return new MongoBinaryValueParser(
        new BinaryValueListener(loader(),
            defineColumn(fieldDefn.key(), MinorType.VARBINARY)),
        fieldDefn.errorFactory());
  }

  protected ScalarWriter defineColumn(String key, MinorType type) {
    return tupleListener.fieldwriterFor(
        MetadataUtils.newScalar(key, type, DataMode.OPTIONAL))
        .scalar();
  }

//  @Override
//  public FieldType fieldType(String key) {
//    return child.fieldType(key);
//  }
//
//  @Override
//  public ValueListener addField(String key, ValueDef valueDef) {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  public ObjectValueListener becomeMap(String key) {
//    return objectListenerFor(key);
//  }
//
//  public boolean isExtendedTypeName(String key) {
//    // TODO: If this is a performance issue, replace with a
//    // static map of contructors.
//    switch (key) {
//      case ExtendedTypeNames.LONG:
//        return true;
//      default:
//        return false;
//    }
//  }
//
//  public ValueListener extendedTypeListenerFor(String key) {
//    return new ExtendedTypeFieldListener(
//        extendedObjectListenerFor(key));
//  }
//
//  private ExtendedTypeObjectListener extendedObjectListenerFor(String key) {
//    // TODO: If this is a performance issue, replace with a
//    // static map of contructors.
//    switch (key) {
//      case ExtendedTypeNames.LONG:
//        return new Int64ObjectListener(tupleListener, key);
//      default:
//        throw new IllegalStateException("Unexpected extended type: " + key);
//    }
//  }
}
