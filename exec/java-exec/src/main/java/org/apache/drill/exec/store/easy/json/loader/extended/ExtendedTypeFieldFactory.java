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
package org.apache.drill.exec.store.easy.json.loader.extended;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.store.easy.json.loader.BaseFieldFactory;
import org.apache.drill.exec.store.easy.json.loader.FieldDefn;
import org.apache.drill.exec.store.easy.json.loader.FieldFactory;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.loader.values.BinaryValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.DateValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.DecimalValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.IntervalValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.StrictBigIntValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.StrictDoubleValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.StrictIntValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.StrictStringValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.TimeValueListener;
import org.apache.drill.exec.store.easy.json.loader.values.TimestampValueListener;
import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;
import org.apache.drill.exec.store.easy.json.parser.ValueParser;

import com.fasterxml.jackson.core.JsonToken;

public class ExtendedTypeFieldFactory extends BaseFieldFactory {

  public ExtendedTypeFieldFactory(JsonLoaderImpl loader, FieldFactory child) {
    super(loader, child);
  }

  @Override
  public ElementParser fieldParser(FieldDefn fieldDefn) {
    ElementParser parser = buildExtendedTypeParser(fieldDefn);
    if (parser == null) {
      return child.fieldParser(fieldDefn);
    } else {
      return parser;
    }
  }

  private ElementParser buildExtendedTypeParser(FieldDefn fieldDefn) {

    // Extended types are objects: { "$type": ... }
    // Extended arrays are [ { "$type": ...
    TokenIterator tokenizer = fieldDefn.tokenizer();
    JsonToken token = tokenizer.requireNext();
    ElementParser parser;
    switch (token) {
      case START_OBJECT:
        parser = extendedTypeParserFor(fieldDefn, false);
        break;
      case START_ARRAY:
        parser = arrayParserFor(fieldDefn);
        break;
      default:
        parser = null;
    }
    tokenizer.unget(token);
    return parser;
  }

  private ElementParser arrayParserFor(FieldDefn fieldDefn) {
    TokenIterator tokenizer = fieldDefn.tokenizer();
    JsonToken token = tokenizer.requireNext();
    if (token != JsonToken.START_OBJECT) {
      tokenizer.unget(token);
      return null;
    }

    ValueParser element = extendedTypeParserFor(fieldDefn, true);
    tokenizer.unget(token);
    if (element == null) {
      return null;
    }

    return scalarArrayParserFor(element);
  }

  private BaseExtendedValueParser extendedTypeParserFor(FieldDefn fieldDefn, boolean isArray) {
    TokenIterator tokenizer = fieldDefn.tokenizer();
    JsonToken token = tokenizer.peek();
    if (token != JsonToken.FIELD_NAME) {
      return null;
    }

    String key = tokenizer.textValue().trim();
    if (!key.startsWith(ExtendedTypeNames.TYPE_PREFIX)) {
      return null;
    }
    return parserFor(fieldDefn, key, isArray);
  }

  private BaseExtendedValueParser parserFor(FieldDefn fieldDefn, String key, boolean isArray) {
    switch (key) {
    case ExtendedTypeNames.LONG:
      return numberLongParser(fieldDefn, isArray);
    case ExtendedTypeNames.DECIMAL:
      return numberDecimalParser(fieldDefn, isArray);
    case ExtendedTypeNames.DOUBLE:
      return numberDoubleParser(fieldDefn, isArray);
    case ExtendedTypeNames.INT:
      return numberIntParser(fieldDefn, isArray);
    case ExtendedTypeNames.DATE:
      return dateParser(fieldDefn, isArray);
    case ExtendedTypeNames.BINARY:
    case ExtendedTypeNames.BINARY_TYPE:
      return binaryParser(fieldDefn, isArray);
    case ExtendedTypeNames.OBJECT_ID:
      return oidParser(fieldDefn, isArray);
    case ExtendedTypeNames.DATE_DAY:
      return dateDayParser(fieldDefn, isArray);
    case ExtendedTypeNames.TIME:
      return timeParser(fieldDefn, isArray);
    case ExtendedTypeNames.INTERVAL:
      return intervalParser(fieldDefn, isArray);
    default:
      return null;
    }
  }

  private BaseExtendedValueParser numberLongParser(FieldDefn fieldDefn, boolean isArray) {
    return new SimpleExtendedValueParser(
        fieldDefn.parser(), ExtendedTypeNames.LONG,
        new StrictBigIntValueListener(loader(),
            fieldDefn.scalarWriterFor(MinorType.BIGINT, isArray)));
  }

  private BaseExtendedValueParser numberDecimalParser(FieldDefn fieldDefn, boolean isArray) {
    // No information about precision and scale, so guess (38, 10).
    // TODO: maybe make a config option?
    return new SimpleExtendedValueParser(
        fieldDefn.parser(), ExtendedTypeNames.DECIMAL,
        new DecimalValueListener(loader(),
            fieldDefn.scalarWriterFor(
                MetadataUtils.newDecimal(fieldDefn.key(), fieldDefn.mode(isArray), 38, 10))));
  }

  private BaseExtendedValueParser numberDoubleParser(FieldDefn fieldDefn, boolean isArray) {
    return new SimpleExtendedValueParser(
        fieldDefn.parser(), ExtendedTypeNames.DOUBLE,
        new StrictDoubleValueListener(loader(),
            fieldDefn.scalarWriterFor(MinorType.FLOAT8, isArray)));
  }

  private BaseExtendedValueParser numberIntParser(FieldDefn fieldDefn, boolean isArray) {
    return new SimpleExtendedValueParser(
        fieldDefn.parser(), ExtendedTypeNames.INT,
        new StrictIntValueListener(loader(),
            fieldDefn.scalarWriterFor(MinorType.INT, isArray)));
  }

  private BaseExtendedValueParser dateParser(FieldDefn fieldDefn, boolean isArray) {
    return new MongoDateValueParser(fieldDefn.parser(),
        new TimestampValueListener(loader(),
            fieldDefn.scalarWriterFor(MinorType.TIMESTAMP, isArray)));
  }

  private BaseExtendedValueParser binaryParser(FieldDefn fieldDefn, boolean isArray) {
    return new MongoBinaryValueParser(fieldDefn.parser(),
        new BinaryValueListener(loader(),
            fieldDefn.scalarWriterFor(MinorType.VARBINARY, isArray)));
  }

  private BaseExtendedValueParser oidParser(FieldDefn fieldDefn, boolean isArray) {
    return new SimpleExtendedValueParser(
        fieldDefn.parser(), ExtendedTypeNames.OBJECT_ID,
        new StrictStringValueListener(loader(),
            fieldDefn.scalarWriterFor(MinorType.VARCHAR, isArray)));
  }

  private BaseExtendedValueParser dateDayParser(FieldDefn fieldDefn, boolean isArray) {
    return new SimpleExtendedValueParser(
        fieldDefn.parser(), ExtendedTypeNames.DATE_DAY,
        new DateValueListener(loader(),
            fieldDefn.scalarWriterFor(MinorType.DATE, isArray)));
  }

  private BaseExtendedValueParser timeParser(FieldDefn fieldDefn, boolean isArray) {
    return new SimpleExtendedValueParser(fieldDefn.parser(), ExtendedTypeNames.TIME,
        new TimeValueListener(loader(),
            fieldDefn.scalarWriterFor(MinorType.TIME, isArray)));
  }

  private BaseExtendedValueParser intervalParser(FieldDefn fieldDefn, boolean isArray) {
    return new SimpleExtendedValueParser(
        fieldDefn.parser(), ExtendedTypeNames.INTERVAL,
        new IntervalValueListener(loader(),
            fieldDefn.scalarWriterFor(MinorType.INTERVAL, isArray)));
  }
}
