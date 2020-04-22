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
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ArrayParser;
import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener.FieldDefn;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.store.easy.json.parser.ValueParser;
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
    // Extended arrays are [ { "$type": ...
    TokenIterator tokenizer = fieldDefn.tokenizer();
    JsonToken token = tokenizer.requireNext();
    ElementParser parser;
    switch (token) {
      case START_OBJECT:
        parser = scalarParserFor(fieldDefn);
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

  private ElementParser scalarParserFor(FieldDefn fieldDefn) {
    TokenIterator tokenizer = fieldDefn.tokenizer();

    JsonToken token = tokenizer.peek();
    if (token != JsonToken.FIELD_NAME) {
      return null;
    }

    String key = tokenizer.textValue().trim();
    if (!key.startsWith(ExtendedTypeNames.TYPE_PREFIX)) {
      return null;
    }
    return parserFor(fieldDefn, key);
  }

  private ElementParser arrayParserFor(FieldDefn fieldDefn) {
    TokenIterator tokenizer = fieldDefn.tokenizer();

    JsonToken token = tokenizer.requireNext();
    if (token != JsonToken.START_OBJECT) {
      tokenizer.unget(token);
      return null;
    }
    JsonToken token = tokenizer.peek();
    if (token != JsonToken.FIELD_NAME) {
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
    case ExtendedTypeNames.OBJECT_ID:
      return oidParser(fieldDefn);
    case ExtendedTypeNames.DATE_DAY:
      return dateDayParser(fieldDefn);
    case ExtendedTypeNames.TIME:
      return timeParser(fieldDefn);
    case ExtendedTypeNames.INTERVAL:
      return intervalParser(fieldDefn);
    default:
      return null;
    }
  }

  private ElementParser numberLongParser(FieldDefn fieldDefn) {
    return new SimpleExtendedValueParser(ExtendedTypeNames.LONG,
        new Int64ValueListener(loader(),
            defineColumn(fieldDefn, MinorType.BIGINT)),
        fieldDefn.errorFactory());
    ArrayListener al = new ScalarArrayListener(loader, colschema, sl);
    return new ArrayParser(fieldDefn.parent(), al);
    ValueListener vl = new ScalarArrayValueListener(loader, colSchema, al);
    ValueParser vp = new ValueParser(fieldDefn.parser(), vl);
  }

  private ElementParser numberLongArrayParser(FieldDefn fieldDefn) {
    return new SimpleExtendedValueParser(ExtendedTypeNames.LONG,
        new Int64ValueListener(loader(),
            defineColumn(fieldDefn, MinorType.BIGINT)),
        fieldDefn.errorFactory());
  }

  private ElementParser numberDecimalParser(FieldDefn fieldDefn) {
    return new SimpleExtendedValueParser(ExtendedTypeNames.DECIMAL,
        new DecimalValueListener(loader(),
            defineColumn(fieldDefn, MinorType.VARDECIMAL)),
        fieldDefn.errorFactory());
  }

  private ElementParser numberDoubleParser(FieldDefn fieldDefn) {
    return new SimpleExtendedValueParser(ExtendedTypeNames.DOUBLE,
        new DoubleValueListener(loader(),
            defineColumn(fieldDefn, MinorType.FLOAT8)),
        fieldDefn.errorFactory());
  }

  private ElementParser numberIntParser(FieldDefn fieldDefn) {
    return new SimpleExtendedValueParser(ExtendedTypeNames.INT,
        new Int32ValueListener(loader(),
            defineColumn(fieldDefn, MinorType.INT)),
        fieldDefn.errorFactory());
  }

  private ElementParser dateParser(FieldDefn fieldDefn) {
    return new MongoDateValueParser(
        new TimestampValueListener(loader(),
            defineColumn(fieldDefn, MinorType.TIMESTAMP)),
        fieldDefn.errorFactory());
  }

  private ElementParser binaryParser(FieldDefn fieldDefn) {
    return new MongoBinaryValueParser(
        new BinaryValueListener(loader(),
            defineColumn(fieldDefn, MinorType.VARBINARY)),
        fieldDefn.errorFactory());
  }

  private ElementParser oidParser(FieldDefn fieldDefn) {
    return new SimpleExtendedValueParser(ExtendedTypeNames.OBJECT_ID,
        new StringValueListener(loader(),
            defineColumn(fieldDefn, MinorType.VARCHAR)),
        fieldDefn.errorFactory());
  }

  private ElementParser dateDayParser(FieldDefn fieldDefn) {
    return new SimpleExtendedValueParser(ExtendedTypeNames.DATE_DAY,
        new DateValueListener(loader(),
            defineColumn(fieldDefn, MinorType.DATE)),
        fieldDefn.errorFactory());
  }

  private ElementParser timeParser(FieldDefn fieldDefn) {
    return new SimpleExtendedValueParser(ExtendedTypeNames.TIME,
        new TimeValueListener(loader(),
            defineColumn(fieldDefn, MinorType.TIME)),
        fieldDefn.errorFactory());
  }

  private ElementParser intervalParser(FieldDefn fieldDefn) {
    return new SimpleExtendedValueParser(ExtendedTypeNames.INTERVAL,
        new IntervalValueListener(loader(),
            defineColumn(fieldDefn, MinorType.INTERVAL)),
        fieldDefn.errorFactory());
  }

  protected ScalarWriter defineColumn(FieldDefn fieldDefn, MinorType type) {
    return tupleListener.fieldwriterFor(
        MetadataUtils.newScalar(fieldDefn.key(), type, DataMode.OPTIONAL))
        .scalar();
  }
}
