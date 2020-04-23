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
package org.apache.drill.exec.store.easy.json.loader.values;

import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Per the <a href="https://docs.mongodb.com/manual/reference/mongodb-extended-json-v1/#bson.data_date">
 * V1 docs</a>:
 * <quote>
 * In Strict mode, {@code <date>} is an ISO-8601 date format with a mandatory time zone field
 * following the template YYYY-MM-DDTHH:mm:ss.mmm<+/-Offset>.
 * <p>
 * In Shell mode, {@code <date>} is the JSON representation of a 64-bit signed
 * integer giving the number of milliseconds since epoch UTC.
 * </quote>
 * <p>
 * Drill dates are in the local time zone, so conversion is needed.
 */
public class TimestampValueListener extends ScalarListener {

  private final DateTimeZone tz = DateTimeZone.getDefault();
  private final DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  public TimestampValueListener(JsonLoaderImpl loader, ScalarWriter writer) {
    super(loader, writer);
  }

  @Override
  public void onValue(JsonToken token, TokenIterator tokenizer) {
    switch (token) {
      case VALUE_NULL:
        setNull();
        break;
      case VALUE_NUMBER_INT:
        writer.setLong(tz.convertUTCToLocal(tokenizer.longValue()));
        break;
      case VALUE_STRING:
        try {
          writer.setLong(tz.convertUTCToLocal(fmt.parseMillis(tokenizer.stringValue())));
        } catch (Exception e) {
          throw loader.dataConversionError(schema(), "date", tokenizer.stringValue());
        }
        break;
      default:
        throw tokenizer.invalidValue(token);
    }
  }
}
