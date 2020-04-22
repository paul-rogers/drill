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

import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.TokenIterator;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.joda.time.Period;
import org.joda.time.format.ISOPeriodFormat;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Drill-specific extension for a time interval (AKA time
 * span or time period).
 */
public class IntervalValueListener extends ExtendedValueListener {

  public IntervalValueListener(JsonLoaderImpl loader, ScalarWriter writer) {
    super(loader, writer);
  }

  @Override
  public void onValue(JsonToken token, TokenIterator tokenizer) {
    switch (token) {
      case VALUE_NULL:
        writer.setNull();
        break;
      case VALUE_STRING:
        try {
          final Period p = ISOPeriodFormat.standard().parsePeriod(tokenizer.stringValue());
          writer.setPeriod(p);
        } catch (Exception e) {
          throw loader.dataConversionError(schema(), "date", tokenizer.stringValue());
        }
        break;
      default:
        throw tokenizer.invalidValue(token);
    }
  }
}
