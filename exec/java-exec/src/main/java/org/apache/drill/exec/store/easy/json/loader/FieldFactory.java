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

import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.ObjectParser.FieldDefn;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;

/**
 * Extensible mechanism to build fields for a JSON object (a Drill
 * row or Map).
 */
public interface FieldFactory {

  /**
   * Add a field. Called only for projected fields. May add a "deferred"
   * undefined field if the value type is undefined. Such fields are added
   * to the underlying row or map at a later time.
   *
   * @see ObjectListener#addField(String, ValueDef)
   */
  ElementParser addField(FieldDefn fieldDefn);

  /**
   * Resolve a field to concrete vector. Called from the above, or when
   * a deferred field resolves to an actual type.
   *
   * @param key field name
   * @param valueDef type and array description. The type must be concrete.
   * @return a parser for the field which, for a deferred field, can
   * replace the original, undefined, parser
   */
  ElementParser resolveField(FieldDefn fieldDefn);

  ElementParser ignoredFieldParser();

  ElementParser forceNullResolution(String key);
  ElementParser forceArrayResolution(String key);
}
