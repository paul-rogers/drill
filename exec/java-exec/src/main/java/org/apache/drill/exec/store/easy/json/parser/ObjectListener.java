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
package org.apache.drill.exec.store.easy.json.parser;

import org.apache.drill.exec.store.easy.json.parser.ElementParser.ValueParser;

/**
 * Represents events on a object value. The object value may be a top-level
 * field or may be the element of an array. The listener gets an event when
 * an object is started and ended, as well as when a new field is discovered.
 * First, the parser asks if the field should be projected. If not, the
 * parser will create a dummy parser to "free-wheel" over whatever values the
 * field contains. (This is one way to avoid structure errors in a JSON file:
 * just ignore them.) Otherwise, the parser will look ahead to guess the
 * field type and will call one of the "add" methods, each of which should
 * return a value listener for the field itself.
 * <p>
 * The structure parser looks ahead some number of tokens to infer the value
 * of the field. While this is helpful, it really only works if the JSON
 * is structured like a list of tuples, if the initial value is not {@code null},
 * and if initial arrays are not empty. The structure parser cannot see
 * into the future beyond the first field value; the value listener for each
 * field must handle "type-deferral" if needed to handle missing or null
 * values. That is, type-consistency is a semantic task handled by the listener,
 * not a syntax task handled by the parser.
 *
 * <h4>Fields</h4>
 *
 * The structure of an object is:
 * <ul>
 * <li>{@code ObjectListener} which represents the object (tuple) as a whole.
 * Each field, indexed by name, is represented as a</li>
 * <li>{@code ValueListener} which represents the value "slot". That value
 * can be scalar, or can be structured, in which case the value listener
 * contains either a</li>
 * <li>{@code ArrayListener} for an array, or a</li>
 * <li>{@code ObjectListener} for a nested object (tuple).</li>
 * </ul>
 */
public interface ObjectListener {

  /**
   * Describes a new field within an object. Allows the listener to control
   * how to handle the field: as unprojected, parsed as a typed field, as
   * text, as JSON, or as a custom parser.
   */
  public interface FieldDefn {

    /**
     * Returns the field name.
     */
    String key();

    /**
     * Looks ahead to guess the field type based on JSON tokens.
     */
    ValueDef lookahead();

    /**
     * Token stream which allows a custom parser to look ahead
     * as needed. The caller must "unget" all tokens to leave the
     * tokenizer at the present location. Note that the underlying
     * Jackson parser will return text for the last token consumed,
     * even if tokens are unwound using the token iterator, so do not
     * look ahead past the first field name or value; on look ahead
     * over "static" tokens such as object and array start characters.
     */
    TokenIterator tokenizer();

    /**
     * Returns the parent parser which is needed to construct standard
     * parsers.
     */
    JsonStructureParser parser();
  }

  /**
   * Called at the start of a set of values for an object. That is, called
   * when the structure parser accepts the <code>{</code> token.
   */
  void onStart();

  /**
   * The structure parser has just encountered a new field for this
   * object. This method returns a parser for the field, along with
   * an optional listener to handle events within the field. THe field typically
   * uses a value parser create by the {@link FieldParserFactory} class.
   * However, special cases (such as Mongo extended types) can create a
   * custom parser.
   * <p>
   * If the field is not projected, the method should return a dummy parser
   * from {@link FieldParserFactory#ignoredFieldParser()}.
   * <p>
   * A normal field will respond to the structure of the JSON file as it
   * appears. The associated value listener receives events for the
   * field value. The value listener may be asked to create additional
   * structure, such as arrays or nested objects.
   * <p>
   * Parse position: <code>{ ... field : ^ ?</code> for a newly-seen field.
   * Constructs a value parser and its listeners by looking ahead
   * some number of tokens to "sniff" the type of the value. For
   * example:
   * <ul>
   * <li>{@code foo: <value>} - Field value</li>
   * <li>{@code foo: [ <value> ]} - 1D array value</li>
   * <li>{@code foo: [ [<value> ] ]} - 2D array value</li>
   * <li>Etc.</li>
   * </ul>
   * <p>
   * There are two cases in which no type estimation is possible:
   *
   * @param field description of the field, including the field name
   * @return a parser for the newly-created field
   */
  ValueParser onField(FieldDefn field);

  /**
   * Called at the end of a set of values for an object. That is, called
   * when the structure parser accepts the <code>}</code> token.
   */
  void onEnd();
}
