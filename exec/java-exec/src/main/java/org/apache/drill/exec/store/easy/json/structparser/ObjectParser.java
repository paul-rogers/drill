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
package org.apache.drill.exec.store.easy.json.structparser;

import java.util.Map;

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.store.easy.json.parser.NullTypeMarker;
import org.apache.drill.exec.store.easy.json.structparser.ContainerListener.ObjectListener;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses a JSON object: <code>{ name : value ... }</code>
 * <p>
 * Creates a map of known fields. Each time a field is parsed,
 * looks up the field in the map. If not found, the value is "sniffed"
 * to determine its type, and a matching state and vector writer created.
 * Thereafter, the previous state is reused. The states ensure that the
 * correct token appears for each subsequent value, causing type errors
 * to be reported as syntax errors rather than as cryptic internal errors.
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
 * when we only see {@code null}. The one for {@code null} must morph into
 * the one for empty lists if we see:<br>
 * <code>{a: null} {a: [ ]  }</code><br>
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
class ObjectParser extends ContainerParser {

  /**
   * Parses: <code>[] | null [ ... ]</code>
   * <p>
   * This state remains in effect as long as the input contains empty arrays or
   * null values. However, once the array contains a non-empty array, this parser
   * detects the type of the array based on this value and replaces this state
   * with the new, resolved array parser based on the token seen.
   * <p>
   * The above implies that we look only at the first token to determine array
   * type. If that first token is {@code null}, then we can't handle the token
   * because arrays don't allow nulls (unless the array is an outer dimension of
   * a multi-dimensional array, but we don't know that yet.)
   * <p>
   * If at the end of a batch, no non-empty array was seen, assumes that the
   * array, when seen, will be an array of scalars, and replaces this state with
   * a text array (as in all-text mode.)
   */
  protected static class NullArrayParser extends AbstractParser implements NullTypeMarker {

    private final ObjectParser objectParser;

    public NullArrayParser(ObjectParser parentState, String key) {
      super(parentState, key);
      this.objectParser = parentState;
      structParser.addNullMarker(this);
    }

    /**
     * Parse <code>null | [] | [ some_token</code>
     */
    @Override
    public boolean parse(TokenIterator tokenizer) {
      JsonToken startToken = tokenizer.requireNext();
      // Position: ^ ?
      switch (startToken) {
        case VALUE_NULL:
          // Position: null ^
          // The value of this array is null, which is the same
          // as an empty array.
          return true;
        case START_ARRAY:
          // Position: [ ^
          break;
        default:
          // Position: ~[ ^
          // This is supposed to be an array.
          throw errorFactory().syntaxError(startToken);
      }

      JsonToken valueToken = tokenizer.requireNext();
      // Position: [ ? ^
      switch (valueToken) {
        case END_ARRAY:
          // Position: [ ] ^
          // An empty array, which we an absorb without knowing
          // the array type.
          return true;
        case VALUE_NULL:
          // Position: [ null ^
          // Nulls not allowed in arrays.
          throw errorFactory().structureError("Drill does not support leading nulls in JSON arrays.");
        default:
          tokenizer.unget(valueToken);

          // Position: [ ^ ?
          // Let's try to resolve the next token to tell us the array type.
          // The above has weeded out the "we don't know" cases.
          JsonElementParser newParser = objectParser.detectArrayParser(tokenizer, key());
          resolve(newParser);

          tokenizer.unget(startToken);
          // Position: ^ [
          // Reparse the array using the new parser
          return newParser.parse(tokenizer);
      }
    }

    @Override
    public void forceResolution() {
      resolve(objectParser.listener.forceNullArrayResolution(this));
    }

    private void resolve(JsonElementParser newParser) {
      // Replace this parser with the new one.
      objectParser.replaceChild(key(), newParser);
      structParser.removeNullMarker(this);
    }
  }

  private final ObjectListener listener;
  private final Map<String, JsonElementParser> members = CaseInsensitiveMap.newHashMap();

  public ObjectParser(JsonStructureParser structParser, ObjectListener listener) {
    super(structParser, JsonStructureParser.ROOT_NAME);
    this.listener = listener;
  }

  public ObjectParser(JsonElementParser parent, String fieldName,
      ObjectListener listener) {
    super(parent, fieldName);
    this.listener = listener;
  }

  @Override
  public boolean parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.next();
    if (token == null) {
      // Position: EOF ^
      return false;
    }
    // Position: ^ ?
    switch (token) {
      case NOT_AVAILABLE:
        return false; // Should never occur

      case VALUE_NULL:
        // Position: null ^
        // Same as omitting the object
        listener.onNull();
        return true;

      case START_OBJECT:
        // Position: { ^
        listener.onStart();
        break;

      default:
        // Position ~{ ^
        // Not a valid object.
        throw errorFactory().syntaxError(token); // Nothing else is valid
    }

    // Parse (field: value)* }

    for (;;) {
      token = tokenizer.requireNext();
      // Position: { (key: value)* ? ^
      switch (token) {
        case END_OBJECT:
          // Position: { (key: value)* } ^
          listener.onEnd();
          return true;

        case FIELD_NAME:
          // Position: { (key: value)* key: ^
          parseMember(tokenizer);
          break;

        default:
          // Position: { (key: value)* ~(key | }) ^
          // Invalid JSON.
          // Actually, we probably won't get here, the JSON parser
          // itself will throw an exception.
          throw errorFactory().syntaxError(token);
      }
    }
  }

  /**
   * Parse a field. Two cases. First, this is a field we've already seen. If so,
   * look up the parser for that field and use it. If this is the first time
   * we've seen the field, "sniff" tokens to determine field type, create a
   * parser, then parse.
   */
  private void parseMember(TokenIterator tokenizer) {
    // Position: key: ^ ?
    final String key = tokenizer.textValue().trim();
    JsonElementParser fieldState = members.get(key);
    if (fieldState == null) {
      // New key; sniff the value to determine the parser to use
      // (which also tell us the kind of column to create in Drill.)
      // Position: key: ^
      fieldState = detectValueParser(tokenizer, key);
      members.put(key, fieldState);
    }
    // Parse the field value using the parser for that field.
    // The structure implies that fields don't change types: the type of
    // the first value (sniffed above) must be repeated for every subsequent
    // object.
    // Position: key: ^ value ...
    fieldState.parse(tokenizer);
  }

  /**
   * If the column is not projected, create a dummy parser to "free wheel" over
   * the value. Otherwise, look ahead a token or two to determine the the type
   * of the field. Then the caller will backtrack to parse the field.
   *
   * @param key name of the field
   * @return parser for the field
   */
  JsonElementParser detectValueParser(TokenIterator tokenizer, final String key) {
    if (key.isEmpty()) {
      throw errorFactory().structureError(
          "Drill does not allow empty keys in JSON key/value pairs");
    }
    if (!listener.isProjected(key)) {
      return new DummyValueParser(this, key);
    }

    // For other types of projection, the projection
    // mechanism will catch conflicts.

    JsonToken token = tokenizer.requireNext();
    // Position: key: ? ^
    JsonElementParser valueParser;
    switch (token) {
      case START_ARRAY:
        // Position: key: [ ^
        valueParser = detectArrayParser(tokenizer, key);
        break;

      case START_OBJECT:
        // Position: key: { ^
        valueParser = objectParser(key);
        break;

      case VALUE_NULL:

        // Position: key: null ^
        valueParser = inferParser(key);
        break;

      default:
        // Position: key: ? ^
        valueParser = typedScalar(token, key);
    }
    tokenizer.unget(token);
    // Position: key: ^ ?
    return valueParser;
  }

  @Override
  protected JsonElementParser nullArrayParser(String key) {
    return new NullArrayParser(this, key);
  }

  @Override
  protected void replaceChild(String key, JsonElementParser newParser) {
    assert members.containsKey(key);
    members.put(key, newParser);
  }

  /**
   * The JSON itself provides a null value, so we can't determine the column
   * type. Use some additional inference methods. (These are experimental. What
   * is really needed is an up-front schema.)
   */
  private JsonElementParser inferParser(String key) {
    JsonElementParser valueParser = listener.inferNullMember(key);
    if (valueParser == null) {
      valueParser = new NullTypeParser(this, key);
    }
    return valueParser;
  }
}
