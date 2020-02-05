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

import java.util.Map;

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
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
      loader.addNullMarker(this);
    }

    /**
     * Parse <code>null | [] | [ some_token</code>
     */
    @Override
    public boolean parse() {
      JsonToken startToken = loader.tokenizer.requireNext();
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
          throw loader.syntaxError(startToken);
      }

      JsonToken valueToken = loader.tokenizer.requireNext();
      // Position: [ ? ^
      switch (valueToken) {
        case END_ARRAY:
          // Position: [ ] ^
          // An empty array, which we an absorb without knowing
          // the array type.
          return true;
        case VALUE_NULL:
          // Position: [ null ^
          // We don't have a type, so we don't have a backing vector to
          // set to null. Besides, null is only allowed in the upper
          // dimensions of multi-dimensional arrays. (This statement would
          // not be true if we were using lists, or if we created the array's
          // offset vector separate from the associated data vector.
          // Ready for a schema yet?
          throw loader.syntaxError("Drill does not support leading nulls in JSON arrays.");
        default:
          loader.tokenizer.unget(valueToken);
          // Position: [ ^ ?
          // Let's try to resolve the next token to tell us the array type.
          // The above has weeded out the "we don't know" cases.
          JsonElementParser newParser = resolve(objectParser.detectArrayParser(key()));
          loader.tokenizer.unget(startToken);
          // Position: [ ^ ?
          // Replace this parser with the new one, then use that new
          // parser to reparse this array.
          objectParser.replaceChild(key(), newParser);
          loader.removeNullMarker(this);
          return newParser.parse();
      }
    }

    @Override
    public void forceResolution() {
      ArrayParser newParser = objectParser.inferScalarArrayFromHint(key());
      if (newParser != null) {
        JsonLoaderImpl.logger.info("Using hints to determine type of JSON array {}. " +
            " Found type {}",
            fullName(), newParser.writer.scalar().schema().type().name());
      } else {
        JsonLoaderImpl.logger.warn("Ambiguous type! JSON array {}" +
            " contains all nulls. Assuming VARCHAR.",
            fullName());
        ArrayWriter arrayWriter = objectParser.newWriter(key(), MinorType.VARCHAR, DataMode.REPEATED).array();
        JsonElementParser elementParser = new ScalarParser.TextParser(objectParser, key(), arrayWriter.scalar());
        newParser = new ArrayParser.ScalarArrayParser(objectParser, key(), arrayWriter, elementParser);
      }
      resolve(newParser);
    }

    private JsonElementParser resolve(JsonElementParser newParser) {
      objectParser.replaceChild(key(), newParser);
      loader.removeNullMarker(this);
      return newParser;
    }

    @Override
    public ColumnMetadata schema() { return null; }
  }

  private final TupleWriter writer;
  private final Map<String, JsonElementParser> members = CaseInsensitiveMap.newHashMap();

  public ObjectParser(JsonLoaderImpl loader, TupleWriter writer, TupleProjection projection) {
    super(loader, JsonLoaderImpl.ROOT_NAME, projection);
    this.writer = writer;
  }

  public ObjectParser(JsonElementParser parent, String fieldName,
      TupleWriter writer, TupleProjection projection) {
    super(parent, fieldName, projection);
    this.writer = writer;
  }

  @Override
  public boolean parse() {
    JsonToken token = loader.tokenizer.next();
    if (token == null) {
      // Position: EOF ^
      return false;
    }
    // Position: ? ^
    switch (token) {
      case NOT_AVAILABLE:
        return false; // Should never occur

      case VALUE_NULL:
        // Position: null ^
        // Same as omitting the object
        return true;

      case START_OBJECT:
        // Position: { ^
        break;

      default:
        // Position ~{ ^
        // Not a valid object.
        throw loader.syntaxError(token); // Nothing else is valid
    }

    // Parse (field: value)* }

    for (;;) {
      token = loader.tokenizer.requireNext();
      // Position: { (key: value)* ? ^
      switch (token) {
        case END_OBJECT:
          // Position: { (key: value)* } ^
          return true;

        case FIELD_NAME:
          // Position: { (key: value)* key: ^
          parseMember();
          break;

        default:
          // Position: { (key: value)* ~(key | }) ^
          // Invalid JSON.
          // Actually, we probably won't get here, the JSON parser
          // itself will throw an exception.
          throw loader.syntaxError(token);
      }
    }
  }

  /**
   * Parse a field. Two cases. First, this is a field we've already seen. If so,
   * look up the parser for that field and use it. If this is the first time we've
   * seen the field, "sniff" tokens to determine field type, create a parser,
   * then parse.
   */

  private void parseMember() {
    // Position: key: ^ ?
    final String key = loader.tokenizer.textValue().trim();
    JsonElementParser fieldState = members.get(key);
    if (fieldState == null) {
      // New key; sniff the value to determine the parser to use
      // (which also tell us the kind of column to create in Drill.)
      // Position: key: ^
      fieldState = detectValueParser(key);
      members.put(key, fieldState);
    }
    // Parse the field value using the parser for that field.
    // The structure implies that fields don't change types: the type of
    // the first value (sniffed above) must be repeated for every subsequent
    // object.
    // Position: key: ^ value ...
    fieldState.parse();
  }

  /**
   * If the column is not projected, create a dummy parser to "free wheel" over
   * the value. Otherwise, look ahead a token or two to determine the the type
   * of the field. Then the caller will backtrack to parse the field.
   *
   * @param key name of the field
   * @return parser for the field
   */
  JsonElementParser detectValueParser(final String key) {
    if (key.isEmpty()) {
      throw loader.syntaxError("Drill does not allow empty keys in JSON key/value pairs");
    }
    if (!projection.isProjected(key)) {
      return new DummyValueParser(this, key);
    }

    // For other types of projection, the projection
    // mechanism will catch conflicts.

    JsonToken token = loader.tokenizer.requireNext();
    // Position: key: ? ^
    JsonElementParser valueParser;
    switch (token) {
      case START_ARRAY:
        // Position: key: [ ^
        valueParser = detectArrayParser(key);
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
    loader.tokenizer.unget(token);
    // Position: key: ^ ?
    return valueParser;
  }

  @Override
  protected ObjectWriter newWriter(String key,
        MinorType type, DataMode mode) {
    int index = writer.addColumn(schemaFor(key, type, mode));
    return writer.column(index);
  }

  @Override
  protected JsonElementParser nullArrayParser(String key) {
    return new ObjectParser.NullArrayParser(this, key);
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
    JsonElementParser valueParser = inferMember(key);
    if (valueParser == null) {
      valueParser = new NullTypeParser(this, key);
    }
    return valueParser;
  }

  /**
   * Experimental feature. If a type hint mechanism is provided, use that
   * to resolve the type of null columns. Really, this should be done earlier,
   * and we should ensure that the suggested type matches the actual JSON
   * type. But, for now, we consult the hint only if we'd otherwise not know
   * the type.
   */
  private JsonElementParser inferMember(String key) {
    if (!loader.options.detectTypeEarly) {
      return null;
    }
    JsonElementParser parser = inferMemberFromType(key);
    if (parser == null) {
      parser = inferFromHint(key);
    }
    return parser;
  }

  JsonElementParser inferMemberFromType(String key) {
    MajorType type = projection.typeOf(key);
    if (type == null) {
      return null;
    }
    DataMode mode = type.getMode();
    MinorType dataType = type.getMinorType();
    switch (dataType) {
      case MAP:
        if (mode == DataMode.REPEATED) {
          return objectArrayParser(key);
        } else {
          return objectParser(key);
        }
      case LIST:
        return listParser(key);

      default:
    }
    if (mode == DataMode.REQUIRED) {
      JsonLoaderImpl.logger.warn("Cardinality conflict! JSON source {}, column {} " +
          "has a hint of cardinality {}, but JSON requires OPTIONAL. " +
          "Hint ignored.",
          loader.options.context, fullName(), mode.name());
      mode = DataMode.OPTIONAL;
    }
    ObjectWriter memberWriter = newWriter(key, dataType, mode);
    if (mode == DataMode.OPTIONAL) {
      return scalarParserForType(dataType, key, memberWriter.scalar());
    } else {
      ArrayWriter arrayWriter = memberWriter.array();
      JsonElementParser elementParser = scalarParserForType(type.getMinorType(), key, arrayWriter.scalar());
      return new ArrayParser.ScalarArrayParser(this, key, arrayWriter, elementParser);
    }
  }

  private JsonElementParser inferFromHint(String key) {
    TupleProjection.Hint hint = projection.typeHint(key);
    if (hint == null) {
      return null;
    }
    switch (hint) {
    case ARRAY:
      return nullArrayParser(key);
    case MAP:
      return objectParser(key);
    case MAP_ARRAY:
      return objectArrayParser(key);
    default:
      return null;
    }
  }

  private ArrayParser inferScalarArrayFromHint(String key) {
    MajorType type = projection.typeOf(key);
    if (type == null) {
      return null;
    }
    if (type.getMode() != DataMode.REPEATED) {
      JsonLoaderImpl.logger.warn("Cardinality conflict! JSON source {}, array {} " +
          "has a hint of cardinality {}, but JSON requires REPEATED. " +
          "Hint ignored.",
          loader.options.context, fullName(), type.getMode().name());
    }
    ArrayWriter arrayWriter = newWriter(key, type.getMinorType(), DataMode.REPEATED).array();
    JsonElementParser elementState = scalarParserForType(type.getMinorType(), key, arrayWriter.scalar());
    return new ArrayParser.ScalarArrayParser(this, key, arrayWriter, elementState);
  }

  @Override
  public ColumnMetadata schema() { return writer.schema(); }
}
