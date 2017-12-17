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
package org.apache.drill.exec.store.easy.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;
import org.apache.drill.exec.vector.accessor.VariantWriter;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Revised JSON loader that is based on the
 * {@link ResultSetLoader} abstraction. Represents the JSON parse as a
 * set of parse states, each of which represents some point in the traversal
 * of the JSON syntax. Parse nodes also handle options such as all text mode
 * vs. type-specific parsing. Think of this implementation as a
 * recursive-descent parser, with the parser itself discovered and
 * "compiled" on the fly as we see incoming data.
 * <p>
 * Actual parsing is handled by the Jackson parser class. The input source is
 * represented as an {@link InputStream} so that this mechanism can parse files
 * or strings.
 * <p>
 * Fields are discovered on the fly. Types are inferred from the first JSON token
 * for a field. Type inference is less than perfect: it cannot handle type changes
 * such as first seeing 10, then 12.5, or first seeing "100", then 200.
 *
 * <h4>Comparison to Original JSON Reader</h4>
 *
 * This class replaces the {@link JsonReader} class used in Drill versions 1.12
 * and before. Compared with the previous version, this implementation:
 * <ul>
 * <li>Materializes parse states as classes rather than as methods and
 * boolean flags as in the prior version.</li>
 * <li>Reports errors as {@link UserException} objects, complete with context
 * information, rather than as generic Java exception as in the prior version.</li>
 * <li>Moves parse options into a separate {@link JsonOptions} class.</li>
 * <li>Iteration protocol is simpler: simply call {@link #next()} until it returns
 * <tt>false</tt>. Errors are reported out-of-band via an exception.</li>
 * <li>The result set loader abstraction is perfectly happy with an empty schema.
 * For this reason, this version (unlike the original) does not make up a dummy
 * column if the schema would otherwise be empty.</li>
 * <li>Projection pushdown is handled by the {@link ResultSetLoader} rather than
 * the JSON loader. This class always creates a vector writer, but the result set
 * loader will return a dummy (no-op) writer for non-projected columns.</li>
 * <li>Like the original version, this version "free wheels" over unprojected objects
 * and arrays; watching only for matching brackets, but ignoring all else.</li>
 * <li>Writes boolean values as SmallInt values, rather than as bits in the
 * prior version.</li>
 * <li>This version also "free-wheels" over all unprojected values. If the user
 * finds that they have inconsistent data in some field f, then the user can
 * project fields except f; Drill will ignore the inconsistent values in f.</li>
 * <li>Because of this free-wheeling capability, this version does not need a
 * "counting" reader; this same reader handles the case in which no fields are
 * projected for <tt>SEELCT COUNT(*)</tt> queries.</li>
 * <li>Runs of null values result in a "deferred null state" that patiently
 * waits for an actual value token to appear, and only then "realizes" a parse
 * state for that type.</li>
 * <li>Provides the same limited error recovery as the original version. See
 * <a href="https://issues.apache.org/jira/browse/DRILL-4653">DRILL-4653</a>
 * and
 * <a href="https://issues.apache.org/jira/browse/DRILL-5953">DRILL-5953</a>.
 * </li>
 * </ul>
 */

public class JsonLoaderImpl implements JsonLoader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonLoaderImpl.class);

  private static final String ROOT_NAME = "<root>";

  public interface TypeNegotiator {
    MajorType typeOf(List<String> path);
  }

  public static class JsonOptions {
    public String context;
    public boolean allTextMode;
    public boolean extended = true;
    public boolean readNumbersAsDouble;

    /**
     * Allow Infinity and NaN for float values.
     */

    public boolean allowNanInf;

    /**
     * Describes whether or not this reader can unwrap a single root array record
     * and treat it like a set of distinct records.
     */
    public boolean skipOuterList = true;
    public boolean skipMalformedRecords;
    public boolean unionEnabled;

    public TypeNegotiator typeNegotiator;
    public boolean useArrayTypes;
  }

  @SuppressWarnings("serial")
  private class RecoverableJsonException extends RuntimeException {
  }

  interface JsonElementParser {
    String fieldName();
    JsonElementParser parent();
    boolean isContainer();
    boolean parse();
  }

  interface NullTypeMarker {
    boolean isEmptyArray();
    void realizeAsText();
  }

  protected abstract class AbstractParser implements JsonElementParser {
    private final JsonElementParser parent;
    private final String fieldName;

    public AbstractParser(JsonElementParser parent, String fieldName) {
      this.parent = parent;
      this.fieldName = fieldName;
    }

    @Override
    public String fieldName() { return fieldName; }
    @Override
    public JsonElementParser parent() { return parent; }
    @Override
    public boolean isContainer() { return false; }
  }

  /**
   * Parses [ value, value ... ]<br>
   * Where value is a scalar. The states for each value ensure that the
   * types are consistent (Drill does not support heterogeneous arrays.)
   */

  protected class ScalarArrayParser extends AbstractParser {

    @SuppressWarnings("unused")
    private final ArrayWriter writer;
    private final JsonElementParser scalarState;

    public ScalarArrayParser(JsonElementParser parent, String fieldName,
        ArrayWriter writer,
        JsonElementParser scalarState) {
      super(parent, fieldName);
      this.writer = writer;
      this.scalarState = scalarState;
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        return true;
      case START_ARRAY:
        writer.setNull(false);
        break;
      default:
        throw syntaxError(token);
      }

      for (;;) {
        token = tokenizer.requireNext();
        switch (token) {
        case END_ARRAY:
          return true;

        default:
          tokenizer.unget(token);
          scalarState.parse();
          writer.save();
          break;
        }
      }
    }

    @Override
    public boolean isContainer() { return true; }
  }

  /**
   * Parses [{ ... }, {...} ...]
   */

  protected class ObjectArrayParser extends AbstractParser {

    private final ArrayWriter writer;
    private final ObjectParser tupleState;

    public ObjectArrayParser(JsonElementParser parent, String fieldName,
        ArrayWriter writer, ObjectParser tupleState) {
      super(parent, fieldName);
      this.writer = writer;
      this.tupleState = tupleState;
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        return true;
      case START_ARRAY:
        break;
      default:
        throw syntaxError(token);
      }

      for (;;) {
        token = tokenizer.requireNext();
        switch (token) {
        case END_ARRAY:
          return true;

        case START_OBJECT:
          tokenizer.unget(token);
          tupleState.parse();
          writer.save();
          break;

        default:
          throw syntaxError(token);
        }
      }
    }

    @Override
    public boolean isContainer() { return true; }
  }

  /**
   * Represents a rather odd state: we have seen a value of one or more nulls,
   * but we have not yet seen a value that would give us a type. This state
   * acts as a placeholder; waiting to see the type, at which point it replaces
   * itself with the actual typed state. If a batch completes with only nulls
   * for this field, then the field becomes a Text field and all values in
   * subsequent batches will be read in "text mode" for that one field in
   * order to avoid a schema change.
   * <p>
   * Note what this state does <i>not</i> do: it does not create a nullable
   * int field per Drill's normal (if less than ideal) semantics. First, JSON
   * <b>never</b> produces an int field, so nullable int is less than ideal.
   * Second, nullable int has no basis in reality and so is a poor choice
   * on that basis.
   */

  protected class NullTypeParser extends AbstractParser implements NullTypeMarker {

    private final ObjectParser tupleState;

    public NullTypeParser(ObjectParser parentState, String fieldName) {
      super(parentState, fieldName);
      this.tupleState = parentState;
      nullStates.add(this);
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.requireNext();

      // If value is the null token, we still don't know the type.

      if (token == JsonToken.VALUE_NULL) {
        return true;
      }

      // Replace ourself with a typed reader.

      tokenizer.unget(token);
      JsonElementParser newState = tupleState.detectValueParser(fieldName());
      tupleState.replaceState(fieldName(), newState);
      nullStates.remove(this);
      return newState.parse();
    }

    @Override
    public void realizeAsText() {
      logger.warn("Ambiguous type! JSON field {}" +
          " contains all nulls. Assuming text mode.",
          fieldName());
      JsonElementParser newState = new TextParser(parent(), fieldName(),
          tupleState.newWriter(fieldName(), MinorType.VARCHAR, DataMode.OPTIONAL).scalar());
      tupleState.replaceState(fieldName(), newState);
      nullStates.remove(this);
    }

    @Override
    public boolean isEmptyArray() { return false; }
  }

  /**
   * Parses: [] | null [ ... ]
   * <p>
   * This state remains in effect as long as the input contains empty arrays or
   * null values. However, once the array contains a non-empty array, detects the
   * type of the array based on this value and replaces this state with the
   * result array parser state.
   * <p>
   * If at the end of a batch, no non-empty array was seen, assumes that the
   * array, when seen, will be an array of scalars, and replaces this state with
   * a text array (as in all-text mode.)
   */

  protected class NullArrayParser extends AbstractParser implements NullTypeMarker {

    private final ObjectParser tupleState;

    public NullArrayParser(ObjectParser parentState, String fieldName) {
      super(parentState, fieldName);
      this.tupleState = parentState;
      nullStates.add(this);
    }

    /**
     * Parse null | [] | [ some_token
     */

    @Override
    public boolean parse() {
      JsonToken startToken = tokenizer.requireNext();
      if (startToken == JsonToken.VALUE_NULL) {
        return true;
      }
      if (startToken != JsonToken.START_ARRAY) {
        throw syntaxError(startToken);
      }
      JsonToken valueToken = tokenizer.requireNext();
      if (valueToken == JsonToken.END_ARRAY) {
        return true;
      }
      tokenizer.unget(valueToken);
      JsonElementParser newState = tupleState.detectArrayType(fieldName(), 1);
      tupleState.replaceState(fieldName(), newState);
      nullStates.remove(this);
      tokenizer.unget(startToken);
      return newState.parse();
    }

    @Override
    public void realizeAsText() {
      logger.warn("Ambiguous type! JJSON array {}" +
          " contains all empty arrays. Assuming text mode elements.",
          fieldName());
      ArrayWriter arrayWriter = tupleState.newWriter(fieldName(), MinorType.VARCHAR, DataMode.REPEATED).array();
      JsonElementParser elementState = new TextParser(tupleState, fieldName(), arrayWriter.scalar());
      JsonElementParser newState = new ScalarArrayParser(tupleState, fieldName(), arrayWriter, elementState);
      tupleState.replaceState(fieldName(), newState);
      nullStates.remove(this);
    }

    @Override
    public boolean isEmptyArray() { return true; }
  }

  protected class NullListParser extends AbstractParser implements NullTypeMarker {

    private final ObjectParser tupleState;

    public NullListParser(ObjectParser parentState, String fieldName) {
      super(parentState, fieldName);
      this.tupleState = parentState;
      nullStates.add(this);
      assert false;
    }

    @Override
    public boolean parse() {
      assert false;
      return false;
    }

    @Override
    public boolean isEmptyArray() {
      assert false;
      return false;
    }

    @Override
    public void realizeAsText() {
      assert false;
    }
  }

  /**
   * Parses { name : value ... }
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
   */

  protected class ObjectParser extends AbstractParser {

    private final TupleWriter writer;
    private final Map<String, JsonElementParser> members = CaseInsensitiveMap.newHashMap();

    public ObjectParser(JsonElementParser parent, String fieldName, TupleWriter writer) {
      super(parent, fieldName);
      this.writer = writer;
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.next();
      if (token == null) {
        return false; // EOF
      }
      switch (token) {
      case NOT_AVAILABLE:
        return false; // Should never occur

      case VALUE_NULL:
        return true; // Null, same as omitting the object

      case START_OBJECT:
        break; // Start the object

      default:
        throw syntaxError(token); // Nothing else is valid
      }

      // Parse (field: value)* }

      for (;;) {
        token = tokenizer.requireNext();
        switch (token) {
        case END_OBJECT:
          return true;

        case FIELD_NAME:
          parseMember();
          break;

        default:
          throw syntaxError(token);
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
      final String key = tokenizer.getText().trim();
      JsonElementParser fieldState = members.get(key);
      if (fieldState == null) {
        fieldState = detectValueParser(key);
        members.put(key, fieldState);
      }
      fieldState.parse();
    }

    /**
     * If the column is not projected, create a dummy parser to "free wheel"
     * over the value. Otherwise,
     * look ahead a token or two to determine the the type of the field.
     * Then the caller will backtrack to parse the field.
     *
     * @param key name of the field
     * @return parser for the field
     */

    private JsonElementParser detectValueParser(final String key) {
      if (key.isEmpty()) {
        throw syntaxError("Drill does not allow empty keys");
      }
      ProjectionType projType = writer.projectionType(key);
      if (projType == ProjectionType.UNPROJECTED) {
        return new DummyValueParser(this, key);
      }

      // For other types of projection, the projection
      // mechanism will catch conflicts.

      JsonToken token = tokenizer.requireNext();
      JsonElementParser state;
      switch (token) {
      case START_ARRAY:
        state = detectArrayType(key, 1);
        break;

      case START_OBJECT:
        state = new ObjectParser(this, key,
              newWriter(key, MinorType.MAP, DataMode.REQUIRED).tuple());
        break;

      case VALUE_NULL:

        // Use the projection type as a hint.

        switch (projType) {
        case ARRAY:
          state = new NullArrayParser(this, key);
          break;
        case TUPLE:
          state = objectParser(key);
          break;
        case TUPLE_ARRAY:

          // Note: Drill syntax does not support this
          // case yet.

          state = objectArrayParser(key);
          break;
        default:
          state = new NullTypeParser(this, key);
        }
        break;

      default:
        state = typedScalar(token, key);
      }
      tokenizer.unget(token);
      return state;
    }

    private ObjectParser objectParser(String key) {
      return new ObjectParser(this, key,
          newWriter(key, MinorType.MAP, DataMode.REQUIRED).tuple());
    }

    private ObjectArrayParser objectArrayParser(String key) {
      ArrayWriter arrayWriter = newWriter(key, MinorType.MAP, DataMode.REPEATED).array();
      return new ObjectArrayParser(this, key, arrayWriter,
          new ObjectParser(this, key, arrayWriter.tuple()));
    }

    private JsonElementParser typedScalar(JsonToken token, String key) {
      if (options.allTextMode) {
        return new TextParser(this, key,
            newWriter(key, MinorType.VARCHAR, DataMode.OPTIONAL).scalar());
      }
      switch (token) {
      case VALUE_FALSE:
      case VALUE_TRUE:
        return new BooleanParser(this, key,
            newWriter(key, MinorType.TINYINT, DataMode.OPTIONAL).scalar());

      case VALUE_NUMBER_INT:
        if (! options.readNumbersAsDouble) {
          return new IntParser(this, key,
              newWriter(key, MinorType.BIGINT, DataMode.OPTIONAL).scalar());
        } // else fall through

      case VALUE_NUMBER_FLOAT:
        return new FloatParser(this, key,
            newWriter(key, MinorType.FLOAT8, DataMode.OPTIONAL).scalar());

      case VALUE_STRING:
        return new StringParser(this, key,
            newWriter(key, MinorType.VARCHAR, DataMode.OPTIONAL).scalar());

      default:
        throw syntaxError(token);
      }
    }

    /**
     * Detect the type of an array by "sniffing" the first element. Requires
     * that array elements all be the same type. (JSON allows heterogeneous
     * arrays; Drill, being relational, requires homogeneous lists.)
     * <p>
     * Drill supports just one-dimensional lists. If this method finds a
     * 2- or higher dimensional list (a list of a list of a list, say), then
     * it reverts this one field to "text mode", capturing the array as
     * text which the client is free to parse.
     * <p>
     * Note that the nested array issue applies only to arrays directly
     * within arrays. Drill handles arrays that contain maps that contain
     * arrays.
     *
     * @param key field name
     * @param depth nesting depth of the array
     * @return the parse state for this array
     */

    private JsonElementParser detectArrayType(String key, int depth) {
      if (depth > 1) {
        // TODO: Handle via nested lists.
        throw syntaxError(JsonToken.START_ARRAY);
      }
      JsonToken token = tokenizer.requireNext();
      JsonElementParser arrayState = null;
      switch (token) {
      case START_ARRAY:
        arrayState = detectArrayType(key, depth + 1);
        break;

      case START_OBJECT:
        if (useList(depth)) {
          arrayState = objectListParser(key);
        } else {
          arrayState = objectArrayParser(key);
        }
        break;

      case END_ARRAY:
        if (useList(depth)) {
          arrayState = new NullListParser(this, key);
        } else {
          arrayState = new NullArrayParser(this, key);
        }
        break;

      default:
        if (useList(depth)) {
          arrayState = scalarListParser(token, key);
        } else {
          arrayState = scalarArrayParser(token, key);
        }
      }
      tokenizer.unget(token);
      return arrayState;
    }

    private boolean useList(int depth) {
      return ! options.useArrayTypes || depth > 1;
    }

    private JsonElementParser objectListParser(String key) {
      assert false;
      return null;
    }

    private JsonElementParser scalarArrayParser(JsonToken token, String fieldName) {
      ArrayWriter arrayWriter = null;
      JsonElementParser elementState = null;
      if (options.allTextMode) {
        arrayWriter = newWriter(fieldName, MinorType.VARCHAR, DataMode.REPEATED).array();
        elementState = new TextParser(this, fieldName, arrayWriter.scalar());
        return new ScalarArrayParser(this, fieldName, arrayWriter, elementState);
      }
      switch (token) {
      case VALUE_FALSE:
      case VALUE_TRUE:
        arrayWriter = newWriter(fieldName, MinorType.TINYINT, DataMode.REPEATED).array();
        elementState = new BooleanParser(this, fieldName, arrayWriter.scalar());
        break;

      case VALUE_NUMBER_INT:
        if (! options.readNumbersAsDouble) {
          arrayWriter = newWriter(fieldName, MinorType.BIGINT, DataMode.REPEATED).array();
          elementState = new IntParser(this, fieldName, arrayWriter.scalar());
          break;
        } // else fall through

      case VALUE_NUMBER_FLOAT:
        arrayWriter = newWriter(fieldName, MinorType.FLOAT8, DataMode.REPEATED).array();
        elementState = new FloatParser(this, fieldName, arrayWriter.scalar());
        break;

      case VALUE_STRING:
        arrayWriter = newWriter(fieldName, MinorType.VARCHAR, DataMode.REPEATED).array();
        elementState = new StringParser(this, fieldName, arrayWriter.scalar());
        break;

      default:
        throw syntaxError(token);
      }
      return new ScalarArrayParser(this, fieldName, arrayWriter, elementState);
    }

    private JsonElementParser scalarListParser(JsonToken token, String fieldName) {
      ArrayWriter arrayWriter = newWriter(fieldName, MinorType.LIST, DataMode.OPTIONAL).array();
      VariantWriter bodyWriter = arrayWriter.variant();
      JsonElementParser elementState = null;
      if (options.allTextMode) {
        elementState = new TextParser(this, fieldName, bodyWriter.scalar(MinorType.VARCHAR));
        return new ScalarArrayParser(this, fieldName, arrayWriter, elementState);
      }
      switch (token) {
      case VALUE_FALSE:
      case VALUE_TRUE:
        elementState = new BooleanParser(this, fieldName, bodyWriter.scalar(MinorType.TINYINT));
        break;

      case VALUE_NUMBER_INT:
        if (! options.readNumbersAsDouble) {
          elementState = new IntParser(this, fieldName, bodyWriter.scalar(MinorType.BIGINT));
          break;
        } // else fall through

      case VALUE_NUMBER_FLOAT:
        elementState = new FloatParser(this, fieldName, bodyWriter.scalar(MinorType.FLOAT8));
        break;

      case VALUE_STRING:
        elementState = new StringParser(this, fieldName, bodyWriter.scalar(MinorType.VARCHAR));
        break;

      default:
        throw syntaxError(token);
      }
      return new ScalarArrayParser(this, fieldName, arrayWriter, elementState);
    }

    /**
     * The field type has been determined. Build a writer for that field given
     * the field name, type and mode (optional or repeated). The result set loader
     * that backs this JSON loader will handle field projection, returning a dummy
     * parser if the field is not projected.
     *
     * @param fieldName name of the field
     * @param type Drill data type
     * @param mode cardinality: either Optional (for map fields) or Repeated
     * (for array members). (JSON does not allow Required fields)
     * @return the object writer for the field, which may be a tuple, scalar
     * or array writer, depending on type
     */

    private ObjectWriter newWriter(String fieldName,
          MinorType type, DataMode mode) {
      MaterializedField field = MaterializedField.create(fieldName,
          MajorType.newBuilder()
            .setMinorType(type)
            .setMode(mode)
            .build());
      int index = writer.addColumn(field);
      return writer.column(index);
    }

    private void replaceState(String fieldName, JsonElementParser newState) {
      assert members.containsKey(fieldName);
      members.put(fieldName, newState);
    }
  }

  /**
   * Parse and ignore an unprojected value. The parsing just "free wheels", we
   * care only about matching brackets, but not about other details.
   */

  protected class DummyValueParser extends AbstractParser {

    public DummyValueParser(JsonElementParser parent, String fieldName) {
      super(parent, fieldName);
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.requireNext();
      switch (token) {
      case START_ARRAY:
      case START_OBJECT:
        parseTail();
        break;

      case VALUE_NULL:
      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
      case VALUE_STRING:
        break;

      default:
        throw syntaxError(token);
      }
      return true;
    }

    public void parseTail() {

      // Parse (field: value)* }

      for (;;) {
        JsonToken token = tokenizer.requireNext();
        switch (token) {

        // Not exactly precise, but the JSON parser handles the
        // details.

        case END_OBJECT:
        case END_ARRAY:
          return;

        case START_OBJECT:
        case START_ARRAY:
          parseTail(); // Recursively ignore objects
          break;

        default:
          break; // Ignore all else
        }
      }
    }
  }

  protected abstract class ScalarParser extends AbstractParser {
    protected final ScalarWriter writer;

    public ScalarParser(JsonElementParser parent, String fieldName, ScalarWriter writer) {
      super(parent, fieldName);
      this.writer = writer;
    }
  }

  /**
   * Parses true | false | null
   */

  public class BooleanParser extends ScalarParser {

    public BooleanParser(JsonElementParser parent, String fieldName,
        ScalarWriter writer) {
      super(parent, fieldName, writer);
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        writer.setNull();
        break;
      case VALUE_TRUE:
        try {
          writer.setInt(1);
        } catch (UnsupportedConversionError e) {
          throw typeError(e);
        }
        break;
      case VALUE_FALSE:
        try {
          writer.setInt(0);
        } catch (UnsupportedConversionError e) {
          throw typeError(e);
        }
        break;
      default:
        throw syntaxError(token, fieldName(), "Boolean");
      }
      return true;
    }
  }

  /**
   * Parses integer | null
   */

  public class IntParser extends ScalarParser {

    public IntParser(JsonElementParser parent, String fieldName,
        ScalarWriter writer) {
      super(parent, fieldName, writer);
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        writer.setNull();
        break;
      case VALUE_NUMBER_INT:
        try {
          writer.setLong(parser.getLongValue());
        } catch (IOException e) {
          throw ioException(e);
        } catch (UnsupportedConversionError e) {
          throw typeError(e);
        }
        break;
      default:
        throw syntaxError(token, fieldName(), "Integer");
      }
      return true;
    }
  }

  /**
   * Parses float | integer | null
   * <p>
   * The integer value is allowed only after seeing a float value which
   * sets the type.
   */

  public class FloatParser extends ScalarParser {

    public FloatParser(JsonElementParser parent, String fieldName,
        ScalarWriter writer) {
      super(parent, fieldName, writer);
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        writer.setNull();
        break;
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
        try {
          writer.setDouble(parser.getValueAsDouble());
        } catch (IOException e) {
          throw ioException(e);
        } catch (UnsupportedConversionError e) {
          throw typeError(e);
        }
        break;
      default:
        throw syntaxError(token, fieldName(), "Float");
      }
      return true;
    }
  }

  /**
   * Parses "str" | null
   */

  public class StringParser extends ScalarParser {

    public StringParser(JsonElementParser parent, String fieldName,
        ScalarWriter writer) {
      super(parent, fieldName, writer);
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        writer.setNull();
        break;
      case VALUE_STRING:
        try {
          writer.setString(parser.getValueAsString());
        } catch (IOException e) {
          throw ioException(e);
        } catch (UnsupportedConversionError e) {
          throw typeError(e);
        }
        break;
      default:
        throw syntaxError(token, fieldName(), "String");
      }
      return true;
    }
  }

  /**
   * Parses "str" | true | false | integer | float
   * <p>
   * Returns the result as a string.
   * <p>
   * If the top level value is a scalar:
   * <ul>
   * <li><tt>null</tt> is mapped to a Drill NULL.</li>
   * <li>Strings are stored in Drill unquoted.</li>
   * </ul>
   * If the top level is a map or array, then scalars within
   * that structure:
   * <ul>
   * <li><tt>null</tt> is mapped to the literal "null".</li>
   * <li>Strings are quoted.</li>
   * </ul>
   */

  public class TextParser extends ScalarParser {

    private final boolean isArray;

    public TextParser(JsonElementParser parent, String fieldName,
        ScalarWriter writer) {
      super(parent, fieldName, writer);
      this.isArray = writer.schema().isArray();
    }

    @Override
    public boolean parse() {
      JsonToken token = tokenizer.requireNext();
      switch (token) {
      case VALUE_NULL:
        if (isArray) {
          try {
            writer.setString("");
          } catch (UnsupportedConversionError e) {
            throw typeError(e);
          }
        } else {
          writer.setNull();
        }
        return true;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
      case VALUE_STRING:
        writer.setString(tokenizer.getText());
        return true;

      default:
        throw syntaxError(token);
      }
    }
  }

  /**
   * Parses ^[ ... ]$
   */

  protected class RootArrayParser extends AbstractParser {

    private RowSetLoader rootWriter;
    private ObjectParser rootTuple;

    public RootArrayParser(RowSetLoader rootWriter) {
      super(null, ROOT_NAME);
      this.rootWriter = rootWriter;
      rootTuple = new ObjectParser(this, ROOT_NAME, rootWriter);
    }

    @Override
    public boolean parse() {
      rootWriter.start();
      JsonToken token = tokenizer.requireNext();
      if (token == JsonToken.END_ARRAY) {
        return false;
      }
      tokenizer.unget(token);
      rootTuple.parse();
      rootWriter.save();
      return true;
    }
  }

  /**
   * Parses:
   * <ul>
   * <li>^{ ... }$</li>
   * <li>^{ ... } { ... } ...$</li>
   * </ul>
   */

  protected class RootTupleState extends ObjectParser {

    private final RowSetLoader rootWriter;

    public RootTupleState(RowSetLoader rootWriter) {
      super(null, ROOT_NAME, rootWriter);
      this.rootWriter = rootWriter;
    }

    @Override
    public boolean parse() {
      if (! rootWriter.start()) {
        throw new IllegalStateException("Caller must check isFull()");
      }
      if (! super.parse()) {
        return false;
      }
      rootWriter.save();
      return true;
    }

    @Override
    public boolean isContainer() { return true; }
  }

  public static final int MAX_LOOKAHEAD = 30;

  public class TokenIterator {
    private JsonToken[] lookahead = new JsonToken[MAX_LOOKAHEAD];
    private int count;

    public JsonToken next() {
      if (count > 0) {
        return lookahead[--count];
      }
      try {
        return parser.nextToken();
      } catch (JsonParseException e) {
        if (options.skipMalformedRecords) {
          throw new RecoverableJsonException();
        } else {
          throw UserException
            .dataReadError(e)
            .addContext("Location", context())
            .build(logger);
        }
      } catch (IOException e) {
        throw ioException(e);
      }
    }

    public String context() {
      JsonLocation location = parser.getCurrentLocation();
      if (location == null) {
        return options.context;
      }
      String token;
      try {
        token = parser.getText();
      } catch (IOException e) {
        token = "<unknown>";
      }
      return new StringBuilder()
          .append(options.context)
          .append(", line ")
          .append(location.getLineNr())
          .append(", column ")
          .append(location.getColumnNr())
          .append(", near token \"")
          .append(token)
          .append("\"")
          .toString();
    }

    public JsonToken requireNext() {
      JsonToken token = next();
      if (token == null) {
        throw UserException
          .dataReadError()
          .message("Premature EOF of JSON file")
          .addContext("Location", tokenizer.context())
          .build(logger);
      }
      return token;
    }

    public void unget(JsonToken token) {
      if (count == lookahead.length) {
        throw UserException
          .dataReadError()
          .message("Excessive JSON array nesting")
          .addContext("Max allowed", lookahead.length)
          .addContext("Location", tokenizer.context())
          .build(logger);
      }
      lookahead[count++] = token;
    }

    public String getText() {
      try {
        return parser.getText();
      } catch (IOException e) {
        throw ioException(e);
      }
    }
  }

  private final JsonParser parser;
  private final RowSetLoader rootWriter;
  private final JsonOptions options;
  private final TokenIterator tokenizer;

  // Using a simple list. Won't perform well if we have hundreds of
  // null fields; but then we've never seen such a pathologically bad
  // case... Usually just one or two fields have deferred nulls.

  private final List<NullTypeMarker> nullStates = new ArrayList<>();
  private JsonElementParser rootState;
  private int errorRecoveryCount;

  public JsonLoaderImpl(InputStream stream, RowSetLoader rootWriter, JsonOptions options) {
    try {
      ObjectMapper mapper = new ObjectMapper()
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
          .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      if (options.allowNanInf) {
        mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
      }

      parser = mapper.getFactory().createParser(stream);
    } catch (JsonParseException e) {
      throw UserException
          .internalError(e)
          .addContext("Failed to create the JSON parser")
          .addContext("Source", options.context)
          .build(logger);
    } catch (IOException e) {
      throw ioException(e);
    }
    this.rootWriter = rootWriter;
    this.options = options;
    tokenizer = new TokenIterator();
    rootState = makeRootState();
  }

  private JsonElementParser makeRootState() {
    JsonToken token = tokenizer.next();
    if (token == null) {
      return null;
    }
    switch (token) {

    // File contains an array of records.

    case START_ARRAY:
      if (options.skipOuterList) {
        return new RootArrayParser(rootWriter);
      } else {
        throw UserException
          .dataReadError()
          .message("JSON includes an outer array, but outer array support is not enabled")
          .addContext("Location", tokenizer.context())
          .build(logger);
      }

    // File contains a sequence of one or more records,
    // presumably sequentially.

    case START_OBJECT:
      tokenizer.unget(token);
      return new RootTupleState(rootWriter);

    // Not a valid JSON file for Drill.

    default:
      throw syntaxError(token);
    }
  }

  @Override
  public boolean next() {
    if (rootState == null) {
      return false;
    }

    // From original code.
    // Does this ever actually occur?

    if (parser.isClosed()) {
      rootState = null;
      return false;
    }
    for (;;) {
      try {
        return rootState.parse();
      } catch (RecoverableJsonException e) {
        if (! recover()) {
          return false;
        }
      }
    }
  }

  /**
   * Finish reading a batch of data. We may have pending "null" columns:
   * a column for which we've seen only nulls, or an array that has
   * always been empty. The batch needs to finish, and needs a type,
   * but we still don't know the type. Since we must decide on one,
   * we do the following:
   * <ul>
   * <li>If given a type negotiator, ask it the type. Perhaps a
   * prior reader in the same scanner determined the type in a
   * previous file.</li>
   * <li>Guess Varchar, and switch to text mode.</li>
   * </ul>
   *
   * Note that neither of these choices is perfect. There is no guarantee
   * that prior files either were seen, or have the same type as this
   * file. Also, switching to text mode means results will vary
   * from run to run depending on the order that we see empty and
   * non-empty values for this column. Plus, since the system is
   * distributed, the decision made here may conflict with that made in
   * some other fragment.
   * <p>
   * The only real solution is for the user to provide schema
   * information to resolve the ambiguity; but Drill has no way to
   * gather that information at present.
   * <p>
   * Bottom line: the user is responsible for not giving Drill
   * ambiguous data that would require Drill to predict the future.
   */

  @Override
  public void endBatch() {
    List<NullTypeMarker> copy = new ArrayList<>();
    copy.addAll(nullStates);
    for (NullTypeMarker state : copy) {
      MajorType type = null;
      if (options.typeNegotiator != null) {
        type = options.typeNegotiator.typeOf(makePath((JsonElementParser) state));
      }
      if (type == null) {
        state.realizeAsText();
      } else {
        realizeAsType(state, type);
      }
    }
    assert nullStates.isEmpty();
  }

  private void realizeAsType(NullTypeMarker marker, MajorType type) {
    JsonElementParser state = (JsonElementParser) marker;
    MinorType dataType = type.getMinorType();
    if (marker.isEmptyArray()) {
      if (type.getMode() != DataMode.REPEATED) {
        marker.realizeAsText();
      } else {
        realizeAsArray(marker, state, dataType);
      }
    } else {
      switch (type.getMode()) {
      case OPTIONAL:
        realizeAsScalar(marker, state, dataType);
        break;
      case REPEATED:
        realizeAsArray(marker, state, dataType);
        break;
      default:
        marker.realizeAsText();
      }
    }
  }

  private void realizeAsArray(NullTypeMarker marker, JsonElementParser state, MinorType type) {
    ObjectParser tupleState = (ObjectParser) state.parent();
    ArrayWriter arrayWriter = tupleState.newWriter(state.fieldName(), type, DataMode.REPEATED).array();
    ScalarWriter scalarWriter = arrayWriter.scalar();
    JsonElementParser elementState = stateForType(type, tupleState, state.fieldName(), scalarWriter);
    logger.warn("Ambiguous type! JSON array {} contains all empty arrays. " +
        "Assuming element type from prior file: {}",
        state.fieldName(), type.toString());
    JsonElementParser newState = new ScalarArrayParser(tupleState, state.fieldName(), arrayWriter, elementState);
    tupleState.replaceState(state.fieldName(), newState);
    nullStates.remove(marker);
  }

  private void realizeAsScalar(NullTypeMarker marker, JsonElementParser state, MinorType type) {
    ObjectParser tupleState = (ObjectParser) state.parent();
    ScalarWriter scalarWriter = tupleState.newWriter(state.fieldName(), type, DataMode.OPTIONAL).scalar();
    JsonElementParser newState = stateForType(type, tupleState, state.fieldName(), scalarWriter);
    logger.warn("Ambiguous type! JSON field {} contains all nulls. " +
        "Assuming element type from prior file: {}",
        state.fieldName(), type.toString());
    tupleState.replaceState(state.fieldName(), newState);
    nullStates.remove(marker);
  }

  private JsonElementParser stateForType(MinorType type, JsonElementParser parent, String fieldName, ScalarWriter scalarWriter) {
    switch (type) {
    case BIGINT:
      return new IntParser(parent, fieldName, scalarWriter);
    case FLOAT8:
      return new FloatParser(parent, fieldName, scalarWriter);
    case TINYINT:
      return new BooleanParser(parent, fieldName, scalarWriter);
    case VARCHAR:
      return new StringParser(parent, fieldName, scalarWriter);
    default:
      throw new IllegalStateException("Unsupported Drill type " + type.toString() + " for JSON array");
    }
  }

  /**
   * Attempt recovery from a JSON syntax error by skipping to the next
   * record. The Jackson parser is quite limited in its recovery abilities.
   *
   * @return <tt>true<tt> if another record can be read, <tt>false</tt>
   * if EOF.
   * @throws UserException if the error is unrecoverable
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-4653">DRILL-4653</a>
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-5953">DRILL-5953</a>
   */

  private boolean recover() {
    logger.warn("Attempting recovery from JSON syntax error. " + tokenizer.context());
    boolean firstAttempt = true;
    for (;;) {
      for (;;) {
        try {
          if (parser.isClosed()) {
            throw unrecoverableError();
          }
          JsonToken token = tokenizer.next();
          if (token == null) {
            if (firstAttempt) {
              throw unrecoverableError();
            }
            return false;
          }
          if (token == JsonToken.NOT_AVAILABLE) {
            return false;
          }
          if (token == JsonToken.END_OBJECT) {
            break;
          }
          firstAttempt = false;
        } catch (RecoverableJsonException e) {
          // Ignore, keep trying
        }
      }
      try {
        JsonToken token = tokenizer.next();
        if (token == null || token == JsonToken.NOT_AVAILABLE) {
          return false;
        }
        if (token == JsonToken.START_OBJECT) {
          logger.warn("Attempting to resume JSON parse. " + tokenizer.context());
          tokenizer.unget(token);
          errorRecoveryCount++;
          return true;
        }
      } catch (RecoverableJsonException e) {
        // Ignore, keep trying
      }
    }
  }

  public int recoverableErrorCount() { return errorRecoveryCount; }

  private UserException unrecoverableError() {
    throw UserException
        .dataReadError()
        .message("Unrecoverable JSON syntax error.")
        .addContext("Location", tokenizer.context())
        .build(logger);
  }

  private UserException syntaxError(JsonToken token, String context, String expected) {
    if (options.skipMalformedRecords) {
      throw new RecoverableJsonException();
    } else {
      return UserException
          .dataReadError()
          .message("JSON encountered a value of the wrong type")
          .message("Field", context)
          .message("Expected type", expected)
          .message("Actual token", token.toString())
          .addContext("Location", tokenizer.context())
          .build(logger);
    }
  }

  private UserException syntaxError(JsonToken token) {
    return UserException
        .dataReadError()
        .message("JSON syntax error.")
        .addContext("Current token", token.toString())
        .addContext("Location", tokenizer.context())
        .build(logger);
  }

  private UserException syntaxError(String message) {
    return UserException
        .dataReadError()
        .message(message)
        .addContext("Location", tokenizer.context())
        .build(logger);
  }

  private UserException ioException(IOException e) {
    return UserException
        .dataReadError(e)
        .addContext("I/O error reading JSON")
        .addContext("Location", parser == null ? options.context : tokenizer.context())
        .build(logger);
  }

  private UserException typeError(UnsupportedConversionError e) {
    return UserException
        .dataReadError()
        .message(e.getMessage())
        .addContext("I/O error reading JSON")
        .addContext("Location", parser == null ? options.context : tokenizer.context())
        .build(logger);
  }

  @Override
  public void close() {
    if (errorRecoveryCount > 0) {
      logger.warn("Read JSON input {} with {} recoverable error(s).",
          options.context, errorRecoveryCount);
    }
    try {
      parser.close();
    } catch (IOException e) {
      logger.warn("Ignored failure when closing JSON source " + options.context, e);
    }
  }

  protected static List<String> makePath(JsonElementParser state) {
    List<String> path = new ArrayList<>();
    while (state != null) {
      if (! state.isContainer()) {
        path.add(state.fieldName());
      }
      state = state.parent();
    }
    Collections.reverse(path);
    return path;
  }
}
