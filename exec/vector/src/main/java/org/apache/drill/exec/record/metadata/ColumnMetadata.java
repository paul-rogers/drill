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
package org.apache.drill.exec.record.metadata;

import java.io.IOException;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.schema.parser.SchemaExprParser;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.joda.time.format.DateTimeFormatter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Metadata description of a column including names, types and structure
 * information.
 * <p>
 * <h4>Serialization</h4>
 *
 * Serialization is used in two places: 1) in the provided schema
 * definition, and 2) if included in a logical plan, as in the mock
 * data source.
 * <p>
 * The type, precision and scale are serialized together in SQL
 * format: INT, VARCHAR(20) or DECIMAL(8,4)
 * <p>
 * Properties are serialized with format and default as separate
 * fields, all other properties in the property list.
 */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.NONE,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  isGetterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"name", "type", "mode", "format", "default", "properties"})
public interface ColumnMetadata extends Propertied {

  /**
   * Predicted number of elements per array entry. Default is
   * taken from the often hard-coded value of 10.
   */
  String EXPECTED_CARDINALITY_PROP = DRILL_PROP_PREFIX + "cardinality";

  /**
   * Default value represented as a string.
   */
  String DEFAULT_VALUE_PROP = DRILL_PROP_PREFIX + "default";

  /**
   * Expected (average) width for variable-width columns.
   */
  String EXPECTED_WIDTH_PROP = DRILL_PROP_PREFIX + "width";

  /**
   * Optional format to use when converting to/from string values.
   */
  String FORMAT_PROP = DRILL_PROP_PREFIX + "format";

  /**
   * Indicates how to handle blanks. Must be one of the valid values defined
   * in AbstractConvertFromString. Normally set on the converter by the plugin
   * rather than by the user in the schema.
   */
  String BLANK_AS_PROP = DRILL_PROP_PREFIX + "blank-as";

  /**
   * Indicates whether to project the column in a wildcard (*) query.
   * Special columns may be excluded from projection. Certain "special"
   * columns may be available only when explicitly requested. For example,
   * the log reader has a "_raw" column which includes the entire input
   * line before parsing. This column can be requested explicitly:<br>
   * <tt>SELECT foo, bar, _raw FROM ...</tt><br>
   * but the column will <i>not</i> be included when using the wildcard:<br>
   * <tt>SELECT * FROM ...</tt>
   * <p>
   * Marking a column (either in the provided schema or the reader schema)
   * will prevent that column from appearing in a wildcard expansion.
   */
  String EXCLUDE_FROM_WILDCARD = DRILL_PROP_PREFIX + "special";

  ObjectWriter WRITER = new ObjectMapper().writerFor(ColumnMetadata.class);
  ObjectReader READER = new ObjectMapper().readerFor(ColumnMetadata.class);

  /**
   * Rough characterization of Drill types into metadata categories.
   * Various aspects of Drill's type system are very, very messy.
   * However, Drill is defined by its code, not some abstract design,
   * so the metadata system here does the best job it can to simplify
   * the messy type system while staying close to the underlying
   * implementation.
   */

  enum StructureType {

    /**
     * Primitive column (all types except List, Map and Union.)
     * Includes (one-dimensional) arrays of those types.
     */

    PRIMITIVE,

    /**
     * Map or repeated map. Also describes the row as a whole.
     */

    TUPLE,

    /**
     * Union or (non-repeated) list. (A non-repeated list is,
     * essentially, a repeated union.)
     */

    VARIANT,

    /**
     * A repeated list. A repeated list is not simply the repeated
     * form of a list, it is something else entirely. It acts as
     * a dimensional wrapper around any other type (except list)
     * and adds a non-nullable extra dimension. Hence, this type is
     * for 2D+ arrays.
     * <p>
     * In theory, a 2D list of, say, INT would be an INT column, but
     * repeated in to dimensions. Alas, that is not how it is. Also,
     * if we have a separate category for 2D lists, we should have
     * a separate category for 1D lists. But, again, that is not how
     * the code has evolved.
     */

    MULTI_ARRAY
  }

  int DEFAULT_ARRAY_SIZE = 10;

  int UNDEFINED = -1;

  int MAX_DECIMAL_PRECISION = 38;

  StructureType structureType();

  /**
   * Schema for <tt>TUPLE</tt> columns.
   *
   * @return the tuple schema
   */

  TupleMetadata mapSchema();

  /**
   * Schema for <tt>VARIANT</tt> columns.
   *
   * @return the variant schema
   */

  VariantMetadata variantSchema();

  /**
   * Schema of inner dimension for <tt>MULTI_ARRAY<tt> columns.
   * If an array is 3D, the outer column represents all 3 dimensions.
   * <tt>outer.childSchema()</tt> gives another <tt>MULTI_ARRAY</tt>
   * for the inner 2D array.
   * <tt>outer.childSchema().childSchema()</tt> gives a column
   * of some other type (but repeated) for the 1D array.
   * <p>
   * Sorry for the mess, but it is how the code works and we are not
   * in a position to revisit data type fundamentals.
   *
   * @return the description of the (n-1) st dimension.
   */

  ColumnMetadata childSchema();
  MaterializedField schema();
  MaterializedField emptySchema();
  @JsonProperty("name")
  String name();
  MinorType type();
  MajorType majorType();
  @JsonProperty("mode")
  DataMode mode();
  int dimensions();
  boolean isNullable();
  boolean isArray();
  boolean isVariableWidth();
  boolean isMap();
  boolean isVariant();
  boolean isDict();

  /**
   * Determine if the schema represents a column with a LIST type with
   * UNION elements. (Lists can be of a single
   * type (with nullable elements) or can be of unions.)
   *
   * @return true if the column is of type LIST of UNIONs
   */

  @JsonIgnore
  boolean isMultiList();

  /**
   * Report whether one column is equivalent to another. Columns are equivalent
   * if they have the same name, type and structure (ignoring internal structure
   * such as offset vectors.)
   */

  boolean isEquivalent(ColumnMetadata other);

  /**
   * For variable-width columns, specify the expected column width to be used
   * when allocating a new vector. Does nothing for fixed-width columns.
   *
   * @param width the expected column width
   */

  void setExpectedWidth(int width);

  /**
   * Get the expected width for a column. This is the actual width for fixed-
   * width columns, the specified width (defaulting to 50) for variable-width
   * columns.
   * @return the expected column width of the each data value. Does not include
   * "overhead" space such as for the null-value vector or offset vector
   */

  int expectedWidth();

  /**
   * For an array column, specify the expected average array cardinality.
   * Ignored for non-array columns. Used when allocating new vectors.
   *
   * @param childCount the expected average array cardinality. Defaults to
   * 1 for non-array columns, 10 for array columns
   */

  void setExpectedElementCount(int childCount);

  /**
   * Returns the expected array cardinality for array columns, or 1 for
   * non-array columns.
   *
   * @return the expected value cardinality per value (per-row for top-level
   * columns, per array element for arrays within lists)
   */

  int expectedElementCount();

  void setFormat(String value);

  @JsonProperty("format")
  String format();

  /**
   * Returns the formatter to use for date/time values. Only valid for
   * date/time columns.
   *
   * @return
   */
  DateTimeFormatter dateTimeFormatter();

  /**
   * Sets the default value property using the string-encoded form of the value.
   * The default value is used for filling a vector when no real data is available.
   *
   * @param value the default value in String representation
   */
  void setDefaultValue(String value);

  /**
   * Returns the default value for this column in String literal representation.
   *
   * @return the default value in String literal representation, or null if no
   * default value has been set
   */
  @JsonProperty("default")
  String defaultValue();

  /**
   * Returns the default value decoded into object form. This is the same as:
   * <pre><code>decodeValue(defaultValue());
   * </code></pre>
   *
   * @return the default value decode as an object that can be passed to
   * the {@link ColumnWriter#setObject()} method.
   */
  Object decodeDefaultValue();

  String valueToString(Object value);
  Object valueFromString(String value);

  /**
   * Extended properties, without the format and default value;
   * primarily for serialization.
   * @return the extended properties, or null if no properties
   */

  @JsonProperty("properties")
  Map<String, String> extendedProperties();

  /**
   * Create an empty version of this column. If the column is a scalar,
   * produces a simple copy. If a map, produces a clone without child
   * columns.
   *
   * @return empty clone of this column
   */

  ColumnMetadata cloneEmpty();

  int precision();
  int scale();

  void bind(TupleMetadata parentTuple);

  ColumnMetadata copy();

  /**
   * Converts type metadata into string representation
   * accepted by the table schema parser.
   *
   * @return type metadata string representation
   */
  @JsonProperty("type")
  String sqlType();
  String typeString();

  /**
   * Converts column metadata into string representation
   * accepted by the table schema parser.
   *
   * @return column metadata string representation
   */
  String columnString();

  /**
   * Converts the column metadata into a format for display in a
   * logical or physical plan. Repeated elements appear as
   * <tt>ARRAY&lt;type></tt>
   */

  String planString();

  /**
   * Converts a generic serialized column description to a specific column
   * metadata subclass based on the type. Types are encoded in SQL-style
   * such as <tt>DECIMAL(10,4)</tt>.
   */
  @JsonCreator
  public static ColumnMetadata createInstance(
        @JsonProperty("name") String name,
        @JsonProperty("type") String type,
        @JsonProperty("mode") DataMode mode,
        @JsonProperty("format") String format,
        @JsonProperty("default") String defaultValue,
        @JsonProperty("properties") Map<String, String> properties)
        throws IOException {
    ColumnMetadata columnMetadata = SchemaExprParser.parseColumn(
        AbstractColumnMetadata.metadataString(name, type, mode));
    columnMetadata.setProperties(properties);
    if (format != null) {
      columnMetadata.setProperty(FORMAT_PROP, format);
    }
    if (defaultValue != null) {
      columnMetadata.setProperty(DEFAULT_VALUE_PROP, defaultValue);
    }
    return columnMetadata;
  }

  /**
   * Converts current {@link ColumnMetadata} implementation into JSON string representation.
   *
   * @return tuple metadata in JSON string representation
   * @throws IllegalStateException if unable to convert current instance into JSON string
   */
  String jsonString();

  /**
   * Converts given JSON string into {@link ColumnMetadata} instance.
   * {@link ColumnMetadata} implementation is determined by the data type.
   *
   * @param jsonString tuple metadata in JSON string representation
   * @return {@link ColumnMetadata} instance, null if given JSON string is null or empty
   * @throws IllegalArgumentException if unable to deserialize given JSON string
   */
  static ColumnMetadata of(String jsonString) {
    if (jsonString == null || jsonString.trim().isEmpty()) {
      return null;
    }
    try {
      return READER.readValue(jsonString);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Unable to deserialize given JSON string into column metadata: " + jsonString, e);
    }
  }
}
