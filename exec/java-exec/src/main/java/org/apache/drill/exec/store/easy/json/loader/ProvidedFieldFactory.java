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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.values.VariantListener;
import org.apache.drill.exec.store.easy.json.parser.ElementParser.ValueParser;
import org.apache.drill.exec.store.easy.json.parser.FieldParserFactory;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener.FieldDefn;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Create a Drill field listener based on a provided schema. The schema
 * takes precedence over the JSON syntax: the schema is expected to
 * accurately describe what will occur for this field in the JSON
 * input.
 */
public class ProvidedFieldFactory extends BaseFieldFactory {

  public ProvidedFieldFactory(TupleListener tupleListener, FieldFactory child) {
    super(tupleListener, child);
    Preconditions.checkArgument(tupleListener.providedSchema() != null);
  }

  public ColumnMetadata providedColumn(String key) {
    return tupleListener.providedSchema().metadata(key);
  }

  /**
   * Build a column and its listener based a provided schema.
   * The user is responsible to ensure that the provided schema
   * accurately reflects the structure of the JSON being parsed.
   */
  @Override
  public ValueParser addField(FieldDefn fieldDefn) {
    ColumnMetadata providedCol = providedColumn(fieldDefn.key());
    if (providedCol == null) {
      return child.addField(fieldDefn);
    }
    return parserFor(fieldDefn, providedCol, listenerFor(fieldDefn, providedCol));
  }

  public ValueParser parserFor(FieldDefn fieldDefn, ColumnMetadata providedCol, ValueListener fieldListener) {
    FieldParserFactory parserFactory = parserFactory();
    String mode = providedCol.property(JsonLoader.JSON_MODE);
    if (mode == null) {
      return parserFactory.valueParser(fieldDefn, fieldListener);
    }
    switch (mode) {
      case JsonLoader.JSON_TEXT_MODE:
        return parserFactory.textValueParser(fieldDefn, fieldListener);
      case JsonLoader.JSON_LITERAL_MODE:
        return parserFactory.jsonTextParser(fieldListener);
      default:
        return parserFactory.valueParser(fieldDefn, fieldListener);
    }
  }

  public ValueListener listenerFor(FieldDefn fieldDefn, ColumnMetadata providedCol) {
    switch (providedCol.structureType()) {

      case PRIMITIVE: {
        ColumnMetadata colSchema = providedCol.copy();
        if (providedCol.isArray()) {
          return scalarArrayListenerFor(colSchema);
        } else {
          return scalarListenerFor(colSchema);
        }
      }

      case TUPLE: {
        // Propagate the provided map schema into the object
        // listener as a provided tuple schema.
        ColumnMetadata colSchema = providedCol.cloneEmpty();
        TupleMetadata providedSchema = providedCol.tupleSchema();
        if (providedCol.isArray()) {
          return objectArrayListenerFor(colSchema, providedSchema);
        } else {
          return objectListenerFor(colSchema, providedSchema);
        }
      }

      case VARIANT: {
        // A variant can contain multiple types. The schema does not
        // declare the types; rather they are discovered by the reader.
        // That is, there is no VARIANT<INT, DOUBLE>, there is just VARIANT.
        ColumnMetadata colSchema = providedCol.cloneEmpty();
        if (providedCol.isArray()) {
          return variantArrayListenerFor(colSchema);
        } else {
          return variantListenerFor(colSchema);
        }
      }

      case MULTI_ARRAY:
        return multiDimArrayListenerForSchema(providedCol);

      default:
        throw loader().unsupportedType(providedCol);
    }
  }

  /**
   * Create a repeated list column and its multiple levels of inner structure
   * from a provided schema. Repeated lists can nest to any number of levels to
   * provide any number of dimensions. In general, if an array is <i>n</i>-dimensional,
   * then there are <i>n</i>-1 repeated lists with some array type as the
   * innermost dimension.
   */
  private ValueListener multiDimArrayListenerForSchema(ColumnMetadata providedSchema) {
    // Parse the stack of repeated lists to count the "outer" dimensions and
    // to locate the innermost array (the "list" which is "repeated").
    int dims = 1; // For inner array
    ColumnMetadata elementSchema = providedSchema;
    while (MetadataUtils.isRepeatedList(elementSchema)) {
      dims++;
      elementSchema = elementSchema.childSchema();
      Preconditions.checkArgument(elementSchema != null);
    }

    ColumnMetadata colSchema = repeatedListSchemaFor(providedSchema.name(), dims,
        elementSchema.cloneEmpty());
    switch (elementSchema.structureType()) {

      case PRIMITIVE:
        return multiDimScalarArrayListenerFor(colSchema, dims);

      case TUPLE:
        return multiDimObjectArrayListenerFor(colSchema,
            dims, elementSchema.tupleSchema());

      case VARIANT:
        return multiDimVariantArrayListenerFor(colSchema, dims);

      default:
        throw loader().unsupportedType(providedSchema);
    }
  }

  /**
   * Create a variant (UNION) column and its associated listener given
   * a column schema.
   */
  private ValueListener variantListenerFor(ColumnMetadata colSchema) {
    return new VariantListener(loader(),
        tupleListener.fieldwriterFor(colSchema).variant());
  }

  /**
   * Create a variant array (LIST) column and its associated listener given
   * a column schema.
   */
  private ValueListener variantArrayListenerFor(ColumnMetadata colSchema) {
    return new ListListener(loader(), tupleListener.fieldwriterFor(colSchema));
  }
}
