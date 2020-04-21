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
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.parser.ElementParser;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueDef;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Accepts { name : value ... }
 * <p>
 * The structure parser maintains a map of known fields. Each time a
 * field is parsed, looks up the field in the map. If not found, the parser
 * looks ahead to find a value token, if any, and calls this class to add
 * a new column. This class creates a column writer based either on the
 * type provided in a provided schema, or inferred from the JSON token.
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
 * when we only see {@code null}. The one for {@code null{@code  must morph into
 * the one for empty lists if we see:<br>
 * {@code {a: null} {a: [ ]  }}<br>
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
 *
 * <h4>Ambiguous Types</h4>
 *
 * JSON nulls are untyped. A run of nulls does not tell us what type will
 * eventually appear. The best solution is to provide a schema. Without a
 * schema, the code is forgiving: defers selection of the column type until
 * the first non-null value (or, forces a type at the end of the batch.)
 * <p>
 * For scalars the pattern is: <code>{a: null} {a: "foo"}</code>. Type
 * selection happens on the value {@code "foo"}.
 * <p>
 * For arrays, the pattern is: <code>{a: []} {a: ["foo"]}</code>. Type
 * selection happens on the first array element. Note that type selection
 * must happen on the first element, even if tha element is null (which,
 * as we just said, ambiguous.)
 * <p>
 * If we are forced to pick a type (because we hit the end of a batch, or
 * we see {@code [null]}, then we pick {@code VARCHAR} as we allow any
 * scalar to be converted to {@code VARCHAR}. This helps for a single-file
 * query, but not if multiple fragments each make their own (inconsistent)
 * decisions. Only a schema provides a consistent answer.
 */
public class TupleListener implements ObjectListener {

  private final JsonLoaderImpl loader;
  private final TupleWriter tupleWriter;
  private final TupleMetadata providedSchema;
  private final FieldFactory fieldFactory;

  public TupleListener(JsonLoaderImpl loader, TupleWriter tupleWriter, TupleMetadata providedSchema) {
    this.loader = loader;
    this.tupleWriter = tupleWriter;
    this.providedSchema = providedSchema;
    this.fieldFactory = loader.fieldFactoryFor(this);
  }

  public JsonLoaderImpl loader() { return loader; }

  public TupleWriter writer() { return tupleWriter; }

  protected TupleMetadata providedSchema() { return providedSchema; }

  @Override
  public void onStart() { }

  @Override
  public void onEnd() { }

  @Override
  public ElementParser onField(FieldDefn field) {
    if (!tupleWriter.isProjected(field.key())) {
      return field.fieldFactory().ignoredFieldParser();
    } else {
      return fieldFactory.addField(field);
    }
  }

  /**
   * Build a column and its listener based on a look-ahead hint.
   */
  public ValueListener resolveField(String key, ValueDef valueDef) {
    return fieldFactory.resolveField(key, valueDef);
  }

  public ObjectWriter fieldwriterFor(ColumnMetadata colSchema) {
    final TupleWriter writer = writer();
    final int index = writer.addColumn(colSchema);
    return writer.column(index);
  }
}
