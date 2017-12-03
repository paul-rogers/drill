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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.VariantMetadata;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;

/**
 * Build the set of writers from a defined schema. Uses the same
 * mechanism as dynamic schema: walks the schema tree adding each
 * column, then recursively adding the contents of maps and variants.
 */

public class BuildFromSchema {

  /**
   * When creating a schema up front, provide the schema of the desired tuple,
   * then build vectors and writers to match. Allows up-front schema definition
   * in addition to on-the-fly schema creation handled elsewhere.
   *
   * @param schema desired tuple schema to be materialized
   */

  public void buildTuple(TupleWriter writer, TupleMetadata schema) {
    for (int i = 0; i < schema.size(); i++) {
      ColumnMetadata colSchema = schema.metadata(i);
      int index = writer.addColumn(colSchema.cloneEmpty());
      assert index == i;
      expandColumn(writer.column(i), colSchema);
    }
  }

  private void expandColumn(ObjectWriter colWriter, ColumnMetadata colSchema) {
    if (colSchema.isMap()) {
      if (colSchema.isArray()) {
        buildTuple(colWriter.array().tuple(), colSchema.mapSchema());
      } else {
        buildTuple(colWriter.tuple(), colSchema.mapSchema());
      }
    } else if (colSchema.isVariant()) {
      buildUnion(colWriter.variant(), colSchema.variantSchema());
    }
  }

  public void buildUnion(VariantWriter writer, VariantMetadata schema) {
    for (ColumnMetadata member : schema.members()) {
      ObjectWriter memberWriter = writer.addMember(member.cloneEmpty());
      expandColumn(memberWriter, member);
    }
  }
}
