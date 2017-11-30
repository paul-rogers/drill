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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.UnionColumnState;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.VariantSchema;
import org.apache.drill.exec.record.VariantMetadata;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.accessor.writer.UnionWriterImpl;

public class UnionState extends ContainerState
  implements VariantWriter.VariantWriterListener {

  public final UnionColumnState columnState;
  public final VariantMetadata schema = new VariantSchema();
  protected final Map<MinorType, ColumnState> columns = new HashMap<>();

  public UnionState(ResultSetLoaderImpl rsLoader, ResultVectorCache vectorCache, ProjectionSet projectionSet, UnionColumnState columnState) {
    super(rsLoader, vectorCache, projectionSet);
    this.columnState = columnState;
  }

  public UnionWriterImpl writer() {
    return (UnionWriterImpl) columnState.writer.variant();
  }

  public void buildSchema(VariantMetadata variantSchema) {
    for (ColumnMetadata member : variantSchema.members()) {
      writer().addColumnWriter(buildColumn(member));
    }
  }

  @Override
  public ObjectWriter addType(MinorType type) {
    if (schema.hasType(type)) {
      throw new IllegalArgumentException("Duplicate type: " + type.toString());
    }
    return null;
  }

  @Override
  protected void addColumn(ColumnState colState) {
    assert ! columns.containsKey(colState.schema().type());
    columns.put(colState.schema().type(), colState);
  }

  @Override
  protected Collection<ColumnState> columnStates() {
    return columns.values();
  }

  @Override
  public int innerCardinality() {
    return columnState.outerCardinality;
  }
}
