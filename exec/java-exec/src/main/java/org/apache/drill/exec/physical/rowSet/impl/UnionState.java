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
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.FixedWidthVectorState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.SimpleVectorState;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.TupleSchema.VariantSchema;
import org.apache.drill.exec.record.VariantMetadata;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.UnionWriterImpl;
import org.apache.drill.exec.vector.complex.UnionVector;

public class UnionState extends ContainerState
  implements VariantWriter.VariantWriterListener {

  public static class UnionVectorState implements VectorState {

    private final UnionVector vector;
    private final SimpleVectorState typesVectorState;

    public UnionVectorState(UnionVector vector, UnionWriterImpl unionWriter) {
      this.vector = vector;
      typesVectorState = new FixedWidthVectorState(unionWriter.typeWriter(), vector.getTypeVector());
    }

    @Override
    public int allocate(int cardinality) {
      return typesVectorState.allocate(cardinality);
    }

    @Override
    public void rollover(int cardinality) {
      typesVectorState.rollover(cardinality);
    }

    @Override
    public void harvestWithLookAhead() {
      typesVectorState.harvestWithLookAhead();
    }

    @Override
    public void startBatchWithLookAhead() {
      typesVectorState.startBatchWithLookAhead();
    }

    @Override
    public void reset() {
      typesVectorState.reset();
    }

    @SuppressWarnings("unchecked")
    @Override
    public UnionVector vector() {
      return vector;
    }

    @Override
    public boolean isProjected() {
      return true;
    }

    @Override
    public void dump(HierarchicalFormatter format) {
      // TODO Auto-generated method stub

    }
  }

  private UnionColumnState columnState;
  private final VariantMetadata schema = new VariantSchema();
  private final Map<MinorType, ColumnState> columns = new HashMap<>();

  public UnionState(LoaderInternals events, ResultVectorCache vectorCache, ProjectionSet projectionSet) {
    super(events, vectorCache, projectionSet);
  }

  public void bindColumnState(UnionColumnState columnState) {
    this.columnState = columnState;
    writer().bindListener(this);
  }

  public UnionWriterImpl writer() {
    return (UnionWriterImpl) columnState.writer.variant();
  }

  public UnionVector vector() {
    return (UnionVector) columnState.vector();
  }

  @Override
  public ObjectWriter addMember(ColumnMetadata member) {
    if (schema.hasType(member.type())) {
      throw new IllegalArgumentException("Duplicate type: " + member.type().toString());
    }
    return addColumn(member).writer();
  }

  @Override
  public ObjectWriter addType(MinorType type) {
    return addMember(VariantSchema.memberMetadata(type));
  }

  @Override
  protected void addColumn(ColumnState colState) {
    assert ! columns.containsKey(colState.schema().type());
    columns.put(colState.schema().type(), colState);
    vector().addType(colState.vector());
  }

  @Override
  protected Collection<ColumnState> columnStates() {
    return columns.values();
  }

  @Override
  public int innerCardinality() {
    return columnState.outerCardinality;
  }

  @Override
  protected boolean isWithinUnion() { return true; }
}
