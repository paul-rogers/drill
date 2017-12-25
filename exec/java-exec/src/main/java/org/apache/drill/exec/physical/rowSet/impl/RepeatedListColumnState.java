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

import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.OffsetVectorState;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.complex.RepeatedListVector;

public class RepeatedListColumnState extends ColumnState {

  public static class RepeatedListVectorState implements VectorState {

    private final ArrayWriter arrayWriter;
    private final RepeatedListVector vector;
    private final OffsetVectorState offsetsState;

    public RepeatedListVectorState(ArrayWriter arrayWriter, RepeatedListVector vector) {
      this.vector = vector;
      this.arrayWriter = arrayWriter;
      offsetsState = new OffsetVectorState(
          arrayWriter, vector.getOffsetVector(), arrayWriter.array());
    }

    @SuppressWarnings("unchecked")
    @Override
    public RepeatedListVector vector() { return vector; }

    @Override
    public int allocate(int cardinality) {
      return offsetsState.allocate(cardinality);
    }

    @Override
    public void rollover(int cardinality) {
      offsetsState.rollover(cardinality);
    }

    @Override
    public void harvestWithLookAhead() {
      offsetsState.harvestWithLookAhead();
    }

    @Override
    public void startBatchWithLookAhead() {
      offsetsState.startBatchWithLookAhead();
    }

    @Override
    public void close() {
      offsetsState.close();
    }

    @Override
    public boolean isProjected() { return true; }

    @Override
    public void dump(HierarchicalFormatter format) {
      format
        .startObject(this)
        .attribute("schema", arrayWriter.schema())
        .attributeIdentity("writer", arrayWriter)
        .attributeIdentity("vector", vector)
        .attribute("offsetsState");
      offsetsState.dump(format);
      format
        .endObject();
    }
  }

  private ColumnState childState;

  public RepeatedListColumnState(LoaderInternals loader,
      AbstractObjectWriter arrayWriter, RepeatedListVectorState vectorState,
      ColumnState childState) {
    super(loader, arrayWriter, vectorState);
    this.childState = childState;
  }

  @Override
  public void updateCardinality(int cardinality) {
    super.updateCardinality(cardinality);
    childState.updateCardinality(innerCardinality());
  }

  @Override
  public void allocateVectors() {
    super.allocateVectors();
    childState.allocateVectors();
  }

  @Override
  public void startBatch(boolean schemaOnly) {
    super.startBatch(schemaOnly);
    childState.startBatch(schemaOnly);
  }

  @Override
  public void rollover() {
    super.rollover();
    childState.rollover();
  }

  @Override
  public void harvestWithLookAhead() {
    super.harvestWithLookAhead();
    childState.harvestWithLookAhead();
  }

  @Override
  public void close() {
    super.close();
    childState.close();
  }

  @Override
  public ColumnMetadata outputSchema() { return schema(); }
}
