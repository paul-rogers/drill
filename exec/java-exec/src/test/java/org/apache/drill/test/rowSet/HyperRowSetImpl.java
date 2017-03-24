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
package org.apache.drill.test.rowSet;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.AccessorUtilities;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.test.rowSet.AbstractRowSetAccessor.BoundedRowIndex;
import org.apache.drill.test.rowSet.AbstractSingleRowSet.StructureBuilder;
import org.apache.drill.test.rowSet.RowSet.HyperRowSet;
import org.apache.drill.test.rowSet.RowSetSchema.LogicalColumn;
import org.apache.drill.test.rowSet.RowSetSchema.PhysicalSchema;

public class HyperRowSetImpl extends AbstractRowSet implements HyperRowSet {

  public static class HyperRowIndex extends BoundedRowIndex {

    private final SelectionVector4 sv4;

    public HyperRowIndex(SelectionVector4 sv4) {
      super(sv4.getCount());
      this.sv4 = sv4;
    }

    @Override
    public int index() {
      return AccessorUtilities.sv4Index(sv4.get(rowIndex));
    }

    @Override
    public int batch( ) {
      return AccessorUtilities.sv4Batch(sv4.get(rowIndex));
    }
  }

  public static class HyperVectorBuilder {

    protected final HyperVectorWrapper<?>[] valueVectors;
    protected final HyperVectorWrapper<AbstractMapVector>[] mapVectors;
    protected int vectorIndex;
    protected int mapIndex;

    @SuppressWarnings("unchecked")
    public HyperVectorBuilder(RowSetSchema schema) {
      valueVectors = new HyperVectorWrapper<?>[schema.access().count()];
      if (schema.access().mapCount() == 0) {
        mapVectors = null;
      } else {
        mapVectors = (HyperVectorWrapper<AbstractMapVector>[])
            new HyperVectorWrapper<?>[schema.access().mapCount()];
      }
    }

    public HyperVectorWrapper<?> mapContainer(VectorContainer container) {
      for (VectorWrapper<?> w : container) {
         if (w.getField().getType().getMinorType() == MinorType.MAP) {
           HyperVectorWrapper<AbstractMapVector> mw = (HyperVectorWrapper<AbstractMapVector>) w;
          mapVectors[mapIndex++] = mw;
          buildMap(mw);
        } else {
          valueVectors[vectorIndex++] = w;
        }
      }
      return valueVectors;
    }

    private void buildMap(HyperVectorWrapper<AbstractMapVector> mapWrapper) {
      for (VectorWrapper<?> w : mapWrapper.) {
        if (v.getField().getType().getMinorType() == MinorType.MAP) {
          MapVector mv = (MapVector) v;
          mapVectors[mapIndex++] = mv;
          buildMap(mv);
        } else {
          valueVectors[vectorIndex++] = v;
        }
      }
    }
  }

  private final SelectionVector4 sv4;
  private final HyperVectorWrapper<ValueVector> hvw[];

  @SuppressWarnings("unchecked")
  public HyperRowSetImpl(BufferAllocator allocator, VectorContainer container, SelectionVector4 sv4) {
    super(allocator, container.getSchema(), container);
    this.sv4 = sv4;
    hvw = new HyperVectorWrapper[schema.count()];

    for (int i = 0; i < schema.count(); i++) {
      hvw[i] = (HyperVectorWrapper<ValueVector>) container.getValueVector(i);
    }
  }

  @Override
  public boolean isExtendable() { return false; }

  @Override
  public boolean isWritable() { return false; }

  @Override
  public RowSetWriter writer() {
    throw new UnsupportedOperationException("Cannot write to a hyper vector");
  }

  @Override
  public RowSetReader reader() {
    return new RowSetReaderImpl(this, new HyperRowIndex(sv4));
  }

  @Override
  public SelectionVectorMode getIndirectionType() { return SelectionVectorMode.FOUR_BYTE; }

  @Override
  public SelectionVector4 getSv4() { return sv4; }

  @Override
  public HyperVectorWrapper<ValueVector> getHyperVector(int i) { return hvw[i]; }

  @Override
  public int rowCount() { return sv4.getCount(); }
}
