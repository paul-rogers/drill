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
package org.apache.drill.exec.physical.rowSet.model.hyper;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.AbstractReaderBuilder;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.VectorDescrip;
import org.apache.drill.exec.physical.rowSet.model.ReaderIndex;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.impl.AccessorUtilities;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.ArrayReaderImpl;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.UnionReaderImpl;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessors.BaseHyperVectorAccessor;

public abstract class BaseReaderBuilder extends AbstractReaderBuilder {

  /**
   * Read-only row index into the hyper row set with batch and index
   * values mapping via an SV4.
   */

  public static class HyperRowIndex extends ReaderIndex {

    private final SelectionVector4 sv4;

    public HyperRowIndex(SelectionVector4 sv4) {
      super(sv4.getCount());
      this.sv4 = sv4;
    }

    @Override
    public int offset() {
      return AccessorUtilities.sv4Index(sv4.get(position));
    }

    @Override
    public int hyperVectorIndex( ) {
      return AccessorUtilities.sv4Batch(sv4.get(position));
    }
  }

  /**
   * Vector accessor used by the column accessors to obtain the vector for
   * each column value. That is, position 0 might be batch 4, index 3,
   * while position 1 might be batch 1, index 7, and so on.
   */

  public static class HyperVectorAccessor extends BaseHyperVectorAccessor {

    private final ValueVector[] vectors;
    private ColumnReaderIndex rowIndex;

    public HyperVectorAccessor(VectorWrapper<?> vw) {
      super(vw.getField().getType());
      vectors = vw.getValueVectors();
    }

    @Override
    public void bind(ColumnReaderIndex index) {
      rowIndex = index;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T vector() {
      return (T) vectors[rowIndex.hyperVectorIndex()];
    }
  }

  protected List<AbstractObjectReader> buildContainerChildren(
      VectorContainer container, MetadataProvider mdProvider) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      VectorWrapper<?> vw = container.getValueVector(i);
      VectorDescrip descrip = new VectorDescrip(mdProvider, i, vw.getField());
      readers.add(buildVectorReader(vw, descrip));
    }
    return readers;
  }

  protected AbstractObjectReader buildVectorReader(VectorWrapper<?> vw, VectorDescrip descrip) {
    VectorAccessor va = new HyperVectorAccessor(vw);
    MajorType type = va.type();

    switch(type.getMinorType()) {
    case MAP:
      return buildMap(vw, va, type, descrip);
    case UNION:
      return buildUnion(vw, va, descrip);
    case LIST:
      return buildList(vw, va, descrip);
    default:
      return buildScalarReader(va, descrip.metadata);
    }
  }

  private AbstractObjectReader buildMap(VectorWrapper<?> vw, VectorAccessor va, MajorType type, VectorDescrip descrip) {

    // Map type

    AbstractObjectReader mapReader = MapReader.build(
        descrip.metadata,
        buildMapMembers(vw,
            descrip.parent.childProvider(descrip.metadata)));

    // Single map

    if (type.getMode() != DataMode.REPEATED) {
      return mapReader;
    }

    // Repeated map

    return ArrayReaderImpl.buildTuple(descrip.metadata, va, mapReader);
  }

  protected List<AbstractObjectReader> buildMapMembers(VectorWrapper<?> vw, MetadataProvider provider) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    MaterializedField mapField = vw.getField();
    for (int i = 0; i < mapField.getChildren().size(); i++) {
      HyperVectorWrapper<? extends ValueVector> child = (HyperVectorWrapper<? extends ValueVector>) vw.getChildWrapper(new int[] {i});
      VectorDescrip descrip = new VectorDescrip(provider, i, child.getField());
      readers.add(buildVectorReader(child, descrip));
      i++;
    }
    return readers;
  }

  private AbstractObjectReader buildUnion(VectorWrapper<?> vw, VectorAccessor unionAccessor, VectorDescrip descrip) {
    MetadataProvider provider = descrip.childProvider();
    final AbstractObjectReader variants[] = new AbstractObjectReader[MinorType.values().length];
    for (int i = 0; i < vw.getField().getType().getSubTypeList().size(); i++) {
      HyperVectorWrapper<? extends ValueVector> child = (HyperVectorWrapper<? extends ValueVector>) vw.getChildWrapper(new int[] {i});
      VectorDescrip memberDescrip = new VectorDescrip(provider, i, child.getField());
      variants[child.getField().getType().getMinorType().ordinal()] = buildVectorReader(child, memberDescrip);
    }
    return UnionReaderImpl.build(
        descrip.metadata,
        unionAccessor,
        variants);
  }

  private AbstractObjectReader buildList(VectorWrapper<?> vw, VectorAccessor listAccessor,
      VectorDescrip listDescrip) {
    HyperVectorWrapper<? extends ValueVector> dataWrapper = (HyperVectorWrapper<? extends ValueVector>) vw.getChildWrapper(new int[] {0});
    VectorDescrip dataMetadata;
    if (dataWrapper.getField().getType().getMinorType() == MinorType.UNION) {

      // If the list holds a union, then the list and union are collapsed
      // together in the metadata layer.

      dataMetadata = listDescrip;
    } else {
      dataMetadata = new VectorDescrip(listDescrip.childProvider(), 0, dataWrapper.getField());
    }
    return ArrayReaderImpl.buildList(listDescrip.metadata,
        listAccessor, buildVectorReader(dataWrapper, dataMetadata));
  }
}
