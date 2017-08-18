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

import org.apache.drill.exec.physical.rowSet.model.ReaderIndex;
import org.apache.drill.exec.physical.rowSet.model.hyper.HyperRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.hyper.HyperRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.impl.AccessorUtilities;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.ObjectArrayReader;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;

/**
 * Build a set of readers for a hyper result set.
 */

public abstract class ReaderBuilderVisitor extends ModelVisitor<Void, ReaderBuilderVisitor.Context> {

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
    public int vectorIndex() {
      return AccessorUtilities.sv4Index(sv4.get(rowIndex));
    }

    @Override
    public int batchIndex( ) {
      return AccessorUtilities.sv4Batch(sv4.get(rowIndex));
    }
  }

  /**
   * Vector accessor used by the column accessors to obtain the vector for
   * each column value. That is, position 0 might be batch 4, index 3,
   * while position 1 might be batch 1, index 7, and so on.
   */

  public static class HyperVectorAccessor implements VectorAccessor {

    private final ValueVector[] vectors;
    private ColumnReaderIndex rowIndex;

    public HyperVectorAccessor(VectorWrapper<?> vw) {
      vectors = vw.getValueVectors();
    }

    @Override
    public void bind(ColumnReaderIndex index) {
      rowIndex = index;
    }

    @Override
    public ValueVector vector() {
      return vectors[rowIndex.batchIndex()];
    }
  }

  public static class Context {
    protected MapColumnModel mapColumn;
    protected AbstractObjectReader mapReader;
    protected List<AbstractObjectReader> childReaders;
  }

  protected AbstractObjectReader[] buildTuple(AbstractHyperTupleModel tuple) {
    Context context = new Context();
    context.childReaders = new ArrayList<>();
    tuple.visitChildren(this, context);
    AbstractObjectReader readers[] = new AbstractObjectReader[context.childReaders.size()];
    return context.childReaders.toArray(readers);
  }

  @Override
  protected Void visitPrimitiveColumn(PrimitiveColumnModel column, Context context ) {
    return visitScalarColumn(column, context);
  }

  @Override
  protected Void visitPrimitiveArrayColumn(PrimitiveColumnModel column, Context context ) {
    return visitScalarColumn(column, context);
  }

  private Void visitScalarColumn(PrimitiveColumnModel column, Context context) {
    context.childReaders.add(ColumnAccessorFactory.buildColumnReader(
        column.schema().majorType(), new HyperVectorAccessor(column.vectors())));
    return null;
  }

  @Override
  protected Void visitMapColumn(MapColumnModel column, Context context ) {
    context.childReaders.add(buildMap(column));
    return null;
  }

  @Override
  protected Void visitMapArrayColumn(MapColumnModel column, Context context ) {
    context.childReaders.add(ObjectArrayReader.build(new HyperVectorAccessor(column.vectors()), buildMap(column)));
    return null;
  }

  protected AbstractObjectReader buildMap(MapColumnModel column) {
    return MapReader.build(column.schema(), buildTuple(column.mapModelImpl()));
  }
}
