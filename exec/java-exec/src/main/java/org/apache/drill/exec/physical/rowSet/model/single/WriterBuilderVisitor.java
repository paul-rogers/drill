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
package org.apache.drill.exec.physical.rowSet.model.single;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapColumnModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.MapModel;
import org.apache.drill.exec.physical.rowSet.model.single.SingleRowSetModel.PrimitiveColumnModel;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.accessor.writer.ObjectArrayWriter;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

/**
 * Build a set of writers for a single (non-hyper) result set model.
 */

public abstract class WriterBuilderVisitor extends ModelVisitor<Void, WriterBuilderVisitor.Context> {

  public static class Context {
    protected MapColumnModel mapColumn;
    protected AbstractObjectWriter mapWriter;
    protected List<AbstractObjectWriter> childWriters;
  }

  @Override
  protected Void visitMap(MapModel map, Context context) {
    context.mapWriter = MapWriter.build(context.mapColumn.schema(), (MapVector) map.vector(), buildTuple(map));
    map.bindWriter(context.mapWriter);
    return null;
  }

  @Override
  protected Void visitMapArray(MapModel map, Context context) {
    context.mapWriter = MapWriter.buildMapArray(context.mapColumn.schema(), (RepeatedMapVector) map.vector(), buildTuple(map));
    map.bindWriter(context.mapWriter);
    return null;
  }

  protected List<AbstractObjectWriter> buildTuple(AbstractSingleTupleModel tuple) {
    Context context = new Context();
    context.childWriters = new ArrayList<>();
    tuple.visitChildren(this, context);
    return context.childWriters;
  }

  @Override
  protected Void visitPrimitiveColumn(PrimitiveColumnModel column, Context context) {
    return visitScalarColumn(column, context);
  }

  @Override
  protected Void visitPrimitiveArrayColumn(PrimitiveColumnModel column, Context context) {
    return visitScalarColumn(column, context);
  }

  private Void visitScalarColumn(PrimitiveColumnModel column, Context context) {
    AbstractObjectWriter colWriter = ColumnAccessorFactory.buildColumnWriter(column.vector());
    context.childWriters.add(colWriter);
    column.bindWriter(colWriter);
    return null;
  }

  @Override
  protected Void visitMapColumn(MapColumnModel column, Context context) {

    // Build the tuple writer for this column. Bind it to the model.

    AbstractObjectWriter tupleWriter = buildMap(column);
    context.childWriters.add(tupleWriter);
    column.bindWriter(tupleWriter);
    return null;
  }

  @Override
  protected Void visitMapArrayColumn(MapColumnModel column, Context context) {

    // First, build a tuple writer for the map, then wrap it in an array
    // writer for the repeated map. Bind that to the column model.

    AbstractObjectWriter tupleWriter = buildMap(column);
    AbstractObjectWriter arrayWriter = ObjectArrayWriter.build((RepeatedMapVector) column.vector(), tupleWriter);
    context.childWriters.add(arrayWriter);
    column.bindWriter(arrayWriter);
    return null;
  }

  protected AbstractObjectWriter buildMap(MapColumnModel column) {
    Context context = new Context();
    context.mapColumn = column;
    column.mapModelImpl().visit(this, context);
    return context.mapWriter;
  }
}
